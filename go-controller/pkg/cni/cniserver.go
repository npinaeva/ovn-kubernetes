// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package cni

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	cnitypes "github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/100"
	"github.com/gorilla/mux"
	nadv1Listers "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	resourceapply "k8s.io/client-go/applyconfigurations/resource/v1"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/ovn-kubernetes/libovsdb/client"

	ovncnitypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/cni/udn"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/dra"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

const kubeletDefaultCRIOperationTimeout = 2 * time.Minute
const resourceNameAnnot = "k8s.v1.cni.cncf.io/resourceName"

// *** The Server is PRIVATE API between OVN components and may be
// changed at any time.  It is in no way a supported interface or API. ***
//
// The Server accepts pod setup/teardown requests from the OVN
// CNI plugin, which is itself called by kubelet when pod networking
// should be set up or torn down.  The OVN CNI plugin gathers up
// the standard CNI environment variables and network configuration provided
// on stdin and forwards them to the Server over a private, root-only
// Unix domain socket, using HTTP as the transport and JSON as the protocol.
//
// The Server interprets standard CNI environment variables as specified
// by the Container Network Interface (CNI) specification available here:
// https://github.com/containernetworking/cni/blob/master/SPEC.md
// While the Server interface is not itself versioned, as the CNI
// specification requires that CNI network configuration is versioned, and
// since the OVN CNI plugin passes that configuration to the
// Server, versioning is ensured in exactly the same way as an executable
// CNI plugin would be versioned.
//
// Security: since the Unix domain socket created by the Server is owned
// by root and inaccessible to any other user, no unprivileged process may
// access the Server.  The Unix domain socket and its parent directory are
// removed and re-created with 0700 permissions each time ovnkube on the node is
// started.

// NewCNIServer creates and returns a new Server object which will listen on a socket in the given path
func NewCNIServer(
	factory factory.NodeWatchFactory,
	kclient kubernetes.Interface,
	networkManager networkmanager.Interface,
	ovsClient client.Client,
	dpuHealth DPUStatusProvider,
	draState dra.DRAState,
) (*Server, error) {
	var nadLister nadv1Listers.NetworkAttachmentDefinitionLister

	if config.IsModeDPU() {
		return nil, fmt.Errorf("unsupported ovnkube-node mode for CNI server: %s", config.OvnKubeNode.Mode)
	}

	router := mux.NewRouter()

	if config.OVNKubernetesFeature.EnableMultiNetwork {
		nadLister = factory.NADInformer().Lister()
	}
	s := &Server{
		Server: http.Server{
			Handler: router,
		},
		clientSet: &ClientSet{
			nadLister: nadLister,
			podLister: corev1listers.NewPodLister(factory.LocalPodInformer().GetIndexer()),
			kclient:   kclient,
		},
		kubeAuth: &KubeAPIAuth{
			Kubeconfig:       config.Kubernetes.Kubeconfig,
			KubeAPIServer:    config.Kubernetes.APIServer,
			KubeAPIToken:     config.Kubernetes.Token,
			KubeAPITokenFile: config.Kubernetes.TokenFile,
		},
		networkManager: networkManager,
		ovsClient:      ovsClient,
		dpuHealth:      dpuHealth,
		draState:       draState,
	}

	if len(config.Kubernetes.CAData) > 0 {
		s.kubeAuth.KubeCAData = base64.StdEncoding.EncodeToString(config.Kubernetes.CAData)
	}

	router.NotFoundHandler = http.HandlerFunc(http.NotFound)
	router.HandleFunc("/metrics", s.handleCNIMetrics).Methods("POST")
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				klog.Errorf("cniserver panic on %s %s: %v\n%s",
					r.Method, r.URL.Path, rec, debug.Stack())
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		result, err := s.handleCNIRequest(r)
		if err != nil {
			var cniErr *cnitypes.Error
			if errors.As(err, &cniErr) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				if encodeErr := json.NewEncoder(w).Encode(cniErr); encodeErr != nil {
					klog.Warningf("Failed to write CNI error response: %v", encodeErr)
				}
				return
			}
			http.Error(w, fmt.Sprintf("%v", err), http.StatusBadRequest)
			return
		}

		// Empty response JSON means success with no body
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(result); err != nil {
			klog.Warningf("Error writing HTTP response: %v", err)
		}
	}).Methods("POST")

	return s, nil
}

// Split the "CNI_ARGS" environment variable's value into a map.  CNI_ARGS
// contains arbitrary key/value pairs separated by ';' and is for runtime or
// plugin specific uses.  Kubernetes passes the pod namespace and name in
// CNI_ARGS.
func gatherCNIArgs(env map[string]string) (map[string]string, error) {
	cniArgs, ok := env["CNI_ARGS"]
	if !ok {
		return nil, fmt.Errorf("missing CNI_ARGS: '%s'", env)
	}

	mapArgs := make(map[string]string)
	for _, arg := range strings.Split(cniArgs, ";") {
		parts := strings.Split(arg, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid CNI_ARG '%s'", arg)
		}
		mapArgs[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return mapArgs, nil
}

func cniRequestToPodRequest(cr *Request, ctx context.Context) (*PodRequest, error) {
	cmd, ok := cr.Env["CNI_COMMAND"]
	if !ok {
		return nil, fmt.Errorf("unexpected or missing CNI_COMMAND")
	}

	req := &PodRequest{
		Command:   command(cmd),
		timestamp: time.Now(),
		ctx:       ctx,
	}

	conf, err := config.ReadCNIConfig(cr.Config)
	if err != nil {
		return nil, fmt.Errorf("broken stdin args")
	}
	req.CNIConf = conf
	klog.Infof("DEBUG: CNI config for pod request: %+v", req.CNIConf)

	req.SandboxID, ok = cr.Env["CNI_CONTAINERID"]
	if !ok {
		return nil, fmt.Errorf("missing CNI_CONTAINERID")
	}
	req.Netns, ok = cr.Env["CNI_NETNS"]
	if !ok {
		return nil, fmt.Errorf("missing CNI_NETNS")
	}

	req.IfName, ok = cr.Env["CNI_IFNAME"]
	if !ok {
		req.IfName = "eth0"
	}

	cniArgs, err := gatherCNIArgs(cr.Env)
	if err != nil {
		return nil, err
	}

	req.PodNamespace, ok = cniArgs["K8S_POD_NAMESPACE"]
	if !ok {
		return nil, fmt.Errorf("missing K8S_POD_NAMESPACE")
	}

	req.PodName, ok = cniArgs["K8S_POD_NAME"]
	if !ok {
		return nil, fmt.Errorf("missing K8S_POD_NAME")
	}

	// UID may not be passed by all runtimes yet. Will be passed
	// by CRIO 1.20+ and containerd 1.5+ soon.
	// CRIO 1.20: https://github.com/cri-o/cri-o/pull/5029
	// CRIO 1.21: https://github.com/cri-o/cri-o/pull/5028
	// CRIO 1.22: https://github.com/cri-o/cri-o/pull/5026
	// containerd 1.6: https://github.com/containerd/containerd/pull/5640
	// containerd 1.5: https://github.com/containerd/containerd/pull/5643
	req.PodUID = cniArgs["K8S_POD_UID"]

	// the first network to the Pod is always named as `default`,
	// capture the effective NAD Name here
	req.netName = conf.Name
	if req.netName == types.DefaultNetworkName {
		req.nadName = types.DefaultNetworkName
		// when default network is setup via multus using a nad, nadKey may actually be different
		req.nadKey = types.DefaultNetworkName
	} else {
		req.nadName = conf.NADName
	}

	if err = updateDeviceInfo(req); err != nil {
		return nil, err
	}
	return req, nil
}

func updateDeviceInfo(pr *PodRequest) error {
	if pr.CNIConf.DeviceID == "" {
		return nil
	}
	klog.Infof("DEBUG: isPCI %t, isVFIO %v deviceID %q", util.IsPCIDeviceName(pr.CNIConf.DeviceID), util.GetSriovnetOps().IsVfPciVfioBound(pr.CNIConf.DeviceID), pr.CNIConf.DeviceID)
	if util.IsPCIDeviceName(pr.CNIConf.DeviceID) {
		// DeviceID is a PCI address
		pr.IsVFIO = util.GetSriovnetOps().IsVfPciVfioBound(pr.CNIConf.DeviceID)
		pr.IsLegacySRIOV = util.IsLegacyVFPCI(pr.CNIConf.DeviceID)
	} else if util.IsAuxDeviceName(pr.CNIConf.DeviceID) {
		// DeviceID is an Auxiliary device name - <driver_name>.<kind_of_a_type>.<id>
		chunks := strings.Split(pr.CNIConf.DeviceID, ".")
		if len(chunks) < 2 {
			return fmt.Errorf("invalid auxiliary device name %q: expected driver.<type>.<id>", pr.CNIConf.DeviceID)
		}
		if chunks[1] != "sf" {
			return fmt.Errorf("only SF auxiliary devices are supported, device name %q is not supported", pr.CNIConf.DeviceID)
		}
	} // else it is a netdev name, which is used for simulated DPU environments.
	return nil
}

func (s *Server) setDeviceInfo(req *PodRequest) (err error) {
	defer func() {
		if err == nil {
			// this is needed for deviceID coming from anywhere
			err = updateDeviceInfo(req)
		}
	}()
	if req.CNIConf.DeviceID != "" {
		// already set by multus
		return
	}
	if req.CNIConf.NADName == "" || req.CNIConf.NADName == types.DefaultNetworkName {
		return nil
	}
	// this is not always the same as req.nadName, because default network overrides the real nadName coming from multus.
	// So in case when default network is accelerated we need to look at the actual CNI config to find nad name
	fullNADName := req.CNIConf.NADName
	if req.Command == CNIAdd {
		nadNamespace, nadName, err := cache.SplitMetaNamespaceKey(fullNADName)
		if err != nil {
			return fmt.Errorf("invalid NAD name %s", nadNamespace)
		}
		nad, err := s.clientSet.nadLister.NetworkAttachmentDefinitions(nadNamespace).Get(nadName)
		if err != nil {
			return fmt.Errorf("failed to get primary UDN's network-attachment-definition %s: %v", nadName, err)
		}
		// if both DeviceClassAnnotation and resourceNameAnnot are present, resourceNameAnnot wins as the old way
		resourceName := nad.Annotations[resourceNameAnnot]
		if resourceName != "" {
			// primary UDN with resourceName specified
			pod, err := s.clientSet.getPod(req.PodNamespace, req.PodName)
			if err != nil {
				return fmt.Errorf("failed to get pod %s/%s: %v", req.PodNamespace, req.PodName, err)
			}
			deviceID, err := udn.GetPodPrimaryUDNDeviceID(pod, resourceName)
			if err != nil {
				return fmt.Errorf("failed to get primary UDN device ID for pod %s/%s resource %s: %v",
					pod.Namespace, pod.Name, resourceName, err)
			}
			req.CNIConf.DeviceID = deviceID
			return nil
		}
		deviceClass := nad.Annotations[dra.DeviceClassAnnotation]
		if deviceClass != "" {
			// DRA based allocation
			devices, _ := s.draState.GetAllocatedDevices(req.PodUID)
			for deviceName, device := range devices {
				if device.DeviceClass == deviceClass && (device.IfaceName == "" || device.IfaceName == req.IfName) {
					deviceAttrs, _ := s.draState.GetDeviceAttributes(deviceName)
					req.CNIConf.DeviceID = deviceAttrs[dra.AttrPCIAddress]
					if err := s.updateClaimStatus(req.ctx, deviceName, device, req.IfName, req.PodNamespace, req.PodUID); err != nil {
						klog.Errorf("DRA: failed to update ResourceClaim status: %v", err)
					}
					return nil
				}
			}
			return fmt.Errorf("failed to find allocated device for pod %s/%s NAD %s DeviceClass %s", req.PodNamespace, req.PodName, fullNADName, deviceClass)
		}
	} else {
		// CNI DEL is only supported for DRA and not for primary UDN
		devices, _ := s.draState.GetAllocatedDevices(req.PodUID)
		for deviceName, device := range devices {
			if device.IfaceName == req.IfName {
				deviceAttrs, _ := s.draState.GetDeviceAttributes(deviceName)
				klog.Infof("DEBUG: found device %s for pod %s/%s NAD %s to release", deviceName, req.PodNamespace, req.PodName, fullNADName)
				req.CNIConf.DeviceID = deviceAttrs[dra.AttrPCIAddress]
			}
		}
		return nil
	}
	return nil
}

// updateClaimStatus writes back the (deviceName → ifaceName) mapping for a DRA
// allocation onto its ResourceClaim's status via server-side apply, and
// updates the in-memory DRA state. The claim's namespace equals the pod's
// namespace; its name is carried on the AllocatedDevice as ClaimName.
func (s *Server) updateClaimStatus(ctx context.Context, deviceName string, device *dra.AllocatedDevice, ifaceName, podNamespace, podUID string) error {
	if device.IfaceName == ifaceName {
		return nil
	}
	if device.ClaimName == "" {
		return fmt.Errorf("missing claim name for allocated device %s (pod uid %s)", deviceName, podUID)
	}
	deviceStatus := resourceapply.
		AllocatedDeviceStatus().
		WithDevice(deviceName).
		WithDriver(dra.DriverName).
		WithPool(s.draState.NodeName()).
		WithNetworkData(resourceapply.NetworkDeviceData().WithInterfaceName(ifaceName))
	resourceClaimApply := resourceapply.
		ResourceClaim(device.ClaimName, podNamespace).
		WithStatus(resourceapply.ResourceClaimStatus().WithDevices(deviceStatus))

	_, err := s.clientSet.kclient.ResourceV1().ResourceClaims(podNamespace).ApplyStatus(ctx,
		resourceClaimApply,
		metav1.ApplyOptions{
			// since devices from the same claim are handled by different CNI requests, it gets very
			// complicated to store and merge all device status for the same claim.
			// Instead, we are using different field manager for each device, so that they can update the claim status
			// independently without overwriting each other's changes.
			FieldManager: dra.DriverName + "/" + deviceName,
			Force:        true,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to update ResourceClaim status for claim %s/%s: %v", podNamespace, device.ClaimName, err)
	}

	device.IfaceName = ifaceName
	s.draState.UpdateAllocatedDevice(podUID, deviceName, device)
	return nil
}

// Dispatch a pod request to the request handler and return the result to the
// CNI server client
func (s *Server) handleCNIRequest(r *http.Request) (result []byte, err error) {
	var cr Request
	b, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(b, &cr); err != nil {
		return nil, err
	}
	// Match the Kubelet default CRI operation timeout of 2m.
	ctx, cancel := context.WithTimeout(context.Background(), kubeletDefaultCRIOperationTimeout)
	defer cancel()

	cmd, ok := cr.Env["CNI_COMMAND"]
	if !ok {
		return nil, fmt.Errorf("unexpected or missing CNI_COMMAND")
	}
	command := command(cmd)

	if err = s.checkDPUHealth(command); err != nil {
		klog.Infof("%s finished CNI request, err %v", command, err)
		return nil, err
	}

	if command == CNICheck || command == CNIStatus || command == CNIUpdate {
		// CNICheck is not considered useful, and has a considerable performance impact
		// to pod bring up times with CRIO. This is due to the fact that CRIO currently calls check
		// after CNI ADD before it finishes bringing the container up
		// CNIUpdate is no-op today
		// CNIStatus is handled by DPU health check gating before reaching here
		klog.Infof("%s finished CNI request, err=nil", command)
		return nil, nil
	}

	request, err := cniRequestToPodRequest(&cr, ctx)
	if err != nil {
		klog.Infof("Failed to convert CNI request %+v to PodRequest, err %v", cr, err)
		return nil, err
	}
	var response *Response
	defer func() {
		var resultForLogging []byte
		var loggingErr error
		if response != nil {
			if resultForLogging, loggingErr = response.MarshalForLogging(); loggingErr != nil {
				klog.Errorf("%s %s CNI request %+v, %v", request, request.Command, request, loggingErr)
			}
		}
		klog.Infof("%s %s finished CNI request %+v, result %q, err %v",
			request, request.Command, request, string(resultForLogging), err)
		if err != nil {
			// Prefix error with request information for easier debugging
			var cniErr *cnitypes.Error
			if !errors.As(err, &cniErr) {
				err = fmt.Errorf("%s %w", request, err)
			}
		}
	}()
	if err = s.setDeviceInfo(request); err != nil {
		klog.Infof("DEBUG: error setting DRA device info for pod %s/%s NAD %s: %v", request.PodNamespace, request.PodName, request.nadName, err)
		return nil, err
	}

	klog.Infof("%s %s starting CNI request", request, request.Command)
	switch request.Command {
	case CNIAdd:
		response, err = request.cmdAdd(s.kubeAuth, s.clientSet, s.ovsClient)
	case CNIDel:
		response, err = request.cmdDel(s.clientSet)
	default:
		err = fmt.Errorf("unsupported CNI command %s", request.Command)
	}

	if err != nil {
		return nil, err
	}

	// check if this is a default network request for a pod with primary UDN
	// and create a primary interface if so
	if request.Command == CNIAdd {
		// We don't do anything extra for CNIDel, because UnconfigureInterface already deletes all ports
		// for a given pod from OVS, also some other cleanup are done in batch in cmdDel
		// TODO on primary UDN del VF MAC won't be reset
		primaryPodRequest, err := s.getPrimaryUDNPodRequest(request)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary UDN pod request: %v", err)
		}
		if primaryPodRequest != nil {
			klog.Infof("DEBUG: primary UDN podRequest for pod %s/%s NAD %s: %#v", request.PodNamespace, request.PodName, request.nadName, primaryPodRequest)
			if err = s.setDeviceInfo(primaryPodRequest); err != nil {
				klog.Infof("DEBUG: error setting DRA device info for pod %s/%s NAD %s: %v", request.PodNamespace, request.PodName, request.nadName, err)
				return nil, err
			}
			klog.V(4).Infof("Pod %s/%s primaryUDN podRequest %v", primaryPodRequest.PodNamespace, primaryPodRequest.PodName, primaryPodRequest)
			primaryResponse, err := primaryPodRequest.cmdAdd(s.kubeAuth, s.clientSet, s.ovsClient)
			if err != nil {
				return nil, fmt.Errorf("failed to add primary UDN pod request: %v", err)
			}
			// merge primary response into the original response
			mergePrimaryUDNResponse(response, primaryResponse, primaryPodRequest)
		}
	}

	if result, err = response.Marshal(); err != nil {
		return nil, fmt.Errorf("failed to marshal result: %v", err)
	}

	return result, nil
}

func (s *Server) getPrimaryUDNPodRequest(originalPodRequest *PodRequest) (*PodRequest, error) {
	if !util.IsNetworkSegmentationSupportEnabled() {
		return nil, nil
	}
	// check if this a default network request for a pod with primary UDN
	// and create a primary interface if so
	if originalPodRequest.nadName != types.DefaultNetworkName {
		return nil, nil
	}
	podNamespace := originalPodRequest.PodNamespace
	podName := originalPodRequest.PodName
	// this function is only called after the default network is set up, so the pod should already be in the
	// network manager's cache and have an active network
	activeNetwork, err := s.networkManager.GetActiveNetworkForNamespace(podNamespace)
	if err != nil {
		return nil, err
	}
	// CNI should always have an active network for a pod on our node
	if activeNetwork == nil {
		return nil, fmt.Errorf("no active network found for namespace %s", podNamespace)
	}
	if activeNetwork.IsDefault() {
		// there is no primary NAD
		return nil, nil
	}
	primaryNADKey, err := s.networkManager.GetPrimaryNADForNamespace(podNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary NAD for namespace %s: %v", podNamespace, err)
	}

	primaryPodRequest := &PodRequest{
		Command:      originalPodRequest.Command,
		PodNamespace: originalPodRequest.PodNamespace,
		PodName:      originalPodRequest.PodName,
		PodUID:       originalPodRequest.PodUID,
		SandboxID:    originalPodRequest.SandboxID,
		Netns:        originalPodRequest.Netns,
		IfName:       "ovn-udn1",
		timestamp:    time.Now(),
		ctx:          originalPodRequest.ctx,
		CNIConf: &ovncnitypes.NetConf{
			// primary UDN MTU will be taken from config.Default.MTU
			// if not specified at the NAD
			MTU:     activeNetwork.MTU(),
			NADName: primaryNADKey,
		},
		netName: activeNetwork.GetNetworkName(),
		nadName: primaryNADKey,
		nadKey:  primaryNADKey,
	}
	// To support for non VFIO devices like SRIOV, get the primary UDN's resource name
	nadNamespace, nadName, err := cache.SplitMetaNamespaceKey(primaryNADKey)
	if err != nil {
		return nil, fmt.Errorf("invalid NAD name %s", primaryNADKey)
	}
	nad, err := s.clientSet.nadLister.NetworkAttachmentDefinitions(nadNamespace).Get(nadName)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary UDN's network-attachment-definition %s: %v", nadName, err)
	}
	resourceName := nad.Annotations[resourceNameAnnot]
	if resourceName != "" {
		pod, err := s.clientSet.getPod(podNamespace, podName)
		if err != nil {
			return nil, fmt.Errorf("failed to get pod %s/%s: %v", podNamespace, podName, err)
		}
		deviceID, err := udn.GetPodPrimaryUDNDeviceID(pod, resourceName)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary UDN device ID for pod %s/%s resource %s: %v",
				pod.Namespace, pod.Name, resourceName, err)
		}
		primaryPodRequest.CNIConf.DeviceID = deviceID
	}
	return primaryPodRequest, nil
}

func mergePrimaryUDNResponse(originalResponse, primaryResponse *Response, primaryPodRequest *PodRequest) {
	// merge primary response into the original response
	if originalResponse == nil || primaryResponse == nil {
		return
	}
	if !config.UnprivilegedMode {
		result := originalResponse.Result
		primaryUDNResult := primaryResponse.Result
		result.Routes = append(result.Routes, primaryUDNResult.Routes...)
		numOfInitialIPs := len(result.IPs)
		numOfInitialIfaces := len(result.Interfaces)
		result.Interfaces = append(result.Interfaces, primaryUDNResult.Interfaces...)
		result.IPs = append(result.IPs, primaryUDNResult.IPs...)

		// Offset the index of the default network IPs to correctly point to the default network interfaces
		for i := numOfInitialIPs; i < len(result.IPs); i++ {
			ifaceIPConfig := result.IPs[i].Copy()
			if result.IPs[i].Interface != nil {
				result.IPs[i].Interface = current.Int(*ifaceIPConfig.Interface + numOfInitialIfaces)
			}
		}
	} else {
		originalResponse.PrimaryUDNPodReq = primaryPodRequest
		originalResponse.PrimaryUDNPodInfo = primaryResponse.PodIFInfo
	}
}

func (s *Server) handleCNIMetrics(w http.ResponseWriter, r *http.Request) {
	var cm CNIRequestMetrics

	b, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(b, &cm); err != nil {
		klog.Warningf("Failed to unmarshal JSON (%s) to CNIRequestMetrics struct: %v",
			string(b), err)
	} else {
		hasErr := fmt.Sprintf("%t", cm.HasErr)
		metrics.MetricCNIRequestDuration.WithLabelValues(string(cm.Command), hasErr).Observe(cm.ElapsedTime)
	}
	// Empty response JSON means success with no body
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte{}); err != nil {
		klog.Warningf("Error writing %s HTTP response for metrics post", err)
	}
}

func (s *Server) checkDPUHealth(cmd command) error {
	if s.dpuHealth == nil || config.IsModeDPU() || config.IsModeFull() {
		return nil
	}

	if cmd != CNIAdd && cmd != CNIStatus {
		return nil
	}

	ready, reason := s.dpuHealth.Ready()
	if ready {
		return nil
	}

	msg := dpuNotReadyMsg
	if reason != "" {
		msg = fmt.Sprintf("%s: %s", msg, reason)
	}
	if cmd == CNIStatus {
		return &cnitypes.Error{Code: 50, Msg: msg}
	}
	return fmt.Errorf("%s", msg)
}
