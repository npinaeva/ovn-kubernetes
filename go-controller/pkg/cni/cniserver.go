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
	"strings"
	"time"

	cnitypes "github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/100"
	"github.com/gorilla/mux"
	nadapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	nadv1Listers "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"
	nadutils "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/utils"
	ovncnitypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/cni/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	types2 "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"

	"github.com/ovn-kubernetes/libovsdb/client"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

const kubeletDefaultCRIOperationTimeout = 2 * time.Minute

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
) (*Server, error) {
	var nadLister nadv1Listers.NetworkAttachmentDefinitionLister

	if config.OvnKubeNode.Mode == types.NodeModeDPU {
		return nil, fmt.Errorf("unsupported ovnkube-node mode for CNI server: %s", config.OvnKubeNode.Mode)
	}

	router := mux.NewRouter()

	if util.IsNetworkSegmentationSupportEnabled() {
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
		handlePodRequestFunc: HandlePodRequest,
		networkManager:       networkManager,
		ovsClient:            ovsClient,
		dpuHealth:            dpuHealth,
	}

	if len(config.Kubernetes.CAData) > 0 {
		s.kubeAuth.KubeCAData = base64.StdEncoding.EncodeToString(config.Kubernetes.CAData)
	}

	router.NotFoundHandler = http.HandlerFunc(http.NotFound)
	router.HandleFunc("/metrics", s.handleCNIMetrics).Methods("POST")
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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

func cniRequestToPodRequest(cr *Request) (*PodRequest, error) {
	cmd, ok := cr.Env["CNI_COMMAND"]
	if !ok {
		return nil, fmt.Errorf("unexpected or missing CNI_COMMAND")
	}

	req := &PodRequest{
		Command:   command(cmd),
		timestamp: time.Now(),
	}

	conf, err := config.ReadCNIConfig(cr.Config)
	if err != nil {
		return nil, fmt.Errorf("broken stdin args")
	}
	req.CNIConf = conf
	req.DeviceInfo = cr.DeviceInfo

	// STATUS requests do not carry pod-specific context. Return early after validating config.
	if req.Command == CNIStatus {
		// Match the Kubelet default CRI operation timeout of 2m.
		req.Ctx, req.Cancel = context.WithTimeout(context.Background(), kubeletDefaultCRIOperationTimeout)
		return req, nil
	}

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
	req.NetName = conf.Name
	if req.NetName == types.DefaultNetworkName {
		req.NadName = types.DefaultNetworkName
	} else {
		req.NadName = conf.NADName
	}

	if conf.DeviceID != "" {
		if util.IsPCIDeviceName(conf.DeviceID) {
			// DeviceID is a PCI address
			req.IsVFIO = util.GetSriovnetOps().IsVfPciVfioBound(conf.DeviceID)
		} else if util.IsAuxDeviceName(conf.DeviceID) {
			// DeviceID is an Auxiliary device name - <driver_name>.<kind_of_a_type>.<id>
			chunks := strings.Split(conf.DeviceID, ".")
			if chunks[1] != "sf" {
				return nil, fmt.Errorf("only SF auxiliary devices are supported")
			}
		} else {
			return nil, fmt.Errorf("expected PCI or Auxiliary device name, got - %s", conf.DeviceID)
		}
	}

	// Match the Kubelet default CRI operation timeout of 2m.
	req.Ctx, req.Cancel = context.WithTimeout(context.Background(), kubeletDefaultCRIOperationTimeout)
	return req, nil
}

func cniRequestToPodRequestNew(ctx context.Context, cr *Request, nad *nadapi.NetworkAttachmentDefinition, attachmentIdx int, deviceID string, foundPrimary bool) (*PodRequest, error) {
	cmd, ok := cr.Env["CNI_COMMAND"]
	if !ok {
		return nil, fmt.Errorf("unexpected or missing CNI_COMMAND")
	}
	req := &PodRequest{
		Command:   command(cmd),
		timestamp: time.Now(),
		Ctx:       ctx,
	}
	// STATUS requests do not carry pod-specific context. Return early after validating config.
	if req.Command == CNIStatus {
		return req, nil
	}

	req.SandboxID, ok = cr.Env["CNI_CONTAINERID"]
	if !ok {
		return nil, fmt.Errorf("missing CNI_CONTAINERID")
	}
	req.Netns, ok = cr.Env["CNI_NETNS"]
	if !ok {
		return nil, fmt.Errorf("missing CNI_NETNS")
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

	req.IfName, ok = cr.Env["CNI_IFNAME"]
	if !ok {
		req.IfName = "eth0"
	}

	if nad != nil {
		netconf, err := util.ParseNetConf(nad)
		if err != nil {
			return nil, fmt.Errorf("failed to parse network annotation %s/%s for pod %s/%s: %w", nad.Namespace, nad.Name, req.PodNamespace, req.PodName, err)
		}
		req.IfName = fmt.Sprintf("net%v", attachmentIdx)
		if foundPrimary {
			req.IfName = fmt.Sprintf("net%v", attachmentIdx+1)
		}
		if netconf.Role == types.NetworkRolePrimary {
			req.IfName = "ovn-udn1"
		}
		req.NadName = nad.Namespace + "/" + nad.Name
		req.NetName = netconf.Name

		req.IsVFIO = false
		if deviceID != "" {
			if util.IsPCIDeviceName(deviceID) {
				// DeviceID is a PCI address
				req.IsVFIO = util.GetSriovnetOps().IsVfPciVfioBound(deviceID)
			} else if util.IsAuxDeviceName(deviceID) {
				// DeviceID is an Auxiliary device name - <driver_name>.<kind_of_a_type>.<id>
				chunks := strings.Split(deviceID, ".")
				if chunks[1] != "sf" {
					return nil, fmt.Errorf("only SF auxiliary devices are supported")
				}
			} else {
				return nil, fmt.Errorf("expected PCI or Auxiliary device name, got - %s", deviceID)
			}

			req.IsVFIO = util.GetSriovnetOps().IsVfPciVfioBound(deviceID)
			// TODo
			req.DeviceInfo = nadapi.DeviceInfo{}
		}

		req.CNIConf = netconf
		req.CNIConf.DeviceID = deviceID
	} else {
		req.NadName = types.DefaultNetworkName
		req.NetName = types.DefaultNetworkName
		req.CNIConf = &ovncnitypes.NetConf{}
	}

	return req, nil
}

// Dispatch a pod request to the request handler and return the result to the
// CNI server client
func (s *Server) handleCNIRequest(r *http.Request) ([]byte, error) {
	var cr Request
	b, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(b, &cr); err != nil {
		return nil, err
	}

	cmd, ok := cr.Env["CNI_COMMAND"]
	if !ok {
		return nil, fmt.Errorf("unexpected or missing CNI_COMMAND")
	}
	klog.Infof("DEBUG: Handling CNI command %s for pod request with env %+v", cmd, cr.Env)

	if err := s.checkDPUHealth(command(cmd)); err != nil {
		return nil, err
	}
	switch command(cmd) {
	case CNICheck:
		// noop...CMD check is not considered useful, and has a considerable performance impact
		// to pod bring up times with CRIO. This is due to the fact that CRIO currently calls check
		// after CNI ADD before it finishes bringing the container up
		return nil, nil
	case CNIUpdate:
		// No-op update path today
		return nil, nil
	case CNIStatus:
		// handled by DPU health check gating before reaching here
		return nil, nil
	case CNIAdd:
		break
	case CNIDel:
		break
	default:
		return nil, fmt.Errorf("unsupported CNI command %s", command(cmd))
	}

	ctx, cancel := context.WithTimeout(context.Background(), kubeletDefaultCRIOperationTimeout)
	defer cancel()

	podRequests, err := s.getPodRequests(ctx, &cr)
	if err != nil {
		return nil, err
	}

	response := &Response{
		KubeAuth: s.kubeAuth,
		Result:   &current.Result{},
	}

	for _, request := range podRequests {
		var result *Response
		switch request.Command {
		case CNIAdd:
			result, err = request.CmdAdd(s.kubeAuth, s.clientSet, s.networkManager, s.ovsClient)
		case CNIDel:
			result, err = request.CmdDel(s.clientSet)
		}
		if result != nil {
			response.Result.Routes = append(response.Result.Routes, result.Result.Routes...)
			response.Result.Interfaces = append(response.Result.Interfaces, result.Result.Interfaces...)
			response.Result.IPs = append(response.Result.IPs, result.Result.IPs...)
		}
		if err != nil {
			klog.Infof("DEBUG: CNI command %s for pod request %+v failed with error: %v", request.Command, request, err)
			// Prefix error with request information for easier debugging
			var cniErr *cnitypes.Error
			if !errors.As(err, &cniErr) {
				err = fmt.Errorf("%s %w", request, err)
			}
			return nil, err
		} else {
			klog.Infof("DEBUG: CNI command %s for pod request %+v succeeded with result: %+v", request.Command, request, result)
		}
	}
	var result, resultForLogging []byte
	var err1 error

	if result, err1 = response.Marshal(); err1 != nil {
		return nil, fmt.Errorf("%s %s CNI request %+v failed to marshal result: %v",
			podRequests[0], podRequests[0].Command, response, err1)
	}
	if resultForLogging, err1 = response.MarshalForLogging(); err1 != nil {
		klog.Errorf("%s %s CNI request %+v, %v", podRequests[0], podRequests[0].Command, result, err1)
	}
	klog.Infof("%s %s finished CNI request %+v, result %q, err %v",
		podRequests[0], podRequests[0].Command, string(resultForLogging), err)

	if err != nil {
		// Prefix errors with request info for easier failure debugging
		return nil, fmt.Errorf("%s %v", podRequests[0], err)
	}
	return result, nil
}

func (s *Server) getPodRequests(ctx context.Context, cr *Request) ([]*PodRequest, error) {
	var podRequests []*PodRequest
	cniArgs, err := gatherCNIArgs(cr.Env)
	if err != nil {
		return nil, err
	}

	podNamespace, ok := cniArgs["K8S_POD_NAMESPACE"]
	if !ok {
		return nil, fmt.Errorf("missing K8S_POD_NAMESPACE")
	}

	podName, ok := cniArgs["K8S_POD_NAME"]
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
	podUID := cniArgs["K8S_POD_UID"]

	var nads []*nadapi.NetworkAttachmentDefinition
	var foundPrimary bool
	err = wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(ctx context.Context) (done bool, err error) {
		pod, err := s.clientSet.podLister.Pods(podNamespace).Get(podName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("failed to get pod %s/%s: %v", podNamespace, podName, err)
			}
			return false, nil
		}
		foundPrimary, nads, err = s.getPodAttachments(pod)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("failed to get NADs for pod %s/%s: %v", podNamespace, podName, err)
			}
			return false, nil
		}
		return true, nil
	})
	klog.Infof("DEBUG: found %v nads for pod %s/%s", len(nads), podNamespace, podName)

	// TODO add default network first
	req, err := cniRequestToPodRequestNew(ctx, cr, nil, 0, "", foundPrimary)
	if err != nil {
		return nil, fmt.Errorf("failed to configure default network for pod %s/%s: %w", podNamespace, podName, err)
	}
	podRequests = append(podRequests, req)

	resourceClaims, err := s.clientSet.kclient.ResourceV1().ResourceClaims(podNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list resource claims in namespace %s: %w", podNamespace, err)
	}
	usedClaims := make(map[types2.UID]bool)

	for i, nad := range nads {
		// default network has index 0
		attachmentIdx := i + 1
		klog.Infof("DEBUG: pod %s has NAD annotation %s/%s", podName, nad.Namespace, nad.Name)
		// each NAD attachment with deviceClass annotation should use one claim
		if deviceClassName, ok := nad.Annotations["k8s.ovn.org/deviceClass"]; ok {
			foundClaim := false
			for _, claim := range resourceClaims.Items {
				if claim.Status.Allocation == nil || len(claim.Status.Allocation.Devices.Results) == 0 {
					continue
				}
				if len(claim.Status.ReservedFor) == 0 || claim.Status.ReservedFor[0].UID != types2.UID(podUID) {
					continue
				}
				if claim.Status.Allocation.Devices.Results[0].Driver != "k8s.ovn.org" {
					continue
				}
				if claim.Spec.Devices.Requests[0].Exactly.DeviceClassName == deviceClassName && !usedClaims[claim.UID] {
					usedClaims[claim.UID] = true
					foundClaim = true
					// now find DeviceID for this device
					resourceSlices, err := s.clientSet.kclient.ResourceV1().ResourceSlices().List(context.Background(), metav1.ListOptions{})
					if err != nil {
						return nil, fmt.Errorf("failed to list resource slices: %w", err)
					}
					var deviceID string
					for _, slice := range resourceSlices.Items {
						if slice.Spec.Driver != "k8s.ovn.org" {
							continue
						}
						for _, device := range slice.Spec.Devices {
							if device.Name == claim.Status.Allocation.Devices.Results[0].Device {
								// foudn device
								attr := device.Attributes["k8s.ovn.org/pciAddress"]
								deviceID = attr.String()
							}
						}
					}
					if deviceID == "" {
						return nil, fmt.Errorf("no device ID found for claim %s", claim.Name)
					}
					req, err = cniRequestToPodRequestNew(ctx, cr, nad, attachmentIdx, deviceID, foundPrimary)
					if err != nil {
						return nil, fmt.Errorf("failed to configure device for pod %s: %w", podName, err)
					}
					podRequests = append(podRequests, req)
					break
				}
			}
			if !foundClaim {
				return nil, fmt.Errorf("no resource claim found for nad %s with device class %s", nad.Name, deviceClassName)
			}
		} else {
			// no devices
			req, err = cniRequestToPodRequestNew(ctx, cr, nad, attachmentIdx, "", foundPrimary)
			if err != nil {
				return nil, fmt.Errorf("failed to configure device for pod %s: %w", podName, err)
			}
			podRequests = append(podRequests, req)
		}
	}
	return podRequests, nil
}

func (s *Server) getPodAttachments(pod *corev1.Pod) (bool, []*nadapi.NetworkAttachmentDefinition, error) {
	var nads []*nadapi.NetworkAttachmentDefinition
	foundPrimary := false
	// check primary network, it is important to attach primary nad first for interface order
	// TODO add namespace label check + wait
	allNads, err := s.clientSet.nadLister.NetworkAttachmentDefinitions(pod.Namespace).List(labels.Everything())
	if err != nil {
		return foundPrimary, nil, fmt.Errorf("failed to list NetworkAttachmentDefinitions: %w", err)
	}
	for _, nad := range allNads {
		netconf, err := util.ParseNetConf(nad)
		if err != nil {
			klog.Infof("Failed to parse NetConf for pod %s: %v", pod.Name, err)
			continue
		}
		if netconf.Role == types.NetworkRolePrimary {
			klog.Infof("DEBUG: pod %s has primary NAD annotation %s/%s", pod.Name, nad.Namespace)
			foundPrimary = true
			nads = append(nads, nad)
		}
	}

	// find if this pod has secondary NAD attachments requested
	netAttachment := pod.Annotations[nadapi.NetworkAttachmentAnnot]
	if netAttachment != "" {
		networks, err := nadutils.ParseNetworkAnnotation(netAttachment, pod.Namespace)
		if err != nil {
			return foundPrimary, nil, fmt.Errorf("failed to parse network annotation for pod %s: %w", pod.Name, err)
		}
		for _, net := range networks {
			nad, err := s.clientSet.nadLister.NetworkAttachmentDefinitions(pod.Namespace).Get(net.Name)
			if err != nil {
				return foundPrimary, nil, fmt.Errorf("failed to list NetworkAttachmentDefinitions: %w", err)
			}
			nads = append(nads, nad)
		}
	}

	return foundPrimary, nads, nil
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

func (s *Server) checkDPUHealth(command command) error {
	if s.dpuHealth == nil || config.OvnKubeNode.Mode != types.NodeModeDPUHost {
		return nil
	}

	if command != CNIAdd && command != CNIStatus {
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
	if command == CNIStatus {
		return &cnitypes.Error{Code: 50, Msg: msg}
	}
	return fmt.Errorf("%s", msg)
}
