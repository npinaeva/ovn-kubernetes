// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package dra

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/nri/pkg/api"
	nadapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	nadutils "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/utils"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/cni"
	ovncnitypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/cni/types"
	types2 "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// Synchronize handles initial NRI synchronization.
func (k *NetworkDriver) Synchronize(_ context.Context, pods []*api.PodSandbox, containers []*api.Container) ([]*api.ContainerUpdate, error) {
	klog.Infof("Synchronized state with the runtime (%d pods, %d containers)...",
		len(pods), len(containers))
	return nil, nil
}

// RunPodSandbox is called when a pod is created by the Container Runtime.
func (k *NetworkDriver) RunPodSandbox(_ context.Context, pod *api.PodSandbox) error {
	// pod.Annotations only has anntoations pod was created with
	klog.Infof("DEBUG: RunPodSandbox called for pod %s", pod.Name)
	podUID := types.UID(pod.Uid)
	networkNamespace := getNetworkNamespace(pod)
	if networkNamespace == "" {
		return nil // No network namespace, nothing to configure
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	nads, err := k.getPodAttachments(pod)
	if err != nil {
		return fmt.Errorf("failed to get pod attachments for pod %s: %w", pod.Name, err)
	}
	usedClaims := make(map[types.UID]bool)
	for i, nad := range nads {
		klog.Infof("DEBUG: pod %s has NAD annotation %s/%s, ignoring %v", pod.Name, nad.Namespace, nad.Name, nad.Annotations["k8s.ovn.org/deviceClass"] == "")
		if deviceClassName, ok := nad.Annotations["k8s.ovn.org/deviceClass"]; ok {
			// NRI call
			if deviceClassName == "virtual" {
				err := k.configureDeviceForPod(networkNamespace, pod, nil, nad, i)
				if err != nil {
					return fmt.Errorf("failed to configure virtual device for pod %s: %w", pod.Name, err)
				}
			} else {
				// each NAD attachment with deviceClass annotation should use one claim
				foundClaim := false
				for claimUID, claim := range k.sharedState.ResourceClaims {
					if claim.podUID != podUID {
						continue
					}
					if claim.deviceClassName == deviceClassName && !usedClaims[claimUID] {
						usedClaims[claimUID] = true
						foundClaim = true
						err := k.configureDeviceForPod(networkNamespace, pod, claim, nad, i)
						if err != nil {
							return fmt.Errorf("failed to configure device for pod %s: %w", pod.Name, err)
						}
					}
				}
				if !foundClaim {
					return fmt.Errorf("no resource claim found for nad %s with device class %s", nad.Name, deviceClassName)
				}
			}
		}
	}
	return nil
}

func (k *NetworkDriver) getPodAttachments(pod *api.PodSandbox) ([]*nadapi.NetworkAttachmentDefinition, error) {
	// find if this pod has secondary NAD attachments requested
	netAttachment := pod.Annotations[nadapi.NetworkAttachmentAnnot]
	var nads []*nadapi.NetworkAttachmentDefinition
	if netAttachment != "" {
		networks, err := nadutils.ParseNetworkAnnotation(netAttachment, pod.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to parse network annotation for pod %s: %w", pod.Name, err)
		}
		for _, net := range networks {
			nad, err := k.nadLister.NetworkAttachmentDefinitions(pod.Namespace).Get(net.Name)
			if err != nil {
				return nil, fmt.Errorf("failed to list NetworkAttachmentDefinitions: %w", err)
			}
			nads = append(nads, nad)
		}
	}
	// check primary network, it is important to attach primary nad in the end to no screw up interface name indexing
	// TODO add namespace label check + wait
	allNads, err := k.nadLister.NetworkAttachmentDefinitions(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("failed to list NetworkAttachmentDefinitions: %w", err)
	}
	for _, nad := range allNads {
		netconf, err := util.ParseNetConf(nad)
		if err != nil {
			klog.Infof("Failed to parse NetConf for pod %s: %v", pod.Name, err)
			continue
		}
		if netconf.Role == types2.NetworkRolePrimary {
			klog.Infof("DEBUG: pod %s has primary NAD annotation %s/%s", pod.Name, nad.Namespace)
			nads = append(nads, nad)
		}
	}
	return nads, nil
}

// StopPodSandbox is called when a pod is stopped by the Container Runtime.
func (k *NetworkDriver) StopPodSandbox(_ context.Context, pod *api.PodSandbox) error {
	klog.Infof("DEBUG: StopPodSandbox called for pod %s", pod.Name)
	podUID := types.UID(pod.Uid)
	networkNamespace := getNetworkNamespace(pod)
	if networkNamespace == "" {
		return nil // No network namespace, nothing to configure
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	// TODO check the minimal required info for delete
	// TODO what multus does if NAD doesn't exist on delete
	nads, err := k.getPodAttachments(pod)
	if err != nil {
		return fmt.Errorf("failed to get pod attachments for pod %s: %w", pod.Name, err)
	}

	for i, nad := range nads {
		klog.Infof("DEBUG: pod %s has NAD annotation %s/%s, ignoring %v", pod.Name, nad.Namespace, nad.Name, nad.Annotations["k8s.ovn.org/deviceClass"] == "")
		if deviceClassName, ok := nad.Annotations["k8s.ovn.org/deviceClass"]; ok {
			// NRI call
			if deviceClassName == "virtual" {
				err := k.cleanupDeviceForPod(networkNamespace, pod, nil, nad, i)
				if err != nil {
					return fmt.Errorf("failed to cleanup virtual device for pod %s: %w", pod.Name, err)
				}
			} else {
				foundClaim := false
				for _, claim := range k.sharedState.ResourceClaims {
					if claim.podUID != podUID {
						continue
					}
					if claim.deviceClassName == deviceClassName {
						foundClaim = true
						err := k.cleanupDeviceForPod(networkNamespace, pod, claim, nad, i)
						if err != nil {
							return fmt.Errorf("failed to cleanup device for pod %s: %w", pod.Name, err)
						}
					}
				}
				if !foundClaim {
					return fmt.Errorf("no claim found for pod %s with device class %s", pod.Name, deviceClassName)
				}
			}
		}
	}
	return nil
}

// RemovePodSandbox is called when a pod is removed by the Container Runtime.
func (k *NetworkDriver) RemovePodSandbox(_ context.Context, pod *api.PodSandbox) error {
	podUID := types.UID(pod.Uid)
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.sharedState.ResourceClaims, podUID)
	return nil
}

func makePodRequest(networkNamespace string, pod *api.PodSandbox, claim *ClaimData, nad *nadapi.NetworkAttachmentDefinition, attachmentIdx int) (cni.PodRequest, error) {
	netconf, err := util.ParseNetConf(nad)
	if err != nil {
		return cni.PodRequest{}, fmt.Errorf("failed to parse network annotation for pod %s: %w", pod.Name, err)
	}
	ifName := fmt.Sprintf("net%v", attachmentIdx+1)
	if netconf.Role == types2.NetworkRolePrimary {
		ifName = "ovn-udn1"
	}
	nadName := nad.Namespace + "/" + nad.Name

	deviceID := ""
	isVFIO := false
	if claim != nil {
		deviceID = claim.deviceName
		if deviceID != "" {
			isVFIO = util.GetSriovnetOps().IsVfPciVfioBound(deviceID)
		}
	}

	cniConf := &ovncnitypes.NetConf{
		DeviceID: deviceID,
		MTU:      netconf.MTU,
	}

	podrequest := cni.PodRequest{
		Command:      cni.CNIAdd,
		PodNamespace: pod.Namespace,
		PodName:      pod.Name,
		PodUID:       pod.Uid,
		SandboxID:    pod.Id,
		Netns:        networkNamespace,
		IfName:       ifName,
		CNIConf:      cniConf,
		// TODO
		IsVFIO:     isVFIO,
		NetName:    netconf.Name,
		NadName:    nadName,
		DeviceInfo: nadapi.DeviceInfo{},
	}
	podrequest.Ctx, podrequest.Cancel = context.WithTimeout(context.Background(), time.Minute)
	return podrequest, nil
}

func (k *NetworkDriver) configureDeviceForPod(networkNamespace string, pod *api.PodSandbox, claim *ClaimData,
	nad *nadapi.NetworkAttachmentDefinition, attachmentIdx int) error {

	podrequest, err := makePodRequest(networkNamespace, pod, claim, nad, attachmentIdx)
	if err != nil {
		return fmt.Errorf("failed to make pod request for pod %s: %w", pod.Name, err)
	}
	klog.Infof("DEBUG: preparing to configure device %s for pod %s in network namespace %s", podrequest.CNIConf.DeviceID, pod.Name, networkNamespace)

	res, err := podrequest.CmdAdd(nil, k.clientset, k.networkManager, k.ovsClient)
	if err != nil {
		klog.Infof("DEBUG: NRI Add failed for pod %s: %v", pod.Name, err)
		return fmt.Errorf("NRI Add failed for pod %s: %w", pod.Name, err)
	}
	klog.Infof("DEBUG: NRI Add result for pod %s: %+v, error: %v", pod.Name, res.Result, err)
	return nil
}

func (k *NetworkDriver) cleanupDeviceForPod(networkNamespace string, pod *api.PodSandbox, claim *ClaimData,
	nad *nadapi.NetworkAttachmentDefinition, attachmentIdx int) error {

	// TODO check if I can do cleanup without NADs, probably using pod annotations

	podrequest, err := makePodRequest(networkNamespace, pod, claim, nad, attachmentIdx)
	if err != nil {
		return fmt.Errorf("failed to make pod request for pod %s: %w", pod.Name, err)
	}

	klog.Infof("DEBUG: preparing to cleanup device %s for pod %s in network namespace %s", podrequest.CNIConf.DeviceID, pod.Name, networkNamespace)

	res, err := podrequest.CmdDel(k.clientset)
	if err != nil {
		klog.Infof("DEBUG: NRI DEL failed for pod %s: %v", pod.Name, err)
		return fmt.Errorf("NRI DEL failed for pod %s: %w", pod.Name, err)
	}
	klog.Infof("DEBUG: NRI DEL result for pod %s: %v, error: %v", pod.Name, res.Result, err)
	return nil
}

func getNetworkNamespace(pod *api.PodSandbox) string {
	if pod.GetLinux() == nil {
		return ""
	}
	for _, nsRef := range pod.GetLinux().GetNamespaces() {
		if nsRef.GetType() == "network" {
			return nsRef.GetPath()
		}
	}
	return ""
}
