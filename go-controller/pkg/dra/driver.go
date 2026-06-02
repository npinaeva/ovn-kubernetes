// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package dra

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/cel-go/cel"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
)

const (
	DriverName = "k8s.ovn.org"
)

// AllocatedDevice represents a network device that has been allocated to a pod.
// The device's name is the key of the per-pod map in SharedState.AllocatedResources;
// it's not stored on the value to avoid drift between key and stored name.
type AllocatedDevice struct {
	DeviceClass string
	ClaimUID    types.UID
	// ClaimName is the name of the ResourceClaim that produced this allocation;
	// its namespace is the pod's namespace. Used to write back device status.
	ClaimName string
	// since a single pod can have multiple requests with the same device class,
	// we need to track which interface has already used which claim
	IfaceName string
}

type DeviceAttributes map[string]string

// SharedState is shared between DRA hooks and NRI hooks.
type SharedState struct {
	mu sync.Mutex
	// AllocatedResources maps a pod UID to its allocated devices, keyed by
	// device name (so we can look up a specific device for a pod in O(1)
	// and avoid duplicates).
	AllocatedResources map[types.UID]map[string]*AllocatedDevice
	DeviceAttributes   map[string]DeviceAttributes
	// nodeName is the name of the node this driver is running on; it equals
	// the ResourceSlice pool name.
	nodeName string
}

type DRAState interface {
	// GetDeviceAttributes returns the attributes of a device by its name.
	GetDeviceAttributes(deviceName string) (DeviceAttributes, bool)
	// GetAllocatedDevices returns the allocated devices for a pod by its UID,
	// keyed by device name.
	GetAllocatedDevices(podUID string) (map[string]*AllocatedDevice, bool)
	// UpdateAllocatedDevice replaces a single allocated device entry for a pod,
	// keyed by deviceName. The pod's allocation map is created if needed.
	UpdateAllocatedDevice(podUID string, deviceName string, device *AllocatedDevice)
	// NodeName returns the node name the driver is running on (== ResourceSlice pool name).
	NodeName() string
}

func (s *SharedState) NodeName() string { return s.nodeName }

func (s *SharedState) GetDeviceAttributes(deviceName string) (DeviceAttributes, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attrs, exists := s.DeviceAttributes[deviceName]
	return attrs, exists
}

func (s *SharedState) GetAllocatedDevices(podUID string) (map[string]*AllocatedDevice, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	devices, exists := s.AllocatedResources[types.UID(podUID)]
	return devices, exists
}

func (s *SharedState) UpdateAllocatedDevice(podUID string, deviceName string, device *AllocatedDevice) {
	s.mu.Lock()
	defer s.mu.Unlock()
	uid := types.UID(podUID)
	if s.AllocatedResources[uid] == nil {
		s.AllocatedResources[uid] = make(map[string]*AllocatedDevice)
	}
	s.AllocatedResources[uid][deviceName] = device
}

// NetworkDriver manages lifecycle of DRA and NRI plugins for one node.
type NetworkDriver struct {
	driverName string
	nodeName   string
	kubeClient kubernetes.Interface
	draPlugin  *kubeletplugin.Helper

	// filterProgram, when non-nil, is applied to the device list before
	// publishing so only matching devices appear in the node's ResourceSlice.
	// See pkg/dra/filter.
	filterProgram cel.Program

	sharedState   *SharedState
	claimToPodUID map[types.UID]types.UID
	cdi           *CDIHandler
}

// New creates a DRA controller for a node. filterProgram (optional, nil to
// disable) filters the published device list by CEL expression.
func New(nodeName string, kubeClient kubernetes.Interface, filterProgram cel.Program) *NetworkDriver {
	return &NetworkDriver{
		driverName:    DriverName,
		nodeName:      nodeName,
		kubeClient:    kubeClient,
		filterProgram: filterProgram,
		sharedState: &SharedState{
			AllocatedResources: make(map[types.UID]map[string]*AllocatedDevice),
			DeviceAttributes:   make(map[string]DeviceAttributes),
			nodeName:           nodeName,
		},
		claimToPodUID: make(map[types.UID]types.UID),
	}
}

func (d *NetworkDriver) GetDRAState() DRAState {
	return d.sharedState
}

// Start initializes and runs DRA plugin.
// TODO create ClaimTemplates on NAD creation?
func (d *NetworkDriver) Start(ctx context.Context) error {
	driverPluginPath := filepath.Join(kubeletplugin.KubeletPluginsDir, d.driverName)
	if err := os.MkdirAll(driverPluginPath, 0o750); err != nil {
		return fmt.Errorf("failed to create plugin path %s: %w", driverPluginPath, err)
	}

	cdi, err := NewCDIHandler(CDISpecDir, d.driverName, cdiClass)
	if err != nil {
		return fmt.Errorf("init CDI handler: %w", err)
	}
	//if err := cdi.CreateCommonSpecFile(); err != nil {
	//	return fmt.Errorf("write common CDI spec: %w", err)
	//}
	d.cdi = cdi

	kubeletOptions := []kubeletplugin.Option{
		kubeletplugin.DriverName(d.driverName),
		kubeletplugin.NodeName(d.nodeName),
		kubeletplugin.KubeClient(d.kubeClient),
	}
	draHelper, err := kubeletplugin.Start(ctx, d, kubeletOptions...)
	if err != nil {
		return fmt.Errorf("start kubelet plugin: %w", err)
	}
	d.draPlugin = draHelper

	if err := wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(context.Context) (bool, error) {
		status := d.draPlugin.RegistrationStatus()
		return status != nil && status.PluginRegistered, nil
	}); err != nil {
		return err
	}

	if err := d.reconcileExistingClaims(ctx); err != nil {
		klog.Errorf("Failed to reconcile existing ResourceClaims at startup: %v", err)
	}

	go d.publishResources(ctx)
	return nil
}

// reconcileExistingClaims lists every ResourceClaim in the cluster that was
// allocated to a device from this node's pool and re-populates the in-memory
// AllocatedResources map. This is called once after the kubelet plugin is
// registered to recover state across an ovnkube-node restart while pods that
// already hold an allocation are still running on the node.
func (d *NetworkDriver) reconcileExistingClaims(ctx context.Context) error {
	list, err := d.kubeClient.ResourceV1().ResourceClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing ResourceClaims: %w", err)
	}
	count := 0
	for i := range list.Items {
		claim := &list.Items[i]
		if claim.Status.Allocation == nil {
			continue
		}
		// Only reconcile claims with at least one allocation result from this
		// node's pool. Pool name equals the node name in publishResources.
		fromThisNode := false
		for _, r := range claim.Status.Allocation.Devices.Results {
			if r.Driver == d.driverName && r.Pool == d.nodeName {
				fromThisNode = true
				break
			}
		}
		if !fromThisNode {
			continue
		}
		// CDI device IDs returned here are discarded.
		// We just want the SharedState rebuilt so CNI lookups work for running pods.
		if _, err := d.prepareResourceClaim(ctx, claim); err != nil {
			klog.Errorf("Reconciling existing ResourceClaim %s/%s failed: %v", claim.Namespace, claim.Name, err)
			continue
		}
		count++
	}
	klog.Infof("DRA: reconciled %d existing allocated ResourceClaim(s) on node %s", count, d.nodeName)
	return nil
}

// Stop gracefully stops DRA and NRI plugins.
func (d *NetworkDriver) Stop() {
	if d.draPlugin != nil {
		d.draPlugin.Stop()
	}
}
