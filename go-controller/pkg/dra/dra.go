// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package dra

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jaypipes/ghw"
	"github.com/vishvananda/netlink"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/deviceattribute"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

const DeviceClassAnnotation = "k8s.ovn.org/deviceClass"

// PrepareResourceClaims implements the DRA prepare callback.
func (d *NetworkDriver) PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	results := make(map[types.UID]kubeletplugin.PrepareResult)
	for _, claim := range claims {
		cdiDevices, err := d.prepareResourceClaim(ctx, claim)
		if err != nil {
			results[claim.UID] = kubeletplugin.PrepareResult{Err: err}
			continue
		}
		results[claim.UID] = kubeletplugin.PrepareResult{Devices: cdiDevices}
	}
	klog.Infof("DEBUG: Prepared claims: %+v", results)
	return results, nil
}

// UnprepareResourceClaims implements the DRA unprepare callback.
func (d *NetworkDriver) UnprepareResourceClaims(_ context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	if len(claims) == 0 {
		return nil, nil
	}
	result := make(map[types.UID]error)
	d.sharedState.mu.Lock()
	for _, claim := range claims {
		klog.Infof("DEBUG: Unpreparing claim %s for podUID %s", claim.UID, d.claimToPodUID[claim.UID])
		delete(d.sharedState.AllocatedResources, d.claimToPodUID[claim.UID])
		delete(d.claimToPodUID, claim.UID)
		result[claim.UID] = nil
	}
	d.sharedState.mu.Unlock()
	// Best-effort: delete the per-claim CDI spec file if one was written
	// during Prepare. RemoveSpec is idempotent — fine for claims that
	// never had a VFIO device.
	if d.cdi != nil {
		for _, claim := range claims {
			if err := d.cdi.DeleteClaimSpecFile(string(claim.UID)); err != nil {
				klog.Errorf("DRA: failed to delete CDI spec for claim %s: %v", claim.UID, err)
			}
		}
	}
	return result, nil
}

// HandleError is called for background errors.
func (d *NetworkDriver) HandleError(_ context.Context, err error, msg string) {
	runtime.HandleError(fmt.Errorf("%s: %w", msg, err))
}

func (d *NetworkDriver) prepareResourceClaim(_ context.Context, claim *resourceapi.ResourceClaim) ([]kubeletplugin.Device, error) {
	klog.V(2).Infof("PrepareResourceClaim Claim %s/%s", claim.Namespace, claim.Name)
	if claim.Status.Allocation == nil {
		return nil, nil
	}
	podUIDs := []types.UID{}
	for _, reserved := range claim.Status.ReservedFor {
		if reserved.Resource != "pods" || reserved.APIGroup != "" {
			klog.Infof("Driver only supports Pods, unsupported reference %#v", reserved)
			continue
		}
		podUIDs = append(podUIDs, reserved.UID)
	}

	var (
		cdiResultDevices []kubeletplugin.Device
		vfioDevs         []claimVFIODevice
	)
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver != d.driverName {
			continue
		}
		// find deviceClass
		requestParts := strings.Split(result.Request, "/")
		requestName := requestParts[0]
		subrequestName := ""
		if len(requestParts) == 2 {
			subrequestName = requestParts[1]
		}
		deviceClassName := ""
		for _, request := range claim.Spec.Devices.Requests {
			if request.Name != requestName {
				continue
			}
			if request.Exactly != nil && request.Exactly.DeviceClassName != "" {
				deviceClassName = request.Exactly.DeviceClassName
			}
			if request.FirstAvailable != nil {
				for _, sub := range request.FirstAvailable {
					if sub.Name == subrequestName { // res.SubRequest is the winner
						deviceClassName = sub.DeviceClassName
					}
				}
			}
		}
		if deviceClassName == "" {
			return nil, fmt.Errorf("device class name not found for claim %s/%s request %s", claim.Namespace, claim.Name, result.Request)
		}
		if len(podUIDs) == 0 {
			return nil, fmt.Errorf("no pod reference found for claim %s/%s", claim.Namespace, claim.Name)
		}
		if len(podUIDs) > 1 {
			return nil, fmt.Errorf("multiple pod references found for claim %s/%s: %v", claim.Namespace, claim.Name, podUIDs)
		}
		klog.Infof("DEBUG: Allocating device %s of class %s for claim %s/%s to pod UID %s", result.Device, deviceClassName, claim.Namespace, claim.Name, podUIDs[0])
		// If the device's interface mapping was already written to
		// claim.Status.Devices by a previous CmdAdd, recover it so the
		// interface↔device pairing survives ovnkube-node restarts without
		// requiring a re-lookup on CmdDel.
		ifaceName := ""
		for _, dev := range claim.Status.Devices {
			if dev.Driver == d.driverName && dev.Pool == d.nodeName && dev.Device == result.Device {
				if dev.NetworkData != nil {
					ifaceName = dev.NetworkData.InterfaceName
				}
				break
			}
		}
		d.sharedState.mu.Lock()
		if d.sharedState.AllocatedResources[podUIDs[0]] == nil {
			d.sharedState.AllocatedResources[podUIDs[0]] = make(map[string]*AllocatedDevice)
		}
		d.sharedState.AllocatedResources[podUIDs[0]][result.Device] = &AllocatedDevice{
			DeviceClass: deviceClassName,
			ClaimUID:    claim.UID,
			ClaimName:   claim.Name,
			IfaceName:   ifaceName,
		}
		d.claimToPodUID[claim.UID] = podUIDs[0]
		deviceAttrs := d.sharedState.DeviceAttributes[result.Device]
		d.sharedState.mu.Unlock()

		isVFIO := deviceAttrs[AttrDriver] == "vfio-pci"
		// Only VFIO needs CDI device injection: kernel netdev VFs are
		// surfaced inside the pod via the CNI/OVS path, no /dev/* files
		// to mount. Anything else returns no CDI device IDs.
		if !isVFIO {
			continue
		}
		vfioDevs = append(vfioDevs, claimVFIODevice{
			DeviceName: result.Device,
			PCIAddress: deviceAttrs[AttrPCIAddress],
			IOMMUGroup: deviceAttrs[AttrIOMMUGroup],
		})
		cdiResultDevices = append(cdiResultDevices, kubeletplugin.Device{
			Requests:   []string{result.Request},
			PoolName:   d.nodeName,
			DeviceName: result.Device,
			// write it now: if file creation fails, we return error and empty result anyway
			CDIDeviceIDs: d.cdi.GetClaimDevices(string(claim.UID), []string{result.Device}),
		})
		klog.Infof("DEBUG: CDI result %+v", cdiResultDevices)
	}

	// Write the per-claim CDI spec once, then stitch the CDI IDs back
	// onto each device result. GetClaimDevices includes the common
	// device first so every container picks up the driver-wide env vars.
	if len(vfioDevs) > 0 && d.cdi != nil {
		if err := d.cdi.CreateClaimSpecFile(string(claim.UID), vfioDevs); err != nil {
			return nil, fmt.Errorf("write per-claim CDI spec for %s: %w", claim.UID, err)
		}
	}
	return cdiResultDevices, nil
}

func (d *NetworkDriver) publishResources(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			devices, err := d.GetDevices()
			if err != nil {
				klog.Errorf("Failed to get devices: %v", err)
				continue
			}
			devices = FilterDevices(d.filterProgram, devices)
			resources := resourceslice.DriverResources{
				Pools: map[string]resourceslice.Pool{
					d.nodeName: {Slices: []resourceslice.Slice{{Devices: devices}}},
				},
			}
			if err := d.draPlugin.PublishResources(ctx, resources); err != nil {
				klog.Errorf("Failed to publish resources: %v", err)
			}
		}
	}
}

func (d *NetworkDriver) GetDevices() ([]resourceapi.Device, error) {
	devices := discoverPCIDevices()
	pciDeviceMap := make(map[string]*resourceapi.Device)
	for i := range devices {
		pciDeviceMap[devices[i].Name] = &devices[i]
	}

	links, err := netlink.LinkList()
	if err != nil {
		return nil, fmt.Errorf("failed to list network interfaces: %w", err)
	}

	for _, link := range links {
		ifName := link.Attrs().Name
		attrs := link.Attrs()
		if attrs.Flags&net.FlagLoopback != 0 {
			continue
		}
		// filter out devices
		if strings.HasPrefix(attrs.Name, "veth") || strings.HasPrefix(attrs.Name, "docker") || strings.HasPrefix(attrs.Name, "cni") ||
			link.Type() == "openvswitch" || link.Type() == "veth" && attrs.Flags&net.FlagUp != 0 || link.Type() == "geneve" || link.Type() == "vrf" {
			continue
		}

		pciAddr, err := pciAddressForNetInterface(ifName)
		if err != nil {
			continue
		}
		// It's a PCI device.

		normalizedAddress := NormalizePCIAddress(pciAddr.String())
		var exists bool
		device, exists := pciDeviceMap[normalizedAddress]
		if !exists {
			// We don't expect this to happen.
			klog.Errorf("Network interface %s has PCI address %q, but it was not found in initial PCI scan.", ifName, pciAddr)
			continue
		}
		addLinkAttributes(device, link)
	}
	// add devices to storage
	d.sharedState.mu.Lock()
	defer d.sharedState.mu.Unlock()
	for _, device := range devices {
		d.sharedState.DeviceAttributes[device.Name] = map[string]string{}
		for attrName, attr := range device.Attributes {
			if attr.StringValue != nil {
				d.sharedState.DeviceAttributes[device.Name][string(attrName)] = *attr.StringValue
			} else if attr.IntValue != nil {
				d.sharedState.DeviceAttributes[device.Name][string(attrName)] = fmt.Sprintf("%d", *attr.IntValue)
			} else if attr.BoolValue != nil {
				d.sharedState.DeviceAttributes[device.Name][string(attrName)] = fmt.Sprintf("%t", *attr.BoolValue)
			}
		}
	}
	return devices, nil
}

func supportedDevice(deviceID string) bool {
	if util.IsPCIDeviceName(deviceID) {
		if util.IsLegacyVFPCI(deviceID) {
			// only allowed testing
			return true
		}
		_, err := util.GetFunctionRepresentorName(deviceID)
		if err != nil {
			return false
		}
	} else if util.IsAuxDeviceName(deviceID) {
		// DeviceID is an Auxiliary device name - <driver_name>.<kind_of_a_type>.<id>
		chunks := strings.Split(deviceID, ".")
		if len(chunks) < 2 {
			return false
		}
		if chunks[1] != "sf" {
			return false
		}
		if util.GetSriovnetOps().IsVfPciVfioBound(deviceID) {
			// VFIO is not supported for Aux devices
			return false
		}
	}
	// else it is a netdev name, which is used for simulated DPU environments.
	return util.IsSimulatedDPU()
}

func addLinkAttributes(device *resourceapi.Device, link netlink.Link) {
	ifName := link.Attrs().Name
	device.Attributes[AttrInterfaceName] = resourceapi.DeviceAttribute{StringValue: &ifName}
	device.Attributes[AttrMac] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Attrs().HardwareAddr.String())}
	device.Attributes[AttrState] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Attrs().OperState.String())}
	device.Attributes[AttrType] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Type())}

	v4 := sets.Set[string]{}
	v6 := sets.Set[string]{}
	if ips, err := netlink.AddrList(link, netlink.FAMILY_ALL); err == nil && len(ips) > 0 {
		for _, address := range ips {
			if !address.IP.IsGlobalUnicast() {
				continue
			}

			if address.IP.To4() == nil && address.IP.To16() != nil {
				v6.Insert(address.IPNet.String())
			} else if address.IP.To4() != nil {
				v4.Insert(address.IPNet.String())
			}
		}
		if v4.Len() > 0 {
			device.Attributes[AttrIPv4] = resourceapi.DeviceAttribute{StringValue: ptr.To(strings.Join(v4.UnsortedList(), ","))}
		}
		if v6.Len() > 0 {
			device.Attributes[AttrIPv6] = resourceapi.DeviceAttribute{StringValue: ptr.To(strings.Join(v6.UnsortedList(), ","))}
		}
	}

	isSRIOV := sriovTotalVFs(ifName) > 0
	device.Attributes[AttrSRIOV] = resourceapi.DeviceAttribute{BoolValue: &isSRIOV}
	if isSRIOV {
		vfs := int64(sriovNumVFs(ifName))
		device.Attributes[AttrSRIOVVfs] = resourceapi.DeviceAttribute{IntValue: &vfs}
	}

	isSriovVirtualFunction := isSriovVf(ifName, sysnetPath)
	if isSriovVirtualFunction {
		device.Attributes[AttrIsSriovVf] = resourceapi.DeviceAttribute{BoolValue: &isSriovVirtualFunction}
	}
}

func discoverPCIDevices() []resourceapi.Device {
	devices := []resourceapi.Device{}

	pci, err := ghw.PCI(
		ghw.WithDisableTools(),
	)
	if err != nil {
		klog.Errorf("Could not get PCI devices: %v", err)
		return devices
	}

	for _, pciDev := range pci.Devices {
		if !isNetworkDevice(pciDev) {
			continue
		}
		device := resourceapi.Device{
			Name:       NormalizePCIAddress(pciDev.Address),
			Attributes: make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute),
			Capacity:   make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity),
		}
		device.Attributes[AttrPCIAddress] = resourceapi.DeviceAttribute{StringValue: &pciDev.Address}
		if !supportedDevice(pciDev.Address) {
			klog.Infof("DEBUG: Skipping unsupported device at PCI address %s", pciDev.Address)
			continue
		}
		if pciDev.Vendor != nil {
			device.Attributes[AttrPCIVendor] = resourceapi.DeviceAttribute{StringValue: &pciDev.Vendor.Name}
		}

		if pciDev.Node != nil {
			device.Attributes[AttrNUMANode] = resourceapi.DeviceAttribute{IntValue: ptr.To(int64(pciDev.Node.ID))}
		}

		pcieRootAttr, err := deviceattribute.GetPCIeRootAttributeByPCIBusID(pciDev.Address)
		if err != nil {
			klog.Infof("Could not get pci root attribute: %v", err)
		} else {
			device.Attributes[pcieRootAttr.Name] = pcieRootAttr.Value
		}

		if driver := pciDriverName(pciDev.Address); driver != "" {
			device.Attributes[AttrDriver] = resourceapi.DeviceAttribute{StringValue: ptr.To(driver)}
		}
		if group, err := pciIOMMUGroup(pciDev.Address); err == nil {
			groupName := strconv.Itoa(group)
			if pciIOMMUGroupIsNoIOMMU(group) {
				groupName = "noiommu-" + groupName
			}
			device.Attributes[AttrIOMMUGroup] = resourceapi.DeviceAttribute{StringValue: ptr.To(groupName)}
		}

		devices = append(devices, device)
	}
	return devices
}

// isNetworkDevice checks the class is 0x2, defined for all types of network controllers
// https://pcisig.com/sites/default/files/files/PCI_Code-ID_r_1_11__v24_Jan_2019.pdf
func isNetworkDevice(dev *ghw.PCIDevice) bool {
	return dev.Class.ID == "02"
}
