// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package dra

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jaypipes/ghw"
	"github.com/vishvananda/netlink"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/deviceattribute"
	"k8s.io/utils/ptr"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
)

// PrepareResourceClaims implements the DRA prepare callback.
func (k *NetworkDriver) PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	results := make(map[types.UID]kubeletplugin.PrepareResult)
	for _, claim := range claims {
		// todo make sure claim is not used yet
		deviceName, err := k.prepareDevice(ctx, claim)
		if err != nil {
			results[claim.UID] = kubeletplugin.PrepareResult{Err: err}
			continue
		}
		k.mu.Lock()
		klog.Infof("DEBUG: claim %s allocated device %s", claim.Name, deviceName)
		k.allocatedResources[claim.UID] = deviceName
		k.mu.Unlock()
	}
	return results, nil
}

// UnprepareResourceClaims implements the DRA unprepare callback.
func (k *NetworkDriver) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	if len(claims) == 0 {
		return nil, nil
	}
	result := make(map[types.UID]error)
	for _, claim := range claims {
		result[claim.UID] = k.unprepareDevice(ctx, claim)
		k.mu.Lock()
		delete(k.allocatedResources, claim.UID)
		k.mu.Unlock()
	}
	return result, nil
}

// HandleError is called for background errors.
func (k *NetworkDriver) HandleError(_ context.Context, err error, msg string) {
	runtime.HandleError(fmt.Errorf("%s: %w", msg, err))
}

func (k *NetworkDriver) prepareDevice(_ context.Context, claim *resourceapi.ResourceClaim) (string, error) {
	if claim.Status.Allocation == nil || len(claim.Status.Allocation.Devices.Results) == 0 {
		return "", fmt.Errorf("claim %s has no allocated devices", claim.Name)
	}
	return claim.Status.Allocation.Devices.Results[0].Device, nil
}

func (k *NetworkDriver) unprepareDevice(_ context.Context, _ kubeletplugin.NamespacedObject) error {
	return nil
}

func (k *NetworkDriver) publishResources(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			devices, err := k.GetDevices()
			if err != nil {
				klog.Errorf("Failed to get devices: %v", err)
				continue
			}
			resources := resourceslice.DriverResources{
				Pools: map[string]resourceslice.Pool{
					k.nodeName: {Slices: []resourceslice.Slice{{Devices: devices}}},
				},
			}
			if err := k.draPlugin.PublishResources(ctx, resources); err != nil {
				klog.Errorf("Failed to publish resources: %v", err)
			}
		}
	}
}

func (k *NetworkDriver) GetDevices() ([]resourceapi.Device, error) {
	devices := discoverPCIDevices()
	//pciDeviceMap := make(map[string]*resourceapi.Device)
	//for i := range devices {
	//	pciDeviceMap[devices[i].Name] = &devices[i]
	//}
	//
	//links, err := netlink.LinkList()
	//if err != nil {
	//	return nil, fmt.Errorf("failed to list network interfaces: %w", err)
	//}
	//
	//for _, link := range links {
	//	attrs := link.Attrs()
	//	// TODO publish veth that are down
	//	if attrs.Flags&net.FlagLoopback != 0 {
	//		continue
	//	}
	//	if strings.HasPrefix(attrs.Name, "veth") || strings.HasPrefix(attrs.Name, "docker") || strings.HasPrefix(attrs.Name, "cni") ||
	//		link.Type() == "openvswitch" || link.Type() == "veth" && attrs.Flags&net.FlagUp != 0 {
	//		continue
	//	}
	//
	//	newDevice := &resourceapi.Device{
	//		Name:       NormalizeInterfaceName(attrs.Name),
	//		Attributes: make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute),
	//	}
	//	addLinkAttributes(newDevice, link)
	//	devices = append(devices, *newDevice)
	//}
	return devices, nil
}

func addLinkAttributes(device *resourceapi.Device, link netlink.Link) {
	ifName := link.Attrs().Name
	device.Attributes[AttrInterfaceName] = resourceapi.DeviceAttribute{StringValue: &ifName}
	device.Attributes[AttrMac] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Attrs().HardwareAddr.String())}
	device.Attributes[AttrMTU] = resourceapi.DeviceAttribute{IntValue: ptr.To(int64(link.Attrs().MTU))}
	device.Attributes[AttrEncapsulation] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Attrs().EncapType)}
	device.Attributes[AttrAlias] = resourceapi.DeviceAttribute{StringValue: ptr.To(link.Attrs().Alias)}
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
		if pciDev.Vendor != nil {
			device.Attributes[AttrPCIVendor] = resourceapi.DeviceAttribute{StringValue: &pciDev.Vendor.Name}
		}
		if pciDev.Product != nil {
			device.Attributes[AttrPCIDevice] = resourceapi.DeviceAttribute{StringValue: &pciDev.Product.Name}
		}
		if pciDev.Subsystem != nil {
			device.Attributes[AttrPCISubsystem] = resourceapi.DeviceAttribute{StringValue: &pciDev.Subsystem.ID}
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
		devices = append(devices, device)
	}
	return devices
}

// isNetworkDevice checks the class is 0x2, defined for all types of network controllers
// https://pcisig.com/sites/default/files/files/PCI_Code-ID_r_1_11__v24_Jan_2019.pdf
func isNetworkDevice(dev *ghw.PCIDevice) bool {
	return dev.Class.ID == "02"
}

const (
	// https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-class-net
	sysnetPath = "/sys/class/net/"
)

func sriovTotalVFs(name string) int {
	totalVfsPath := filepath.Join(sysnetPath, name, "/device/sriov_totalvfs")
	totalBytes, err := os.ReadFile(totalVfsPath)
	if err != nil {
		klog.V(7).Infof("error trying to get total VFs for device %s: %v", name, err)
		return 0
	}
	total := bytes.TrimSpace(totalBytes)
	t, err := strconv.Atoi(string(total))
	if err != nil {
		klog.Errorf("Error in obtaining maximum supported number of virtual functions for network interface: %s: %v", name, err)
		return 0
	}
	return t
}

func sriovNumVFs(name string) int {
	numVfsPath := filepath.Join(sysnetPath, name, "/device/sriov_numvfs")
	numBytes, err := os.ReadFile(numVfsPath)
	if err != nil {
		klog.V(7).Infof("error trying to get number of VFs for device %s: %v", name, err)
		return 0
	}
	num := bytes.TrimSpace(numBytes)
	t, err := strconv.Atoi(string(num))
	if err != nil {
		klog.Errorf("Error in obtaining number of virtual functions for network interface: %s: %v", name, err)
		return 0
	}
	return t
}

// isSriovVf reports whether a network interface is a SR-IOV Virtual Function.
// In sysfs this is exposed as a "physfn" symlink under the PCI device.
func isSriovVf(name string, syspath string) bool {
	physfnPath := filepath.Join(syspath, name, "device", "physfn")
	info, err := os.Lstat(physfnPath)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink != 0
}
