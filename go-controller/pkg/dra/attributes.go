/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dra

const (
	AttrPrefix = DriverName

	AttrInterfaceName = AttrPrefix + "/" + "ifName"
	AttrPCIAddress    = AttrPrefix + "/" + "pciAddress"
	AttrMac           = AttrPrefix + "/" + "mac"
	AttrPCIVendor     = AttrPrefix + "/" + "pciVendor"
	AttrNUMANode      = AttrPrefix + "/" + "numaNode"
	AttrState         = AttrPrefix + "/" + "state"
	AttrType          = AttrPrefix + "/" + "type"
	AttrIPv4          = AttrPrefix + "/" + "ipv4"
	AttrIPv6          = AttrPrefix + "/" + "ipv6"
	// PFs supporting SR-IOV are labeled with the attribute "sriov: true".
	AttrSRIOV     = AttrPrefix + "/" + "sriov"
	AttrSRIOVVfs  = AttrPrefix + "/" + "sriovVfs"
	AttrIsSriovVf = AttrPrefix + "/" + "isSriovVf"
	// AttrDriver is the kernel driver currently bound to the PCI device
	// (basename of /sys/bus/pci/devices/<bdf>/driver), e.g. "mlx5_core",
	// "vfio-pci", "iavf". Absent if the device is unbound. Used by DRA
	// selectors to target vfio-pci-bound VFs.
	AttrDriver = AttrPrefix + "/" + "driver"
	// AttrIOMMUGroup is the IOMMU group identifier the device belongs to,
	// in the form used by /dev/vfio/<group>. For vfio-noiommu groups the
	// value is prefixed: "noiommu-<N>". For regular IOMMU groups it is
	// just the numeric ID as a string.
	AttrIOMMUGroup = AttrPrefix + "/" + "iommuGroup"
)
