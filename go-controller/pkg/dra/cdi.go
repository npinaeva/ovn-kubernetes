// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package dra

import (
	"fmt"
	"os"

	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
	cdispec "tags.cncf.io/container-device-interface/specs-go"
)

const (
	// CDISpecDir is the runtime-managed location scanned by containerd /
	// CRI-O for CDI specs.
	CDISpecDir = "/var/run/cdi"
	// cdiClass identifies the kind of devices this driver publishes via CDI.
	// The CDI "kind" is "<vendor>/<class>", here "k8s.ovn.org/network".
	cdiClass = "network"
)

// CDIHandler wraps a CDI spec cache with the lifecycle methods the DRA
// driver uses: a one-time common spec at startup, plus a transient
// per-claim spec written during PrepareResourceClaims and removed during
// UnprepareResourceClaims. Mirrors the structure used by
// kubernetes-sigs/dra-example-driver.
type CDIHandler struct {
	cache      *cdiapi.Cache
	driverName string
	class      string
}

// NewCDIHandler creates a CDIHandler rooted at the given spec directory.
// The directory is created if it doesn't already exist; containerd and
// CRI-O scan /var/run/cdi by default.
func NewCDIHandler(root, driverName, class string) (*CDIHandler, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create CDI spec dir %s: %w", root, err)
	}
	cache, err := cdiapi.NewCache(cdiapi.WithSpecDirs(root))
	if err != nil {
		return nil, fmt.Errorf("create CDI cache: %w", err)
	}
	return &CDIHandler{
		cache:      cache,
		driverName: driverName,
		class:      class,
	}, nil
}

// CreateClaimSpecFile writes a transient per-claim CDI spec containing
// one CDI Device per allocated VFIO device. Container edits include the
// device nodes for /dev/vfio/vfio and /dev/vfio/<group>, plus the PCI
// BDF surfaced as an env var.
func (h *CDIHandler) CreateClaimSpecFile(claimUID string, devices []claimVFIODevice) error {
	if len(devices) == 0 {
		return nil
	}
	specName := cdiapi.GenerateTransientSpecName(h.vendor(), h.class, claimUID)

	spec := &cdispec.Spec{
		Kind:    h.kind(),
		Devices: make([]cdispec.Device, 0, len(devices)),
	}
	for _, d := range devices {
		edits := cdispec.ContainerEdits{
			DeviceNodes: []*cdispec.DeviceNode{
				{Path: "/dev/vfio/vfio", Permissions: "rw", Type: "c"},
				{Path: fmt.Sprintf("/dev/vfio/%s", d.IOMMUGroup), Permissions: "rw", Type: "c"},
			},
		}
		spec.Devices = append(spec.Devices, cdispec.Device{
			Name:           cdiPerClaimDeviceName(claimUID, d.DeviceName),
			ContainerEdits: edits,
		})
	}
	minVersion, err := cdispec.MinimumRequiredVersion(spec)
	if err != nil {
		return fmt.Errorf("minimum CDI version: %w", err)
	}
	spec.Version = minVersion

	return h.cache.WriteSpec(spec, specName)
}

// DeleteClaimSpecFile removes the per-claim CDI spec written by
// CreateClaimSpecFile. Safe to call when no such spec exists.
func (h *CDIHandler) DeleteClaimSpecFile(claimUID string) error {
	specName := cdiapi.GenerateTransientSpecName(h.vendor(), h.class, claimUID)
	return h.cache.RemoveSpec(specName)
}

// GetClaimDevices returns the CDI device IDs that the kubelet should
// pass to the CRI runtime for a claim's allocated devices. The list
// always includes the "common" device first so every container picks
// up the shared env vars, then one entry per allocated device.
func (h *CDIHandler) GetClaimDevices(claimUID string, deviceNames []string) []string {
	out := make([]string, 0, len(deviceNames)+1)
	for _, name := range deviceNames {
		out = append(out, cdiparser.QualifiedName(h.vendor(), h.class, cdiPerClaimDeviceName(claimUID, name)))
	}
	return out
}

func (h *CDIHandler) kind() string   { return h.vendor() + "/" + h.class }
func (h *CDIHandler) vendor() string { return h.driverName }

// claimVFIODevice describes one vfio-pci-bound device to be advertised
// inside a per-claim CDI spec.
type claimVFIODevice struct {
	DeviceName string // matches resourceapi.Device.Name (normalized BDF)
	PCIAddress string // raw BDF
	// IOMMUGroup is the basename of the VFIO character device under
	// /dev/vfio/. For regular IOMMU groups this is the numeric ID
	// ("0", "42"); for vfio-noiommu groups it is "noiommu-<N>".
	IOMMUGroup string
}

// cdiPerClaimDeviceName forms the CDI device name inside a per-claim
// spec: "<claimUID>-<deviceName>". Globally unique because both
// components are unique on their own.
func cdiPerClaimDeviceName(claimUID, deviceName string) string {
	return fmt.Sprintf("%s-%s", claimUID, deviceName)
}
