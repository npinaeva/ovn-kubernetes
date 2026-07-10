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

import (
	"strings"
)

const (
	// NormalizedPCIPrefix is the prefix used when normalizing a PCI Address.
	NormalizedPCIPrefix = "pci"
)

// NormalizePCIAddress takes a PCI address and converts it into a DNS-1123
// acceptable format.
func NormalizePCIAddress(pciAddress string) string {
	// Replace ":" and "." with "-" to make it DNS-1123 compliant.
	// A PCI address like "0000:8a:00.0" becomes "0000-8a-00-0".
	r := strings.NewReplacer(":", "-", ".", "-")
	return NormalizedPCIPrefix + "-" + r.Replace(pciAddress)
}
