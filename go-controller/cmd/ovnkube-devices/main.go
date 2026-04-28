// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/dra"
)

func main() {
	d := new(dra.NetworkDriver)
	devices, err := d.GetDevices()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error discovering devices: %v\n", err)
		os.Exit(1)
	}
	out, err := json.MarshalIndent(devices, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error marshaling output: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}
