// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package dra

import (
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
	celtypes "github.com/google/cel-go/common/types"
	"github.com/google/cel-go/ext"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/klog/v2"
)

// Compile parses a CEL filter expression that evaluates over a `Device`'s
// attributes. The variable `attributes` is bound in the CEL environment to
// the device's `map[QualifiedName]DeviceAttribute`. A non-empty `expr`
// returns a runnable cel.Program; an empty `expr` returns (nil, nil) which
// FilterDevices interprets as "no filtering".
func Compile(expr string) (cel.Program, error) {
	if expr == "" {
		return nil, nil
	}
	env, err := cel.NewEnv(
		ext.NativeTypes(reflect.ValueOf(resourceapi.DeviceAttribute{})),
		cel.Variable("attributes", cel.MapType(cel.StringType, cel.ObjectType("v1.DeviceAttribute"))),
	)
	if err != nil {
		return nil, fmt.Errorf("failed creating CEL env: %w", err)
	}
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed CEL compile: %w", issues.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("CEL program: %w", err)
	}
	return prg, nil
}

// FilterDevices keeps only the devices whose attributes make `prg` evaluate
// to true. When `prg` is nil, the input is returned unchanged. Devices that
// cause an evaluation error are kept (fail-open) so a bad expression can't
// silently hide hardware; the error is logged.
func FilterDevices(prg cel.Program, devices []resourceapi.Device) []resourceapi.Device {
	if prg == nil {
		return devices
	}
	out := make([]resourceapi.Device, 0, len(devices))
	for _, dev := range devices {
		val, _, err := prg.Eval(map[string]interface{}{"attributes": dev.Attributes})
		if err != nil {
			klog.Warningf("DRA filter evaluation failed for %s: %v (keeping device)", dev.Name, err)
			out = append(out, dev)
			continue
		}
		b, ok := val.(celtypes.Bool)
		if !ok {
			klog.Warningf("DRA filter for %s returned %T, expected bool (skipping device)", dev.Name, val)
			continue
		}
		if b == celtypes.True {
			out = append(out, dev)
		}
	}
	return out
}
