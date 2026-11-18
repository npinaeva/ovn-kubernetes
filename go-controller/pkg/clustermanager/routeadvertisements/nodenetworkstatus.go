// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package routeadvertisements

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	frrtypes "github.com/metallb/frr-k8s/api/v1beta1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"

	nnstypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/nodenetworkstatus/v1alpha1"
	ratypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/routeadvertisements/v1"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
)

// maxReportedNodeFailures bounds the node failures spelled out in the RA
// status, so a cluster-wide problem does not produce an unreadable condition.
const maxReportedNodeFailures = 3

// nodeGeneration is the per-node outcome of generating the configuration of a
// RouteAdvertisements.
type nodeGeneration struct {
	// networkVRFs maps every selected network to the VRF it is advertised
	// in: "default" for the default VRF, never "auto".
	networkVRFs map[string]string
	// advertised maps each node whose configuration was generated to the
	// networks advertised from it.
	advertised map[string][]string
	// failed maps each node whose configuration could not be generated to
	// the cause. Other nodes are not affected by these failures.
	failed map[string]error
}

// nodeFailuresError reports nodes whose configuration could not be generated
// while the configuration of other nodes was applied. It deliberately wraps
// neither errConfig nor errPending: the RouteAdvertisements stays accepted and
// is reported as degraded instead.
type nodeFailuresError struct {
	failed map[string]error
}

func (e *nodeFailuresError) Error() string {
	nodes := sets.List(sets.KeySet(e.failed))
	reported := make([]string, 0, maxReportedNodeFailures)
	for _, node := range nodes[:min(len(nodes), maxReportedNodeFailures)] {
		reported = append(reported, fmt.Sprintf("%s: %v", node, e.failed[node]))
	}
	msg := fmt.Sprintf("failed generating configuration for %d node(s): %s", len(nodes), strings.Join(reported, "; "))
	if len(nodes) > maxReportedNodeFailures {
		msg += fmt.Sprintf("; and %d more", len(nodes)-maxReportedNodeFailures)
	}
	return msg
}

// retriable reports whether any node failed for a reason that an update of a
// watched object will not resolve, so the reconcile has to be retried.
func (e *nodeFailuresError) retriable() bool {
	for _, err := range e.failed {
		if !errors.Is(err, errConfig) && !errors.Is(err, errPending) {
			return true
		}
	}
	return false
}

// advertisedVRF returns the VRF a network is advertised in for a target VRF.
func advertisedVRF(targetVRF, networkVRF string) string {
	switch targetVRF {
	case "":
		return types.DefaultNetworkName
	case "auto":
		return networkVRF
	default:
		return targetVRF
	}
}

// nodeNetworkStatusName returns the name of the object for a node and a
// network. Network names are not valid object names, so they are hashed; the
// real values are in the spec.
func nodeNetworkStatusName(node, network string) string {
	sum := sha256.Sum256([]byte(network))
	suffix := "." + hex.EncodeToString(sum[:8])
	if len(node)+len(suffix) <= 253 {
		return node + suffix
	}
	sum = sha256.Sum256([]byte(node + "/" + network))
	return hex.EncodeToString(sum[:])
}

// nodeNetworkStatusEntry is what a RouteAdvertisements reports for a network
// it advertises from a node.
func nodeNetworkStatusEntry(ra *ratypes.RouteAdvertisements, vrf string) nnstypes.NodeNetworkRouteAdvertisement {
	entry := nnstypes.NodeNetworkRouteAdvertisement{Name: ra.Name, VRF: vrf}
	for _, advertisement := range ra.Spec.Advertisements {
		entry.Advertisements = append(entry.Advertisements, nnstypes.AdvertisementType(advertisement))
	}
	slices.Sort(entry.Advertisements)
	return entry
}

func sameNodeNetworkStatusEntry(a, b nnstypes.NodeNetworkRouteAdvertisement) bool {
	return a.Name == b.Name && a.VRF == b.VRF && slices.Equal(a.Advertisements, b.Advertisements)
}

// desiredNodeNetworkStatuses returns, by node and network, the entries a
// RouteAdvertisements should have after its configuration was generated.
func desiredNodeNetworkStatuses(ra *ratypes.RouteAdvertisements, generation nodeGeneration) map[string]map[string]nnstypes.NodeNetworkRouteAdvertisement {
	desired := map[string]map[string]nnstypes.NodeNetworkRouteAdvertisement{}
	if ra == nil {
		return desired
	}
	for node, networks := range generation.advertised {
		desired[node] = make(map[string]nnstypes.NodeNetworkRouteAdvertisement, len(networks))
		for _, network := range networks {
			desired[node][network] = nodeNetworkStatusEntry(ra, generation.networkVRFs[network])
		}
	}
	return desired
}

// generatedNodes returns the nodes that generated FRRConfigurations apply to.
func generatedNodes(frrConfigs []*frrtypes.FRRConfiguration) sets.Set[string] {
	nodes := sets.New[string]()
	for _, frrConfig := range frrConfigs {
		if frrConfig.Annotations[types.OvnRouteAdvertisementsKey] == "" {
			continue
		}
		if node := frrConfig.Spec.NodeSelector.MatchLabels["kubernetes.io/hostname"]; node != "" {
			nodes.Insert(node)
		}
	}
	return nodes
}

// preservedNodeNetworkStatuses returns a filter for the entries of nodes whose
// configuration could not be regenerated. Their previous FRRConfigurations are
// left in place, so an entry stays only while those still publish what the
// RouteAdvertisements asks for now. If the target VRF changed, for example,
// the old routes are in the wrong VRF and the node must go back to
// translating pod traffic.
func (c *Controller) preservedNodeNetworkStatuses(ra *ratypes.RouteAdvertisements, generation nodeGeneration, preserved sets.Set[string]) (func(node, network string, entry nnstypes.NodeNetworkRouteAdvertisement) bool, error) {
	if ra == nil || len(preserved) == 0 {
		return func(string, string, nnstypes.NodeNetworkRouteAdvertisement) bool { return false }, nil
	}
	existing, err := c.frrLister.List(labels.SelectorFromSet(labels.Set{types.OvnRouteAdvertisementsKey: ra.Name}))
	if err != nil {
		return nil, fmt.Errorf("failed listing FRRConfigurations of RouteAdvertisements %q: %w", ra.Name, err)
	}
	configured := generatedNodes(existing)
	return func(node, network string, entry nnstypes.NodeNetworkRouteAdvertisement) bool {
		vrf, selected := generation.networkVRFs[network]
		return selected && preserved.Has(node) && configured.Has(node) &&
			sameNodeNetworkStatusEntry(entry, nodeNetworkStatusEntry(ra, vrf))
	}, nil
}

// withdrawNodeNetworkStatuses removes the entries of a RouteAdvertisements
// that are neither desired nor kept. It runs before FRRConfigurations are
// deleted: a node that still translates pod traffic while routes are
// advertised keeps working, a node that stopped translating with no routes
// left does not.
func (c *Controller) withdrawNodeNetworkStatuses(
	ctx context.Context,
	raName string,
	desired map[string]map[string]nnstypes.NodeNetworkRouteAdvertisement,
	keep func(node, network string, entry nnstypes.NodeNetworkRouteAdvertisement) bool,
) (bool, error) {
	objs, err := c.nnsIndexer.ByIndex(types.NodeNetworkStatusRouteAdvertisementsIndex, raName)
	if err != nil {
		return false, fmt.Errorf("failed listing NodeNetworkStatuses of RouteAdvertisements %q: %w", raName, err)
	}
	var hadUpdates bool
	var errs []error
	for _, obj := range objs {
		nns, ok := obj.(*nnstypes.NodeNetworkStatus)
		if !ok {
			continue
		}
		node, network := nns.Spec.NodeName, nns.Spec.NetworkName
		if _, wanted := desired[node][network]; wanted {
			// updated in place once the new configuration is applied
			continue
		}
		i := slices.IndexFunc(nns.Spec.RouteAdvertisements, func(entry nnstypes.NodeNetworkRouteAdvertisement) bool {
			return entry.Name == raName
		})
		if i < 0 || (keep != nil && keep(node, network, nns.Spec.RouteAdvertisements[i])) {
			continue
		}
		updated, err := c.updateNodeNetworkStatus(ctx, node, network, raName, nil)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		hadUpdates = hadUpdates || updated
	}
	return hadUpdates, errors.Join(errs...)
}

// publishNodeNetworkStatuses records the desired entries. It runs after
// FRRConfigurations are applied, so that a node stops translating pod traffic
// only once its routes are published.
func (c *Controller) publishNodeNetworkStatuses(
	ctx context.Context,
	raName string,
	desired map[string]map[string]nnstypes.NodeNetworkRouteAdvertisement,
) (bool, error) {
	var hadUpdates bool
	var errs []error
	for _, node := range sets.List(sets.KeySet(desired)) {
		for _, network := range sets.List(sets.KeySet(desired[node])) {
			entry := desired[node][network]
			updated, err := c.updateNodeNetworkStatus(ctx, node, network, raName, &entry)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			hadUpdates = hadUpdates || updated
		}
	}
	return hadUpdates, errors.Join(errs...)
}

// updateNodeNetworkStatus sets, or removes when entry is nil, the entry of a
// RouteAdvertisements in the object of a node and a network. Other
// RouteAdvertisements advertising the same network own the other entries of
// that object, so only this one is touched. An object left without entries is
// deleted.
func (c *Controller) updateNodeNetworkStatus(ctx context.Context, node, network, raName string, entry *nnstypes.NodeNetworkRouteAdvertisement) (bool, error) {
	name := nodeNetworkStatusName(node, network)
	client := c.nnsClient.K8sV1alpha1().NodeNetworkStatuses()

	var existing *nnstypes.NodeNetworkStatus
	obj, found, err := c.nnsIndexer.GetByKey(name)
	if err != nil {
		return false, fmt.Errorf("failed getting NodeNetworkStatus %q: %w", name, err)
	}
	if found {
		existing = obj.(*nnstypes.NodeNetworkStatus)
	}

	var hadUpdates bool
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if existing == nil {
			if entry == nil {
				return nil
			}
			created, err := c.createNodeNetworkStatus(ctx, name, node, network, *entry)
			switch {
			case apierrors.IsAlreadyExists(err):
				// the cache has not seen it yet
				existing, err = client.Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					return err
				}
			case err != nil:
				return err
			default:
				hadUpdates = created
				return nil
			}
		}

		entries := slices.DeleteFunc(slices.Clone(existing.Spec.RouteAdvertisements), func(e nnstypes.NodeNetworkRouteAdvertisement) bool {
			return e.Name == raName
		})
		if entry != nil {
			entries = append(entries, *entry)
			slices.SortFunc(entries, func(a, b nnstypes.NodeNetworkRouteAdvertisement) int { return strings.Compare(a.Name, b.Name) })
		}
		if slices.EqualFunc(entries, existing.Spec.RouteAdvertisements, sameNodeNetworkStatusEntry) {
			return nil
		}

		if len(entries) == 0 {
			err = client.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
				UID:             &existing.UID,
				ResourceVersion: &existing.ResourceVersion,
			}})
		} else {
			updated := existing.DeepCopy()
			updated.Spec.RouteAdvertisements = entries
			_, err = client.Update(ctx, updated, metav1.UpdateOptions{FieldManager: fieldManager})
		}
		switch {
		case apierrors.IsNotFound(err):
			existing = nil
			if entry == nil {
				return nil
			}
			// deleted behind our back: fail as a conflict to create it again
			return apierrors.NewConflict(nnstypes.Resource("nodenetworkstatuses"), name, err)
		case apierrors.IsConflict(err):
			// the cached object is stale, retry on the current one
			current, getErr := client.Get(ctx, name, metav1.GetOptions{})
			if getErr != nil && !apierrors.IsNotFound(getErr) {
				return getErr
			}
			existing = nil
			if getErr == nil {
				existing = current
			}
			return err
		case err != nil:
			return err
		}
		hadUpdates = true
		return nil
	})
	if err != nil {
		return hadUpdates, fmt.Errorf("failed updating NodeNetworkStatus of node %q and network %q for RouteAdvertisements %q: %w",
			node, network, raName, err)
	}
	return hadUpdates, nil
}

// createNodeNetworkStatus creates the object for a node and a network. The
// object is owned by the node so it is garbage collected with it. Nothing is
// created for a node that is gone.
func (c *Controller) createNodeNetworkStatus(ctx context.Context, name, node, network string, entry nnstypes.NodeNetworkRouteAdvertisement) (bool, error) {
	owner, err := c.nodeLister.Get(node)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	nns := &nnstypes.NodeNetworkStatus{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       owner.Name,
				UID:        owner.UID,
			}},
		},
		Spec: nnstypes.NodeNetworkStatusSpec{
			NodeName:            node,
			NetworkName:         network,
			RouteAdvertisements: []nnstypes.NodeNetworkRouteAdvertisement{entry},
		},
	}
	_, err = c.nnsClient.K8sV1alpha1().NodeNetworkStatuses().Create(ctx, nns, metav1.CreateOptions{FieldManager: fieldManager})
	if err != nil {
		return false, err
	}
	return true, nil
}

// nodeNetworkStatusNeedsUpdate ignores our own writes: reconciling the
// RouteAdvertisements again would only confirm them.
func nodeNetworkStatusNeedsUpdate(oldObj, newObj *nnstypes.NodeNetworkStatus) bool {
	if newObj != nil && isOwnUpdate(newObj.ManagedFields) {
		return false
	}
	return oldObj == nil || newObj == nil || oldObj.Generation != newObj.Generation
}

// reconcileNodeNetworkStatus reconciles the RouteAdvertisements an object
// lists. On startup this removes the entries of RouteAdvertisements that were
// deleted while the controller was not running.
func (c *Controller) reconcileNodeNetworkStatus(_ context.Context, key string) error {
	obj, found, err := c.nnsIndexer.GetByKey(key)
	if err != nil || !found {
		return err
	}
	for _, entry := range obj.(*nnstypes.NodeNetworkStatus).Spec.RouteAdvertisements {
		c.raController.Reconcile(entry.Name)
	}
	return nil
}

// pruneNodeNetworkStatuses removes the entries of a RouteAdvertisements from
// the nodes left without any of its FRRConfigurations.
func (c *Controller) pruneNodeNetworkStatuses(ctx context.Context, raName string, remaining []*frrtypes.FRRConfiguration) (bool, error) {
	configured := generatedNodes(remaining)
	return c.withdrawNodeNetworkStatuses(ctx, raName, nil, func(node, _ string, _ nnstypes.NodeNetworkRouteAdvertisement) bool {
		return configured.Has(node)
	})
}

// nodeNetworkStatusPublished reports whether nodes already rely on
// NodeNetworkStatus for a RouteAdvertisements.
func nodeNetworkStatusPublished(ra *ratypes.RouteAdvertisements) bool {
	return ra.Annotations[types.OvnRouteAdvertisementsNodeNetworkStatusKey] == "true"
}

// adoptLegacyAdvertisements covers the first reconcile of a RouteAdvertisements
// that predates NodeNetworkStatus. Its nodes consider every selected network
// advertised on every selected node. A node failing right now has no entries
// to preserve yet, but it may have FRRConfigurations from before: give it the
// entries it would have had, so that switching nodes over to NodeNetworkStatus
// does not make it translate pod traffic while its routes are still there.
func (c *Controller) adoptLegacyAdvertisements(
	ra *ratypes.RouteAdvertisements,
	generation nodeGeneration,
	preserved sets.Set[string],
	desired map[string]map[string]nnstypes.NodeNetworkRouteAdvertisement,
) error {
	if ra == nil || nodeNetworkStatusPublished(ra) || len(preserved) == 0 {
		return nil
	}
	existing, err := c.frrLister.List(labels.SelectorFromSet(labels.Set{types.OvnRouteAdvertisementsKey: ra.Name}))
	if err != nil {
		return fmt.Errorf("failed listing FRRConfigurations of RouteAdvertisements %q: %w", ra.Name, err)
	}
	for node := range generatedNodes(existing).Intersection(preserved) {
		desired[node] = make(map[string]nnstypes.NodeNetworkRouteAdvertisement, len(generation.networkVRFs))
		for network, vrf := range generation.networkVRFs {
			desired[node][network] = nodeNetworkStatusEntry(ra, vrf)
		}
	}
	return nil
}

// markNodeNetworkStatusPublished tells nodes to rely on NodeNetworkStatus for
// a RouteAdvertisements. It must only be called once all its entries are
// written: a node switching over earlier would see a network it advertises as
// not advertised.
func (c *Controller) markNodeNetworkStatusPublished(ctx context.Context, ra *ratypes.RouteAdvertisements) error {
	if ra == nil || nodeNetworkStatusPublished(ra) {
		return nil
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:"true"}}}`, types.OvnRouteAdvertisementsNodeNetworkStatusKey)
	_, err := c.raClient.K8sV1().RouteAdvertisements().Patch(ctx, ra.Name, ktypes.MergePatchType, []byte(patch), metav1.PatchOptions{FieldManager: fieldManager})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed marking RouteAdvertisements %q as reported by NodeNetworkStatus: %w", ra.Name, err)
	}
	return nil
}
