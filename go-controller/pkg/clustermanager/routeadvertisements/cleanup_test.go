// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package routeadvertisements

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	nadtypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	nadfake "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned/fake"
	nadlisters "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"
	frrapi "github.com/metallb/frr-k8s/api/v1beta1"
	frrfake "github.com/metallb/frr-k8s/pkg/client/clientset/versioned/fake"
	frrlisters "github.com/metallb/frr-k8s/pkg/client/listers/api/v1beta1"
	"github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	ctesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	controllerutil "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

func TestSharedNADRetainsBothAdvertisements(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	stale := &nadtypes.NetworkAttachmentDefinition{ObjectMeta: metav1.ObjectMeta{Namespace: "blue", Name: "blue", ResourceVersion: "1"}}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	g.Expect(indexer.Add(stale)).To(gomega.Succeed())

	// East's write reached the API before west's reconcile saw it in the cache.
	live := stale.DeepCopy()
	live.ResourceVersion = "2"
	live.Annotations = map[string]string{
		types.OvnRouteAdvertisementsKey:       `["east"]`,
		types.OvnRouteAdvertisementsIntentKey: `{"east":{"targetVRF":"red"}}`,
	}
	client := nadfake.NewSimpleClientset()
	nads := client.K8sCniCncfIoV1().NetworkAttachmentDefinitions(live.Namespace)
	_, err := nads.Create(ctx, live, metav1.CreateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "publish east's updated NAD")
	// The fake client applies merge patches without checking resourceVersion.
	client.PrependReactor("patch", "network-attachment-definitions", func(action ctesting.Action) (bool, runtime.Object, error) {
		var patch nadtypes.NetworkAttachmentDefinition
		g.Expect(json.Unmarshal(action.(ctesting.PatchAction).GetPatch(), &patch)).To(gomega.Succeed())
		if patch.ResourceVersion != "" && patch.ResourceVersion != live.ResourceVersion {
			return true, nil, apierrors.NewConflict(action.GetResource().GroupResource(), live.Name, errors.New("stale version"))
		}
		return false, nil, nil
	})
	c := Controller{nadClient: client, nadLister: nadlisters.NewNetworkAttachmentDefinitionLister(indexer)}
	updateWest := func() error {
		updates, err := c.getNADUpdates(ctx, "west", &util.RouteAdvertisementsIntent{TargetVRF: "green"}, []*nadtypes.NetworkAttachmentDefinition{stale})
		g.Expect(err).NotTo(gomega.HaveOccurred(), "prepare west's NAD update")
		_, err = c.applyNADUpdates(ctx, "west", updates)
		return err
	}
	g.Expect(apierrors.IsConflict(updateWest())).To(gomega.BeTrue(), "west must retry its stale patch")
	g.Expect(indexer.Update(live)).To(gomega.Succeed())
	g.Expect(updateWest()).To(gomega.Succeed(), "west can merge its intent once the cache catches up")
	nad, err := nads.Get(ctx, live.Name, metav1.GetOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	var refs []string
	g.Expect(json.Unmarshal([]byte(nad.Annotations[types.OvnRouteAdvertisementsKey]), &refs)).To(gomega.Succeed())
	g.Expect(refs).To(gomega.ConsistOf("east", "west"))
	g.Expect(nad.Annotations[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(`{"east":{"targetVRF":"red"},"west":{"targetVRF":"green"}}`))
	both := nad.DeepCopy()

	// West is deleted before its successful patch reaches the informer.
	updates, err := c.getNADUpdates(ctx, "west", nil, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "prepare withdrawal against the current NAD")
	updated, err := c.applyNADUpdates(ctx, "west", updates)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "withdraw west using the current NAD")
	g.Expect(updated).To(gomega.BeTrue(), "withdraw west even though the cache only knows about east")
	nad, err = nads.Get(ctx, live.Name, metav1.GetOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "get the shared NAD after withdrawal")
	g.Expect(nad.Annotations[types.OvnRouteAdvertisementsKey]).To(gomega.MatchJSON(`["east"]`))
	g.Expect(nad.Annotations[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(`{"east":{"targetVRF":"red"}}`))

	// Replay the shared NAD's watch events, including a VRF-only change
	// while both RAs still select it. East's intent is the same throughout.
	queue := &controllerutil.FakeController{}
	c.raController = queue
	checkEvent := func(old, new *nadtypes.NetworkAttachmentDefinition) {
		t.Helper()
		new = new.DeepCopy()
		now := metav1.Now()
		new.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: fieldManager, Time: &now}}
		queue.Reconciles = nil
		g.Expect(c.nadNeedsUpdate(old, new)).To(gomega.BeFalse(), "an owned patch needs no cluster-wide reconcile")
		g.Expect(c.nadNeedsUpdate(new, new)).To(gomega.BeFalse(), "an informer resync needs no reconcile")
		g.Expect(queue.Reconciles).To(gomega.Equal([]string{"Reconcile:west"}), "only west's entry changed")
	}
	checkEvent(live, both)
	changedVRF := both.DeepCopy()
	changedVRF.Annotations[types.OvnRouteAdvertisementsIntentKey] = `{"east":{"targetVRF":"red"},"west":{"targetVRF":""}}`
	checkEvent(both, changedVRF)
	egressOnly := changedVRF.DeepCopy()
	egressOnly.Annotations[types.OvnRouteAdvertisementsIntentKey] = nad.Annotations[types.OvnRouteAdvertisementsIntentKey]
	checkEvent(changedVRF, egressOnly) // West keeps its reference after dropping pod advertisement.
	checkEvent(egressOnly, nad)
}

func TestController_rejectedUpdateWithdrawsConfigurations(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	var old []*testFRRConfig
	for _, node := range []string{"node", "node1", "node2"} {
		cfg := generatedFRRConfig("ovnk-generated-"+node, node, true)
		cfg.Routers[0].Prefixes = []string{"1.1.99.0/24", "1.3.0.0/24", "fd03::/64"}
		cfg.Routers[0].Neighbors[0].Advertise = cfg.Routers[0].Prefixes
		cfg.Routers[0].Imports = []string{"blue"}
		cfg.Routers = append(cfg.Routers, &testRouter{ASN: 1, VRF: "blue", Imports: []string{"default"}})
		if node == "node1" {
			cfg.Annotations[types.OvnRouteAdvertisementsKey] = "ra/bad-template/node1"
		}
		old = append(old, cfg)
	}
	tt := newFRRReconcileTest(t, g, "eth0", old...)
	config.Gateway.Mode = config.GatewayModeShared
	previousIPv6 := config.IPv6Mode
	t.Cleanup(func() { config.IPv6Mode = previousIPv6 })
	config.IPv6Mode = true
	nm := &networkmanager.FakeNetworkManager{PrimaryNetworks: map[string]util.NetInfo{}}
	tt.c.nm = nm
	for _, name := range []string{"blue", "green"} {
		refs := `["ra","other"]`
		if name == "green" {
			refs = `["other"]`
		}
		tt.addNetwork(nm, &testNAD{Name: name, Namespace: name, Network: util.GenerateCUDNNetworkName(name),
			Topology: types.Layer3Topology, Subnet: "1.3.0.0/16,fd03::/48", Labels: map[string]string{"color": name},
			Annotations: map[string]string{types.OvnRouteAdvertisementsKey: refs}})
	}
	for _, name := range []string{"node1", "node2", "pending"} {
		node := (&testNode{Name: name, Labels: map[string]string{corev1.LabelHostname: name},
			SubnetsAnnotation: `{"default":"1.1.0.0/24","cluster_udn_green":"1.3.0.0/24"}`}).Node()
		if name == "node2" {
			delete(node.Annotations, types.NodeSubnetsAnnotation)
		} else if name == "pending" {
			node.Annotations[util.OVNNodePrimaryDPUHostAddr] = `{"ipv4":"172.18.0.1/24"}`
		}
		_, err := tt.fakeClientset.KubeClient.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred(), "create node %s for rejected-update cleanup", name)
		g.Eventually(func() error { _, err := tt.c.nodeLister.Get(name); return err }, 5*time.Second).Should(gomega.Succeed(), "wait for node %s in the cache", name)
	}
	tt.updateNodeSubnets("node", `{"default":"1.1.0.0/24","cluster_udn_green":"1.3.0.0/24"}`)
	client := tt.fakeClientset.FRRClient.(*frrfake.Clientset)
	frrClient := client.ApiV1beta1().FRRConfigurations("frrNamespace")
	bad := (&testFRRConfig{Name: "bad-template", Namespace: "frrNamespace", NodeSelector: map[string]string{corev1.LabelHostname: "node1"},
		Routers: []*testRouter{{ASN: 1, VRF: "unrelated", Neighbors: []*testNeighbor{{ASN: 1, Interface: "eth0"}}}}}).FRRConfiguration()
	_, err := frrClient.Create(ctx, bad, metav1.CreateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	tt.waitForFRRCache()
	ra := (&testRA{Name: "ra", AdvertisePods: true, AdvertiseEgressIPs: true, SelectsDefault: true,
		TargetVRF: unsafeInputValue, NetworkSelector: map[string]string{"color": "green"}}).RouteAdvertisements()
	ra.UID, ra.Generation = "uid-ra", 1
	tt.updateRA(ra)
	annotations := func(name string) map[string]string {
		nad, err := tt.fakeClientset.NetworkAttchDefClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(name).Get(ctx, name, metav1.GetOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred(), "read annotations on NAD %s/%s", name, name)
		return nad.Annotations
	}
	references := func(name string) []string {
		var refs []string
		g.Expect(json.Unmarshal([]byte(annotations(name)[types.OvnRouteAdvertisementsKey]), &refs)).To(gomega.Succeed())
		return refs
	}
	nadClient := tt.fakeClientset.NetworkAttchDefClient.(interface{ Actions() []ctesting.Action })
	nadActionsBefore := len(nadClient.Actions())
	client.ClearActions()
	failDelete := true
	client.PrependReactor("delete", "frrconfigurations", func(action ctesting.Action) (bool, runtime.Object, error) {
		if failDelete && action.(ctesting.DeleteAction).GetName() == old[1].Name {
			return true, nil, errors.New("injected cleanup delete failure")
		}
		return false, nil, nil
	})
	err = tt.c.reconcile(ctx, "ra")
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("injected cleanup delete failure")))
	g.Expect(errors.Is(err, errConfig)).To(gomega.BeFalse(), "cleanup failures must be retried")
	g.Expect(references("blue")).To(gomega.ConsistOf("ra", "other"), "retain references until withdrawal succeeds")
	g.Expect(references("green")).To(gomega.ConsistOf("other"))
	failDelete = false
	tt.failNADPatch.Store(true)
	err = tt.c.reconcile(ctx, "ra")
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("injected NAD patch failure")))
	g.Expect(errors.Is(err, errConfig)).To(gomega.BeFalse(), "a failed NAD update must be retried")
	g.Expect(tt.frrConfigNames()).To(gomega.ConsistOf("frrConfig", "bad-template"), "FRR withdrawal completes before NAD cleanup")
	g.Expect(references("blue")).To(gomega.ConsistOf("ra", "other"), "retain the RA reference after the failed NAD patch")
	g.Expect(tt.defaultNADAnnotations()[types.OvnRouteAdvertisementsKey]).To(gomega.Equal(`["ra","other"]`))
	tt.failNADPatch.Store(false)
	g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
	g.Expect(references("blue")).To(gomega.ConsistOf("other"))
	g.Expect(references("green")).To(gomega.ConsistOf("other"), "do not publish newly selected networks")
	g.Expect(tt.defaultNADAnnotations()[types.OvnRouteAdvertisementsKey]).To(gomega.Equal(`["other"]`))
	g.Expect(tt.frrConfigNames()).To(gomega.ConsistOf("frrConfig", "bad-template"), "withdraw every generated object, keeping the templates")
	status := tt.expectRAStatus(false, true)
	accepted := meta.FindStatusCondition(status.Status.Conditions, conditionTypeAccepted)
	g.Expect(accepted.Reason).To(gomega.Equal("ConfigurationError"))
	g.Expect(accepted.Message).To(gomega.ContainSubstring("invalid TargetVRF"))
	for _, action := range client.Actions() {
		g.Expect(action.GetVerb()).NotTo(gomega.BeElementOf("create", "update", "patch"), "rejected input must not publish a valid subset")
	}
	for _, action := range nadClient.Actions()[nadActionsBefore:] {
		g.Expect(action.GetVerb()).NotTo(gomega.BeElementOf("create", "update"))
		if patch, ok := action.(ctesting.PatchAction); ok && patch.GetNamespace() == "green" {
			g.Expect(patch.GetName()).NotTo(gomega.Equal("green"), "a rejected RA must never be published to the green NAD")
		}
	}

	// Correct the inputs while both unready nodes remain unready. Valid
	// nodes must resume, including the newly selected green network.
	bad.Spec.BGP.Routers[0].VRF = ""
	_, err = frrClient.Update(ctx, bad, metav1.UpdateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	tt.waitForFRRCache()
	ra.Spec.TargetVRF = ""
	ra.Generation++
	tt.updateRA(ra)
	g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
	tt.expectRAStatus(true, true)
	generated := tt.generatedConfigs("ra")
	g.Expect(generated).To(gomega.HaveKey("ra/frrConfig/node"))
	g.Expect(generated["ra/frrConfig/node"].Spec.BGP.Routers[0].Prefixes).To(gomega.ConsistOf("1.1.0.0/24", "1.3.0.0/24"))
	g.Expect(references("green")).To(gomega.ConsistOf("ra", "other"))
	g.Expect(annotations("green")[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(`{"ra":{"uid":"uid-ra","generation":2,"targetVRF":"","egressIP":true}}`), "green must record the corrected RA generation")

	ra.Spec.FRRConfigurationSelector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: "peer", Operator: "Unknown"}}
	ra.Generation++
	tt.updateRA(ra)
	g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed(), "reject the malformed selector without retrying")
	status = tt.expectRAStatus(false, true)
	accepted = meta.FindStatusCondition(status.Status.Conditions, conditionTypeAccepted)
	g.Expect(accepted.Reason).To(gomega.Equal("ConfigurationError"))
	g.Expect(accepted.Message).To(gomega.ContainSubstring("invalid frrConfigurationSelector"))
	g.Expect(tt.generatedConfigs("ra")).To(gomega.BeEmpty(), "withdraw the previously working advertisements")
	g.Expect(references("green")).To(gomega.ConsistOf("other"), "remove only the rejected RA's reference")
	g.Expect(annotations("green")[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(`{}`), "withdraw green's pod advertisement intent")
}

func TestControllerKeepsWorkingPeeringTemplate(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	tt := newFRRReconcileTest(t, g, "eth0")
	tt.updateRA((&testRA{Name: "ra", SelectsDefault: true, AdvertisePods: true, AdvertiseEgressIPs: true}).RouteAdvertisements())
	client := tt.fakeClientset.FRRClient.ApiV1beta1().FRRConfigurations("frrNamespace")
	peer := &testNeighbor{ASN: 2, Address: "192.0.2.1"}
	second := (&testFRRConfig{Name: "second", Namespace: "frrNamespace",
		Routers: []*testRouter{{ASN: 1, Neighbors: []*testNeighbor{peer}}}}).FRRConfiguration()
	second, err := client.Create(ctx, second, metav1.CreateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "create the second peering template")
	tt.waitForFRRCache()
	g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
	tt.expectRAStatus(true, false)
	before := tt.generatedConfigs("ra")
	g.Expect(before).To(gomega.HaveKey("ra/frrConfig/node"), "the first peer must be installed")
	g.Expect(before).To(gomega.HaveKey("ra/second/node"), "the second peer must be installed before withdrawal")

	updatePeer := func(address string, selector metav1.LabelSelector) {
		t.Helper()
		second.Spec.NodeSelector = selector
		second.Spec.BGP.Routers[0].Neighbors[0].Address = address
		second, err = client.Update(ctx, second, metav1.UpdateOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred(), "update the second peering template")
		tt.waitForFRRCache()
		g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed(), "reconcile the second peering template")
	}

	invalidSelector := metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
		Key: "role", Operator: metav1.LabelSelectorOpIn,
	}}}
	for _, edit := range []struct {
		address    string
		selector   metav1.LabelSelector
		diagnostic string
	}{
		{peer.Address, invalidSelector, "invalid node selector"},
		{"invalid", metav1.LabelSelector{}, "invalid neighbor"},
		{"2001:db8::1", metav1.LabelSelector{}, "no routes or peers match the target VRF"}, // The node has only an IPv4 subnet.
	} {
		updatePeer(edit.address, edit.selector)
		ra := tt.expectRAStatus(true, true)
		configs := tt.generatedConfigs("ra")
		g.Expect(configs["ra/frrConfig/node"]).To(gomega.Equal(before["ra/frrConfig/node"]), "preserve the working peer after %s", edit.diagnostic)
		g.Expect(configs).NotTo(gomega.HaveKey("ra/second/node"), "withdraw the second peer after %s", edit.diagnostic)
		msg := meta.FindStatusCondition(ra.Status.Conditions, conditionTypeDegraded).Message
		g.Expect(msg).To(gomega.ContainSubstring("frrNamespace/second"))
		g.Expect(msg).To(gomega.ContainSubstring(edit.diagnostic))

		updatePeer(peer.Address, metav1.LabelSelector{})
		tt.expectRAStatus(true, false)
		configs = tt.generatedConfigs("ra")
		g.Expect(configs["ra/frrConfig/node"]).To(gomega.Equal(before["ra/frrConfig/node"]), "preserve the working peer through recovery")
		g.Expect(configs).To(gomega.HaveKey("ra/second/node"), "reinstall the second peer after %s", edit.diagnostic)
		g.Expect(configs["ra/second/node"].Spec).To(gomega.Equal(before["ra/second/node"].Spec), "restore the original advertisement")
	}
}

func TestControllerRetriesIndependentTemplateWrites(t *testing.T) {
	for _, verb := range []string{"create", "update"} {
		t.Run(verb, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()
			tt := newFRRReconcileTest(t, g, "eth0")
			client := tt.fakeClientset.FRRClient.(*frrfake.Clientset)
			source := (&testFRRConfig{Name: "first", Namespace: "frrNamespace",
				Routers: []*testRouter{{ASN: 1, Neighbors: []*testNeighbor{{ASN: 2, Interface: "eth1"}}}}}).FRRConfiguration()
			_, err := client.ApiV1beta1().FRRConfigurations(source.Namespace).Create(ctx, source, metav1.CreateOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred(), "create the first peering template")
			tt.waitForFRRCache()
			prefix := "1.1.0.0/24"
			if verb == "update" {
				g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
				tt.waitForFRRCache()
				prefix = "1.1.2.0/24"
				tt.updateNodeSubnets("node", `{"default":"1.1.2.0/24"}`)
			}
			failure := errors.New("template rejected")
			reject := true
			client.PrependReactor(verb, "frrconfigurations", func(action ctesting.Action) (bool, runtime.Object, error) {
				cfg := action.(interface{ GetObject() runtime.Object }).GetObject().(*frrapi.FRRConfiguration)
				if reject && cfg.Annotations[types.OvnRouteAdvertisementsKey] == "ra/first/node" {
					return true, nil, failure
				}
				return false, nil, nil
			})
			g.Expect(errors.Is(tt.c.reconcile(ctx, "ra"), failure)).To(gomega.BeTrue(), "retry the failed %s", verb)
			ra := tt.expectRAStatus(true, true)
			g.Expect(meta.FindStatusCondition(ra.Status.Conditions, conditionTypeDegraded).Message).To(gomega.ContainSubstring("ra/first/node"))
			configs := tt.generatedConfigs("ra")
			g.Expect(configs).To(gomega.HaveKey("ra/frrConfig/node"), "publish the sibling even when the first write fails")
			g.Expect(configs["ra/frrConfig/node"].Spec.BGP.Routers[0].Prefixes).To(gomega.ContainElement(prefix))
			if verb == "create" {
				g.Expect(configs).NotTo(gomega.HaveKey("ra/first/node"))
				expectFRRMetrics(t, "ra", 1, 1, 0)
			} else {
				g.Expect(configs["ra/first/node"].Spec.BGP.Routers[0].Prefixes).To(gomega.ContainElement("1.1.0.0/24"), "an unsuccessful update leaves the last API object in place")
				expectFRRMetrics(t, "ra", 2, 2, 0)
				failList := true
				client.PrependReactor("list", "frrconfigurations", func(ctesting.Action) (bool, runtime.Object, error) {
					if failList {
						return true, nil, errors.New("metrics read unavailable")
					}
					return false, nil, nil
				})
				g.Expect(errors.Is(tt.c.reconcile(ctx, "ra"), failure)).To(gomega.BeTrue(), "a failed metrics read must preserve the write error")
				expectFRRMetrics(t, "ra", 2, 2, 0)
				failList = false
			}
			nadKey := config.Default.ClusterDefaultNetworkNAD
			nads := tt.c.nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(nadKey.Namespace)
			nad, err := nads.Get(ctx, nadKey.Name, metav1.GetOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred(), "read the default network's intent")
			g.Expect(nad.Annotations[types.OvnRouteAdvertisementsIntentKey]).To(gomega.ContainSubstring(`"ra"`), "the sibling's configuration must be usable immediately")
			if verb == "update" {
				ra := (&testRA{Name: "ra", SelectsDefault: true, AdvertisePods: true, AdvertiseEgressIPs: true}).RouteAdvertisements()
				ra.Generation = 2
				tt.updateRA(ra)
				tt.updateNodeSubnets("node", `{"default":"1.1.3.0/24"}`)
				prefix = "1.1.3.0/24"
				g.Expect(errors.Is(tt.c.reconcile(ctx, "ra"), failure)).To(gomega.BeTrue(), "retry the failed template")
				g.Expect(tt.generatedConfigs("ra")["ra/frrConfig/node"].Spec.BGP.Routers[0].Prefixes).To(gomega.ContainElement(prefix), "an EgressIP-enabled RA can update siblings once withdrawals succeed")
				nad, err = nads.Get(ctx, nadKey.Name, metav1.GetOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred(), "read the updated intent for the same target VRF")
				g.Expect(nad.Annotations[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(`{"ra":{"generation":2,"targetVRF":"","egressIP":true}}`), "publish additive changes while the target VRF stays the same")

				tt.waitForDefaultNADCache()
				podsOnly := (&testRA{Name: "ra", SelectsDefault: true, AdvertisePods: true}).RouteAdvertisements()
				podsOnly.Generation = 3
				tt.updateRA(podsOnly)
				g.Expect(errors.Is(tt.c.reconcile(ctx, "ra"), failure)).To(gomega.BeTrue(), "retry the failed withdrawal")
				g.Expect(tt.defaultNADAnnotations()).To(gomega.Equal(nad.Annotations), "retain EgressIP intent while a template update is rejected")
			}
			reject = false
			tt.waitForFRRCache()
			g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
			tt.expectRAStatus(true, false)
			g.Expect(tt.generatedConfigs("ra")["ra/first/node"].Spec.BGP.Routers[0].Prefixes).To(gomega.ContainElement(prefix), "restore the failed template on retry")
			expectFRRMetrics(t, "ra", 2, 1, 0)
		})
	}
}

func TestControllerAttemptsAllFRRWithdrawals(t *testing.T) {
	for _, scenario := range []string{"obsolete keys", "duplicate keys", "same-node replacement", "unknown node"} {
		t.Run(scenario, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()
			duplicates := scenario == "duplicate keys"
			var existing []*testFRRConfig
			for i := range 4 {
				node := fmt.Sprintf("node-%d", i)
				if duplicates || scenario == "same-node replacement" {
					node = "node"
				}
				old := generatedFRRConfig(fmt.Sprintf("ovnk-generated-%d", i), node, true)
				if scenario == "same-node replacement" {
					old.Annotations[types.OvnRouteAdvertisementsKey] = "ra/old-template/node"
				} else if scenario == "unknown node" {
					old.Annotations[types.OvnRouteAdvertisementsKey] = "invalid-key"
				}
				existing = append(existing, old)
			}
			if scenario == "same-node replacement" {
				existing = append(existing, generatedFRRConfig("ovnk-generated-retained", "node", true))
			}
			tt := newFRRReconcileTest(t, g, "eth0", existing...)
			client := tt.fakeClientset.FRRClient.(*frrfake.Clientset)
			desired := generatedFRRConfig("ovnk-generated-new", "node", true).FRRConfiguration()
			if scenario == "same-node replacement" {
				desired.Spec.BGP.Routers[0].ASN = 2
			}
			failures := []error{errors.New("first deletion failed"), errors.New("second deletion failed")}
			attempts := 0
			client.PrependReactor("delete", "frrconfigurations", func(ctesting.Action) (bool, runtime.Object, error) {
				attempts++
				if attempts <= len(failures) {
					return true, nil, failures[attempts-1]
				}
				return false, nil, nil
			})
			client.ClearActions()
			result := frrGenerationResult{frrConfigs: []*frrapi.FRRConfiguration{desired}}
			changed, err := tt.c.updateFRRConfigurations(ctx, "ra", &result, false)
			g.Expect(changed).To(gomega.BeTrue())
			for _, failure := range failures {
				g.Expect(errors.Is(err, failure)).To(gomega.BeTrue())
			}
			if scenario == "unknown node" {
				expectFRRMetrics(t, "ra", 2, 1, 0)
			} else {
				expectFRRMetrics(t, "ra", 3, 1, 0)
			}
			if duplicates {
				g.Expect(attempts).To(gomega.Equal(3), "retain one desired object and attempt every duplicate deletion")
			} else {
				g.Expect(attempts).To(gomega.Equal(4))
				if scenario == "obsolete keys" {
					current, err := client.ApiV1beta1().FRRConfigurations(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
					g.Expect(err).NotTo(gomega.HaveOccurred())
					g.Expect(current).To(gomega.Equal(desired), "another node's failed withdrawal must not block this configuration")
				} else {
					for _, action := range client.Actions() {
						g.Expect(action.GetVerb()).NotTo(gomega.BeElementOf("create", "update"), "wait for withdrawals that could conflict with this node")
					}
					if scenario == "same-node replacement" {
						current, err := client.ApiV1beta1().FRRConfigurations(desired.Namespace).Get(ctx, "ovnk-generated-retained", metav1.GetOptions{})
						g.Expect(err).NotTo(gomega.HaveOccurred(), "keep the node's current configuration until its obsolete template is removed")
						g.Expect(current.Spec.BGP.Routers[0].ASN).To(gomega.Equal(uint32(1)))
					}
				}
			}
			tt.waitForFRRCache()
			// Failed objects remain retryable; after success the desired config is published.
			_, err = tt.c.updateFRRConfigurations(ctx, "ra", &result, false)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			remaining := tt.generatedConfigs("ra")
			key := desired.Annotations[types.OvnRouteAdvertisementsKey]
			g.Expect(remaining).To(gomega.HaveLen(1))
			g.Expect(remaining).To(gomega.HaveKey(key))
			g.Expect(remaining[key].Spec).To(gomega.Equal(desired.Spec))
			if !duplicates {
				g.Expect(remaining[key]).To(gomega.Equal(desired))
			}
		})
	}
}

// Deletion needs neither prefix ownership nor an informer that has observed the
// previous publish. Read failures, however, must leave the existing state alone.
func TestWholeWithdrawalReadsAPIAndRetries(t *testing.T) {
	for _, scenario := range []string{"stale cache", "read failure", "generation read failure"} {
		t.Run(scenario, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()
			old := generatedFRRConfig("ovnk-generated-old", "node", true)
			other := generatedFRRConfig("ovnk-generated-other", "node", true)
			other.Labels[types.OvnRouteAdvertisementsKey] = "other"
			other.Annotations[types.OvnRouteAdvertisementsKey] = "other/frrConfig/node"
			tt := newFRRReconcileTest(t, g, "eth0", old, other)
			tt.c.frrLister = frrlisters.NewFRRConfigurationLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))
			client := tt.fakeClientset.FRRClient.(*frrfake.Clientset)
			failRead := scenario == "read failure"
			readErr := errors.New("injected read failure")
			client.PrependReactor("list", "frrconfigurations", func(ctesting.Action) (bool, runtime.Object, error) {
				if failRead {
					return true, nil, readErr
				}
				return false, nil, nil
			})
			result := frrGenerationResult{err: errConfig}
			if scenario == "generation read failure" {
				result.err = errors.New("injected generation read failure")
			}
			_, err := tt.c.reconcileRouteAdvertisements(ctx, "ra", nil, &result)
			if scenario == "read failure" || scenario == "generation read failure" {
				g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("injected")))
				if scenario == "read failure" {
					g.Expect(errors.Is(err, readErr)).To(gomega.BeTrue(), "withdrawal must preserve the underlying API read error")
				}
				g.Expect(errors.Is(err, errConfig)).To(gomega.BeFalse(), "read failures must remain retryable even when withdrawing an invalid RA")
				_, getErr := client.ApiV1beta1().FRRConfigurations(old.Namespace).Get(ctx, old.Name, metav1.GetOptions{})
				g.Expect(getErr).NotTo(gomega.HaveOccurred())
				g.Expect(tt.defaultNADAnnotations()[types.OvnRouteAdvertisementsKey]).To(gomega.Equal(`["ra","other"]`))
				failRead = false
				_, err = tt.c.reconcileRouteAdvertisements(ctx, "ra", nil, &frrGenerationResult{err: errConfig})
			}
			g.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = client.ApiV1beta1().FRRConfigurations(old.Namespace).Get(ctx, old.Name, metav1.GetOptions{})
			g.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue())
			remaining, err := client.ApiV1beta1().FRRConfigurations(other.Namespace).Get(ctx, other.Name, metav1.GetOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(remaining.Spec).To(gomega.Equal(other.FRRConfiguration().Spec))
			g.Expect(tt.defaultNADAnnotations()[types.OvnRouteAdvertisementsKey]).To(gomega.Equal(`["other"]`))
		})
	}
}

func TestRollbackAfterNADPatchFailure(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := t.Context()
	tt := newFRRReconcileTest(t, g, "eth0")
	source := (&testFRRConfig{Name: "frrConfig", Namespace: "frrNamespace", Routers: []*testRouter{
		{ASN: 1, VRF: "blue", Neighbors: []*testNeighbor{{ASN: 2, Interface: "eth0"}}},
		{ASN: 1, VRF: "red", Neighbors: []*testNeighbor{{ASN: 2, Interface: "eth1"}}},
	}}).FRRConfiguration()
	_, err := tt.c.frrClient.ApiV1beta1().FRRConfigurations(source.Namespace).Update(ctx, source, metav1.UpdateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "make both target VRFs available")
	tt.waitForFRRCache()
	reconcileVRF := func(vrf string, generation int64) error {
		t.Helper()
		ra := (&testRA{Name: "ra", SelectsDefault: true, AdvertisePods: true, TargetVRF: vrf}).RouteAdvertisements()
		ra.Generation = generation
		tt.updateRA(ra)
		return tt.c.reconcile(ctx, "ra")
	}
	expectExport := func(vrf string) {
		t.Helper()
		configs := tt.generatedConfigs("ra")
		g.Expect(configs).To(gomega.HaveKey("ra/frrConfig/node"))
		g.Expect(util.RouteAdvertisementsConfigurationExportsPrefix(configs["ra/frrConfig/node"], vrf, "1.1.0.0/24")).To(gomega.BeTrue(), "FRRConfiguration must export the pod subnet in VRF %s", vrf)
	}
	g.Expect(reconcileVRF("blue", 1)).To(gomega.Succeed(), "publish the initial intent")
	tt.waitForDefaultNADCache()
	tt.waitForFRRCache()
	tt.c.frrLister = tt.frrCacheWithout("")

	tt.failNADPatch.Store(true)
	g.Expect(reconcileVRF("red", 2)).To(gomega.MatchError(gomega.ContainSubstring("injected NAD patch failure")))
	expectExport("red")
	g.Expect(tt.defaultNADAnnotations()[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(`{"ra":{"generation":1,"targetVRF":"blue"}}`), "the failed patch leaves the older intent in place")

	// The informer still shows blue. Observe the API at the moment the NAD
	// advances so a later FRR write cannot hide incorrect ordering.
	tt.failNADPatch.Store(false)
	tt.fakeClientset.NetworkAttchDefClient.(Fake).PrependReactor("patch", "network-attachment-definitions", func(ctesting.Action) (bool, runtime.Object, error) {
		expectExport("blue")
		return false, nil, nil
	})
	g.Expect(reconcileVRF("blue", 3)).To(gomega.Succeed(), "restore the original export before publishing the rollback")
	g.Expect(tt.defaultNADAnnotations()[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(`{"ra":{"generation":3,"targetVRF":"blue"}}`), "publish the rollback generation")
}

func TestPartialDeselectionWithdrawsBeforeClearingNAD(t *testing.T) {
	for _, tc := range []struct{ name, verb, oldVRF string }{
		{"delete obsolete template", "delete", "red"},
		{"change target VRF", "update", "red"},
		{"drop EgressIP in the same VRF", "update", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := t.Context()
			old := generatedFRRConfig("ovnk-generated-old", "node", true)
			old.Routers[0].VRF = tc.oldVRF
			old.RawConfig = ""
			old.Routers[0].Prefixes = []string{"10.200.1.0/24", "10.201.1.0/24"}
			if tc.verb == "delete" {
				old.Annotations[types.OvnRouteAdvertisementsKey] = "ra/blue-peers/node"
			}
			gen2 := `{"ra":{"uid":"uid-ra","generation":2,"targetVRF":"red"}}`
			if tc.oldVRF == "" {
				old.Routers[0].Prefixes = append(old.Routers[0].Prefixes, "192.0.2.10/32")
				gen2 = `{"ra":{"uid":"uid-ra","generation":2,"targetVRF":"","egressIP":true}}`
			}
			old.Routers[0].Neighbors[0].Advertise = old.Routers[0].Prefixes
			tt := newFRRReconcileTest(t, g, "eth0", old)
			// The RA drops blue and starts advertising the default network.
			// Red keeps its pod routes, but its previous export must change.
			nm := &networkmanager.FakeNetworkManager{PrimaryNetworks: map[string]util.NetInfo{}}
			tt.c.nm = nm
			// Only one namespace received generation 2 before the rollback.
			const (
				gen1 = `{"ra":{"uid":"uid-ra","generation":1,"targetVRF":""}}`
				gen3 = `{"ra":{"uid":"uid-ra","generation":3,"targetVRF":""}}`
			)
			for namespace, intent := range map[string]string{"red": gen2, "lagging": gen1} {
				tt.addNetwork(nm, &testNAD{Name: "red", Namespace: namespace, Network: util.GenerateCUDNNetworkName("red"),
					Topology: types.Layer3Topology, Transport: types.NetworkTransportNoOverlay, Subnet: "10.201.0.0/16/24", Labels: map[string]string{"color": "red"},
					Annotations: map[string]string{types.OvnRouteAdvertisementsKey: `["ra"]`, types.OvnRouteAdvertisementsIntentKey: intent}})
			}
			expectRedIntent := func(namespace, want string) {
				t.Helper()
				nad, err := tt.c.nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespace).Get(ctx, "red", metav1.GetOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred(), "read red's intent in namespace %s", namespace)
				g.Expect(nad.Annotations[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(want), "red's published intent in namespace %s", namespace)
			}
			tt.updateNodeSubnets("node", `{"default":"1.1.0.0/24","cluster_udn_red":"10.201.1.0/24"}`)
			ra := (&testRA{Name: "ra", SelectsDefault: true, AdvertisePods: true, NetworkSelector: map[string]string{"color": "red"}}).RouteAdvertisements()
			ra.UID, ra.Generation = "uid-ra", 3
			tt.updateRA(ra)
			blue := (&testNAD{Name: "blue", Namespace: "tenant", Network: util.GenerateCUDNNetworkName("blue"),
				Topology: types.Layer3Topology, Subnet: "10.200.0.0/16", Annotations: map[string]string{
					types.OvnRouteAdvertisementsKey:       `["ra","other"]`,
					types.OvnRouteAdvertisementsIntentKey: `{"ra":{"targetVRF":"red"},"other":{"targetVRF":"green"}}`,
				}}).NAD()
			nads := tt.c.nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(blue.Namespace)
			_, err := nads.Create(ctx, blue, metav1.CreateOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred(), "seed blue's previously published intent")
			g.Eventually(func() (*nadtypes.NetworkAttachmentDefinition, error) {
				return tt.c.nadLister.NetworkAttachmentDefinitions(blue.Namespace).Get(blue.Name)
			}, 5*time.Second).Should(gomega.Equal(blue), "observe blue's NAD while its FRR configuration is still missing from the cache")
			tt.c.frrLister = tt.frrCacheWithout(old.Name)
			// The API has generation 2 on red/red, but the informer still
			// shows generation 1 in both namespaces when the rollback starts.
			nadLister := tt.c.nadLister
			stale := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			cached, err := nadLister.List(labels.Everything())
			g.Expect(err).NotTo(gomega.HaveOccurred(), "snapshot the NAD cache")
			for _, nad := range cached {
				if nad.Namespace == "red" && nad.Name == "red" {
					nad = nad.DeepCopy()
					nad.Annotations[types.OvnRouteAdvertisementsIntentKey] = gen1
				}
				g.Expect(stale.Add(nad)).To(gomega.Succeed(), "cache NAD %s/%s", nad.Namespace, nad.Name)
			}
			tt.c.nadLister = nadlisters.NewNetworkAttachmentDefinitionLister(stale)

			client := tt.fakeClientset.FRRClient.(*frrfake.Clientset)
			failure := errors.New("injected " + tc.verb + " failure")
			reject := true
			client.PrependReactor(tc.verb, "frrconfigurations", func(ctesting.Action) (bool, runtime.Object, error) {
				if reject {
					return true, nil, failure
				}
				return false, nil, nil
			})
			g.Expect(errors.Is(tt.c.reconcile(ctx, "ra"), failure)).To(gomega.BeTrue(), "retry the failed withdrawal")
			tt.expectRAStatus(true, true)
			g.Expect(tt.defaultNADAnnotations()[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(gen3), "publish the independent default network's intent despite the failed withdrawal")
			cfg, err := client.ApiV1beta1().FRRConfigurations(old.Namespace).Get(ctx, old.Name, metav1.GetOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred(), "read the export left by the failed withdrawal")
			g.Expect(cfg.Spec).To(gomega.Equal(old.FRRConfiguration().Spec), "the previous VRF and prefixes are still configured")
			expectRedIntent("red", gen2)
			expectRedIntent("lagging", gen1)
			current, err := nads.Get(ctx, blue.Name, metav1.GetOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred(), "read blue's NAD after the failed withdrawal")
			g.Expect(current.Annotations).To(gomega.Equal(blue.Annotations), "retain blue's reference and intent until its export is withdrawn")

			// Observe the FRR object at the moment the retry patches blue's NAD.
			tt.fakeClientset.NetworkAttchDefClient.(*nadfake.Clientset).PrependReactor("patch", "network-attachment-definitions", func(action ctesting.Action) (bool, runtime.Object, error) {
				if action.GetNamespace() == blue.Namespace && action.(ctesting.PatchAction).GetName() == blue.Name {
					cfg, err := client.ApiV1beta1().FRRConfigurations(old.Namespace).Get(ctx, old.Name, metav1.GetOptions{})
					if tc.verb == "delete" {
						g.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue(), "delete blue's export before clearing its NAD")
					} else {
						g.Expect(err).NotTo(gomega.HaveOccurred(), "retain the shared export")
						g.Expect(cfg.Spec.BGP.Routers[0].Prefixes).To(gomega.ConsistOf("1.1.0.0/24", "10.201.1.0/24"), "withdraw blue and publish the selected networks before clearing blue's NAD")
					}
				}
				return false, nil, nil
			})
			reject = false
			tt.c.nadLister = nadLister
			g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed(), "retry while the FRR cache still lacks blue's export")
			configs := tt.generatedConfigs("ra")
			g.Expect(configs).To(gomega.HaveLen(1), "withdraw blue's obsolete configuration")
			g.Expect(configs).To(gomega.HaveKey("ra/frrConfig/node"), "publish the selected networks")
			g.Expect(util.RouteAdvertisementsConfigurationExportsPrefix(configs["ra/frrConfig/node"], "", "10.201.1.0/24")).To(gomega.BeTrue(), "export red's pod subnet through the default VRF")
			expectRedIntent("red", gen3)
			expectRedIntent("lagging", gen3)
			current, err = nads.Get(ctx, blue.Name, metav1.GetOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred(), "read blue's NAD after withdrawal")
			g.Expect(current.Annotations[types.OvnRouteAdvertisementsKey]).To(gomega.MatchJSON(`["other"]`), "remove only the deselected RA's reference")
			g.Expect(current.Annotations[types.OvnRouteAdvertisementsIntentKey]).To(gomega.MatchJSON(`{"other":{"targetVRF":"green"}}`), "preserve the other RA's intent")
		})
	}
}
