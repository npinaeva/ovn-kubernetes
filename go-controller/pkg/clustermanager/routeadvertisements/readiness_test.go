// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package routeadvertisements

import (
	"context"
	"fmt"
	"testing"
	"time"

	frrtypes "github.com/metallb/frr-k8s/api/v1beta1"
	"github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	ratypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/routeadvertisements/v1"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

type replacingNetworkManager struct {
	*networkmanager.FakeNetworkManager
	previous util.NetInfo
}

func (nm *replacingNetworkManager) GetNetwork(name string) util.NetInfo {
	if nm.previous != nil && name == nm.previous.GetNetworkName() {
		previous := nm.previous
		nm.previous = nil
		return previous
	}
	return nm.FakeNetworkManager.GetNetwork(name)
}

func TestNetworkReadinessUsesSelectedNetwork(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	tt := newFRRReconcileTest(t, g, "eth0")
	config.Gateway.Mode = config.GatewayModeShared
	config.OVNKubernetesFeature.EnableDynamicUDNAllocation = false
	nm := &networkmanager.FakeNetworkManager{PrimaryNetworks: map[string]util.NetInfo{}}
	networkName := util.GenerateCUDNNetworkName("production-blue-network")
	tt.addNetwork(nm, &testNAD{Name: "blue", Namespace: "blue", Network: networkName,
		Topology: types.Layer3Topology, Subnet: "1.3.0.0/16/24", Labels: map[string]string{"color": "blue"}})
	previous := util.NewMutableNetInfo(nm.GetNetwork(networkName))
	previous.SetNetworkID(7)
	replacement := util.NewMutableNetInfo(previous)
	replacement.SetNetworkID(12)
	nm.PrimaryNetworks["blue"] = replacement
	// Selection sees the old network; any later lookup sees its replacement.
	// The long CUDN name makes its source VRF depend on the network ID, while
	// the target VRF and pod prefixes stay unchanged across the replacement.
	tt.c.nm = &replacingNetworkManager{FakeNetworkManager: nm, previous: previous}
	tt.updateNodeSubnets("node", fmt.Sprintf(`{"default":"1.1.0.0/24",%q:"1.3.0.0/24"}`, networkName))
	ra := (&testRA{Name: "ra", AdvertisePods: true, SelectsDefault: true, NetworkSelector: map[string]string{"color": "blue"}}).RouteAdvertisements()
	ra.UID, ra.Generation = "ra-uid", 1
	tt.updateRA(ra)

	for _, network := range []util.NetInfo{previous, replacement} {
		g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
		configs := tt.generatedConfigs("ra")
		g.Expect(configs).To(gomega.HaveLen(1))
		cfg := configs["ra/frrConfig/node"]
		g.Expect(util.RouteAdvertisementsConfigurationExportsPrefix(cfg, "", "1.3.0.0/24")).To(gomega.BeTrue())
		g.Expect(cfg.Spec.BGP.Routers).To(gomega.ConsistOf(
			gomega.HaveField("VRF", gomega.Equal("")),
			gomega.HaveField("VRF", gomega.Equal(util.GetNetworkVRFName(network))),
		))
		g.Expect(cfg.Spec.BGP.Routers).To(gomega.ContainElement(gomega.And(
			gomega.HaveField("VRF", gomega.Equal("")),
			gomega.HaveField("Imports", gomega.ConsistOf(frrtypes.Import{VRF: util.GetNetworkVRFName(network)})),
		)))
		tt.waitForFRRCache()
	}
}

func newNetworkReadinessTest(t *testing.T, g *gomega.WithT) (*frrReconcileTest, *networkmanager.FakeNetworkManager, *ratypes.RouteAdvertisements) {
	t.Helper()
	tt := newFRRReconcileTest(t, g, "eth0")
	config.Gateway.Mode = config.GatewayModeShared
	config.OVNKubernetesFeature.EnableDynamicUDNAllocation = false
	nm := &networkmanager.FakeNetworkManager{PrimaryNetworks: map[string]util.NetInfo{}}
	tt.c.nm = nm
	tt.addNetwork(nm, &testNAD{Name: "blue", Namespace: "blue", Network: util.GenerateCUDNNetworkName("blue"),
		Topology: types.Layer3Topology, Subnet: "1.3.0.0/16/24", Labels: map[string]string{"color": "blue", "selected": "true"}})
	tt.updateNodeSubnets("node", `{"default":"1.1.0.0/24","cluster_udn_blue":"1.3.0.0/24"}`)
	ra := (&testRA{Name: "ra", AdvertisePods: true, SelectsDefault: true, NetworkSelector: map[string]string{"color": "blue"}}).RouteAdvertisements()
	ra.UID, ra.Generation = "ra-uid", 1
	tt.updateRA(ra)
	return tt, nm, ra
}

func TestAutoCoverageFollowsRenderedRouters(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	tt, nm, ra := newNetworkReadinessTest(t, g)
	config.OVNKubernetesFeature.EnableDynamicUDNAllocation = true
	nm.SetNodeActive("cluster_udn_blue", "node", true)
	other := (&testNode{Name: "other", SubnetsAnnotation: `{"default":"1.1.1.0/24"}`}).Node()
	_, err := tt.fakeClientset.KubeClient.CoreV1().Nodes().Create(ctx, other, metav1.CreateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "create a second node where blue is inactive")
	g.Eventually(func() error { _, err := tt.c.nodeLister.Get(other.Name); return err }, 5*time.Second).Should(gomega.Succeed(), "wait for the second node")
	ra.Spec.TargetVRF = "auto"
	tt.updateRA(ra)
	client := tt.fakeClientset.FRRClient.ApiV1beta1().FRRConfigurations("frrNamespace")
	source, err := client.Get(ctx, "frrConfig", metav1.GetOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "get the default network peer")
	source.Spec.BGP.Routers = append(source.Spec.BGP.Routers, frrtypes.Router{ASN: 1, VRF: "blue", Neighbors: []frrtypes.Neighbor{{ASN: 1}}})
	for _, peer := range []string{"2001:db8::1", "192.0.2.1"} {
		source.Spec.BGP.Routers[1].Neighbors[0].Address = peer
		source, err = client.Update(ctx, source, metav1.UpdateOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred(), "configure blue's peer %s", peer)
		tt.waitForFRRCache()
		g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
		cfg := tt.generatedConfigs("ra")["ra/frrConfig/node"]
		g.Expect(cfg).NotTo(gomega.BeNil(), "retain the working default VRF")
		g.Expect(util.RouteAdvertisementsConfigurationExportsPrefix(cfg, "", "1.1.0.0/24")).To(gomega.BeTrue())
		missing := peer == "2001:db8::1"
		g.Expect(util.RouteAdvertisementsConfigurationExportsPrefix(cfg, "blue", "1.3.0.0/24")).To(gomega.Equal(!missing))
		status := tt.expectRAStatus(true, missing)
		if missing {
			g.Expect(meta.FindStatusCondition(status.Status.Conditions, conditionTypeDegraded).Message).To(gomega.ContainSubstring("missing [cluster_udn_blue]"))
		}
	}
	before := tt.generatedConfigs("ra")["ra/frrConfig/other"]
	g.Expect(before).NotTo(gomega.BeNil(), "the default-only node has a working configuration")
	source.Spec.BGP.Routers[1].Neighbors[0].DualStackAddressFamily = true
	_, err = client.Update(ctx, source, metav1.UpdateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "set an unsupported peer option in blue's VRF")
	tt.waitForFRRCache()
	g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
	tt.expectRAStatus(true, true)
	configs := tt.generatedConfigs("ra")
	g.Expect(configs).NotTo(gomega.HaveKey("ra/frrConfig/node"), "withdraw the template where blue is active")
	g.Expect(configs["ra/frrConfig/other"]).To(gomega.Equal(before), "blue's invalid peer leaves the default-only node untouched")
}

func TestAutoVRFKeepsIndependentSubnet(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	tt, _, ra := newNetworkReadinessTest(t, g)
	ra.Spec.TargetVRF = "auto"
	tt.updateRA(ra)
	blue := (&testFRRConfig{Name: "blue-only", Namespace: "frrNamespace", Routers: []*testRouter{
		{ASN: 1, VRF: "blue", Neighbors: []*testNeighbor{{ASN: 2, Address: "192.0.2.2"}}},
	}}).FRRConfiguration()
	_, err := tt.fakeClientset.FRRClient.ApiV1beta1().FRRConfigurations(blue.Namespace).Create(ctx, blue, metav1.CreateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "create blue's independent peering template")
	tt.waitForFRRCache()
	g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
	before := tt.generatedConfigs("ra")
	g.Expect(before).To(gomega.HaveLen(2), "both peering configurations start with an allocation")
	for _, subnets := range []string{`{"default":"1.1.0.0/24"}`, `{"default":"1.1.0.0/24","cluster_udn_blue":["bad-cidr"]}`} {
		tt.waitForFRRCache()
		tt.updateNodeSubnets("node", subnets)
		g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
		configs := tt.generatedConfigs("ra")
		g.Expect(configs).To(gomega.HaveLen(1), "withdraw only blue's configuration")
		g.Expect(configs["ra/frrConfig/node"]).To(gomega.Equal(before["ra/frrConfig/node"]), "keep the working default VRF intact")
		status := tt.expectRAStatus(true, true)
		g.Expect(meta.FindStatusCondition(status.Status.Conditions, conditionTypeDegraded).Message).To(gomega.ContainSubstring("cluster_udn_blue"))
	}
	tt.waitForFRRCache()
	tt.updateNodeSubnets("node", `{"default":"1.1.0.0/24","cluster_udn_blue":"1.3.0.0/24"}`)
	g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
	tt.expectRAStatus(true, false)
	configs := tt.generatedConfigs("ra")
	g.Expect(configs).To(gomega.HaveKey("ra/blue-only/node"))
	g.Expect(util.RouteAdvertisementsConfigurationExportsPrefix(configs["ra/blue-only/node"], "blue", "1.3.0.0/24")).To(gomega.BeTrue(), "restore blue's advertisement after allocation recovers")
}

func TestNetworkReadinessWithdrawsOnFailedAddition(t *testing.T) {
	for _, specChange := range []bool{false, true} {
		name := "new matching network"
		if specChange {
			name = "RA selector update"
		}
		t.Run(name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()
			tt, nm, ra := newNetworkReadinessTest(t, g)
			createGreen := func() {
				tt.addNetwork(nm, &testNAD{Name: "green", Namespace: "green", Network: util.GenerateCUDNNetworkName("green"),
					Topology: types.Layer3Topology, Subnet: "1.4.0.0/16", Labels: map[string]string{"selected": "true"}})
			}
			selectGreen := func() {
				ra.Spec.NetworkSelectors = (&testRA{SelectsDefault: true, NetworkSelector: map[string]string{"selected": "true"}}).RouteAdvertisements().Spec.NetworkSelectors
				ra.Generation++
				tt.updateRA(ra)
			}
			if specChange {
				createGreen()
			} else {
				selectGreen()
			}
			other := (&testRA{Name: "other", AdvertisePods: true, SelectsDefault: true}).RouteAdvertisements()
			_, err := tt.fakeClientset.RouteAdvertisementsClient.K8sV1().RouteAdvertisements().Create(ctx, other, metav1.CreateOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred(), "create independent RA %s", other.Name)
			g.Eventually(func() error { _, err := tt.c.raLister.Get("other"); return err }, 5*time.Second).Should(gomega.Succeed(), "wait for RA %s in the cache", other.Name)
			g.Expect(tt.c.reconcile(ctx, "other")).To(gomega.Succeed())
			otherConfigs := tt.generatedConfigs("other")
			g.Expect(otherConfigs).To(gomega.HaveLen(1))
			g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
			tt.waitForFRRCache()
			key := "ra/frrConfig/node"
			g.Expect(tt.generatedConfigs("ra")[key].Spec.BGP.Routers[0].Prefixes).To(gomega.ConsistOf("1.1.0.0/24", "1.3.0.0/24"))
			if specChange {
				selectGreen()
			} else {
				createGreen()
			}
			for range 2 {
				g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
				g.Expect(tt.generatedConfigs("ra")).To(gomega.BeEmpty(), "green's failure withdraws the other networks for this RA/node too")
				tt.waitForFRRCache()
				tt.expectRAStatus(true, true)
				g.Expect(tt.c.reconcile(ctx, "other")).To(gomega.Succeed())
				g.Expect(tt.generatedConfigs("other")).To(gomega.Equal(otherConfigs), "another RA on this node must continue working")
			}

			tt.updateNodeSubnets("node", `{"default":"1.1.0.0/24","cluster_udn_blue":"1.3.0.0/24","cluster_udn_green":"1.4.0.0/24"}`)
			g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
			tt.expectRAStatus(true, false)
			g.Expect(tt.generatedConfigs("ra")[key].Spec.BGP.Routers[0].Prefixes).To(gomega.ConsistOf("1.1.0.0/24", "1.3.0.0/24", "1.4.0.0/24"), "recovery advertises the existing networks again")
			tt.waitForFRRCache()
			source := (&testFRRConfig{Name: "extra", Namespace: "frrNamespace", Routers: []*testRouter{{ASN: 1, Neighbors: []*testNeighbor{{ASN: 1, Interface: "eth1"}}}}}).FRRConfiguration()
			_, err = tt.fakeClientset.FRRClient.ApiV1beta1().FRRConfigurations(source.Namespace).Create(ctx, source, metav1.CreateOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			tt.waitForFRRCache()
			tt.updateNodeSubnets("node", `{"default":"1.1.1.0/24","cluster_udn_blue":"1.3.0.0/24","cluster_udn_green":"1.4.0.0/24"}`)
			g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
			expanded := tt.generatedConfigs("ra")
			g.Expect(expanded).To(gomega.HaveLen(2))
			g.Expect(expanded[key].Spec.BGP.Routers[0].Prefixes).To(gomega.ConsistOf("1.1.1.0/24", "1.3.0.0/24", "1.4.0.0/24"))
			g.Expect(expanded).To(gomega.HaveKey("ra/extra/node"))
		})
	}
}

func TestControllerWithdrawsFailedNodeRegardlessOfNetworkActivity(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprintf("blue remains active=%t", active), func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()
			tt, nm, _ := newNetworkReadinessTest(t, g)
			config.OVNKubernetesFeature.EnableDynamicUDNAllocation = true
			blue := util.GenerateCUDNNetworkName("blue")
			nm.SetNodeActive(blue, "node", true)
			g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
			configs := tt.generatedConfigs("ra")
			g.Expect(configs).To(gomega.HaveLen(1))
			original := configs["ra/frrConfig/node"]
			g.Expect(util.RouteAdvertisementsConfigurationExportsPrefix(original, "", "1.3.0.0/24")).To(gomega.BeTrue())
			tt.waitForFRRCache()

			// Blue stays selected but may have lost its last pod. Missing
			// default-network allocations must withdraw the shared config
			// whether blue is still active or not.
			nm.SetNodeActive(blue, "node", active)
			tt.updateNodeSubnets("node", `{}`)
			for range 2 {
				g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
				tt.expectRAStatus(true, true)
				g.Expect(tt.generatedConfigs("ra")).To(gomega.BeEmpty())
				tt.waitForFRRCache()
			}
		})
	}
}
