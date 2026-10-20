// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package factory

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	frrapi "github.com/metallb/frr-k8s/api/v1beta1"
	"github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

func TestMain(m *testing.M) {
	// Third-party NAD and FRR fake clients do not send WatchList's initial
	// bookmark. Disable WatchList before any test uses client-go: it caches
	// feature-gate environment values on first use, so per-test t.Setenv
	// calls could be too late and cleanup would not reset the cached value.
	if err := os.Setenv("KUBE_FEATURE_WatchListClient", "false"); err != nil {
		fmt.Fprintf(os.Stderr, "disable client-go WatchList for factory tests: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestRouteAdvertisementsNodeWatch(t *testing.T) {
	for _, localNode := range []string{"local", strings.Repeat("a", 63) + ".example.org"} {
		for _, nodeAgent := range []bool{false, true} {
			name := "OVN controller"
			if nodeAgent {
				name = "node agent"
			}
			t.Run(name+"/"+localNode, func(t *testing.T) {
				g := gomega.NewWithT(t)
				g.Expect(config.PrepareTestConfig()).To(gomega.Succeed(), "prepare watch factory test configuration")
				t.Cleanup(func() { g.Expect(config.PrepareTestConfig()).To(gomega.Succeed(), "restore test configuration") })
				config.OVNKubernetesFeature.EnableMultiNetwork = true
				config.OVNKubernetesFeature.EnableRouteAdvertisements = true
				clients := util.GetOVNClientset()
				for _, node := range []string{localNode, "remote"} {
					_, err := clients.FRRClient.ApiV1beta1().FRRConfigurations("frr").Create(context.Background(),
						&frrapi.FRRConfiguration{ObjectMeta: metav1.ObjectMeta{Name: node, Namespace: "frr", Labels: map[string]string{
							types.OvnRouteAdvertisementsKey: "ra", types.OvnRouteAdvertisementsNodeKey: util.RouteAdvertisementsNodeLabel(node),
						}}}, metav1.CreateOptions{})
					g.Expect(err).NotTo(gomega.HaveOccurred(), "create FRR configuration for node %s", node)
				}
				var wf *WatchFactory
				var err error
				if nodeAgent {
					wf, err = NewNodeWatchFactory(clients.GetNodeClientset(), localNode)
				} else {
					wf, err = NewOVNKubeControllerWatchFactory(clients.GetOVNKubeControllerClientset(), localNode)
				}
				g.Expect(err).NotTo(gomega.HaveOccurred(), "create %s watch factory for node %s", name, localNode)
				defer wf.Shutdown()
				g.Expect(wf.Start()).To(gomega.Succeed(), "start watch factory and synchronize its caches")
				configs, err := wf.FRRConfigurationsInformer().Lister().List(labels.Everything())
				g.Expect(err).NotTo(gomega.HaveOccurred(), "list cached FRR configurations for node %s", localNode)
				g.Expect(configs).To(gomega.HaveLen(1), "the node watch must exclude remote configurations")
				g.Expect(configs[0].Name).To(gomega.Equal(localNode), "the cached configuration must belong to the local node")
				indexed, err := wf.FRRConfigurationsInformer().Informer().GetIndexer().ByIndex(types.OvnRouteAdvertisementsKey, "ra")
				g.Expect(err).NotTo(gomega.HaveOccurred(), "look up configurations by RA and node %s", localNode)
				g.Expect(indexed).To(gomega.HaveLen(1), "the RA index must include the local configuration")
			})
		}
	}
}
