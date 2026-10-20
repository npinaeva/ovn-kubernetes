// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package routeadvertisements

import (
	"context"
	"errors"
	"testing"

	frrfake "github.com/metallb/frr-k8s/pkg/client/clientset/versioned/fake"
	"github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctesting "k8s.io/client-go/testing"

	controllerutil "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
)

func TestCleanupFailureStatusAtRetryExhaustion(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	tt := newFRRReconcileTest(t, g, "eth0")
	g.Expect(tt.c.reconcile(ctx, "ra")).To(gomega.Succeed())
	raClient := tt.fakeClientset.RouteAdvertisementsClient.K8sV1().RouteAdvertisements()
	previous := tt.expectRAStatus(true, false)

	// Missing allocations must still reach withdrawal.
	g.Expect(tt.generatedConfigs("ra")).To(gomega.HaveLen(1))
	tt.updateNodeSubnets("node", `{}`)
	ra := previous.DeepCopy()
	ra.Generation++
	ra.Spec.TargetVRF = unsafeInputValue
	tt.updateRA(ra)

	tt.fakeClientset.FRRClient.(*frrfake.Clientset).PrependReactor("delete", "frrconfigurations", func(ctesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("injected withdrawal failure")
	})

	// Exercise the failed attempts, then invoke the same callback the
	// workqueue uses when it exhausts its retry budget.
	var err error
	for range controllerutil.DefaultMaxAttempts + 1 {
		err = tt.c.reconcile(ctx, "ra")
		g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("injected withdrawal failure")))
	}
	beforeCallback, getErr := raClient.Get(ctx, "ra", metav1.GetOptions{})
	g.Expect(getErr).NotTo(gomega.HaveOccurred())
	g.Expect(beforeCallback.Status.Conditions).To(gomega.Equal(previous.Status.Conditions), "rejected cleanup returns before status publication")
	g.Expect(tt.c.handleReconcileError(ctx, "ra", err)).To(gomega.Succeed())
	afterCallback := tt.expectRAStatus(false, true)
	degraded := meta.FindStatusCondition(afterCallback.Status.Conditions, conditionTypeDegraded)
	g.Expect(degraded.Message).To(gomega.ContainSubstring("injected withdrawal failure"))
}
