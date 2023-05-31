package selector_based_controllers

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
	libovsdbtest "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/testing/libovsdb"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"

	v1 "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func newNamespaceMeta(namespace string, additionalLabels map[string]string) metav1.ObjectMeta {
	labels := map[string]string{
		"name": namespace,
	}
	for k, v := range additionalLabels {
		labels[k] = v
	}
	return metav1.ObjectMeta{
		UID:         types.UID(namespace),
		Name:        namespace,
		Labels:      labels,
		Annotations: map[string]string{},
	}
}

func newNamespace(namespace string) *v1.Namespace {
	return &v1.Namespace{
		ObjectMeta: newNamespaceMeta(namespace, nil),
		Spec:       v1.NamespaceSpec{},
		Status:     v1.NamespaceStatus{},
	}
}

func newPodMeta(namespace, name string, additionalLabels map[string]string) metav1.ObjectMeta {
	labels := map[string]string{
		"name": name,
	}
	for k, v := range additionalLabels {
		labels[k] = v
	}
	return metav1.ObjectMeta{
		Name:      name,
		UID:       types.UID(name),
		Namespace: namespace,
		Labels:    labels,
	}
}

func newPod(namespace, name, node, podIP string) *v1.Pod {
	podIPs := []v1.PodIP{}
	if podIP != "" {
		podIPs = append(podIPs, v1.PodIP{IP: podIP})
	}
	return &v1.Pod{
		ObjectMeta: newPodMeta(namespace, name, nil),
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "containerName",
					Image: "containerImage",
				},
			},
			NodeName: node,
		},
		Status: v1.PodStatus{
			Phase:  v1.PodRunning,
			PodIP:  podIP,
			PodIPs: podIPs,
		},
	}
}

func getStaleNetpolAddrSetDbIDs(policyNamespace, policyName, policyType, idx, controller string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.AddressSetNetworkPolicy, controller, map[libovsdbops.ExternalIDKey]string{
		libovsdbops.ObjectNameKey: policyNamespace + "_" + policyName,
		// direction and idx uniquely identify address set (= gress policy rule)
		libovsdbops.PolicyDirectionKey: strings.ToLower(policyType),
		libovsdbops.GressIdxKey:        idx,
	})
}

func eventuallyExpectAddressSetsWithIP(fakeController *FakeController, peer knet.NetworkPolicyPeer, namespace, ip string) {
	if peer.PodSelector != nil {
		dbIDs := GetPodSelectorAddrSetDbIDs(GetPodSelectorKey(peer.PodSelector, peer.NamespaceSelector, namespace), fakeController.controllerName)
		fakeController.asf.EventuallyExpectAddressSetWithIPs(dbIDs, []string{ip})
	}
}

func eventuallyExpectEmptyAddressSetsExist(fakeController *FakeController, peer knet.NetworkPolicyPeer, namespace string) {
	if peer.PodSelector != nil {
		dbIDs := GetPodSelectorAddrSetDbIDs(GetPodSelectorKey(peer.PodSelector, peer.NamespaceSelector, namespace), fakeController.controllerName)
		fakeController.asf.EventuallyExpectEmptyAddressSetExist(dbIDs)
	}
}

var _ = ginkgo.Describe("OVN PodSelectorAddressSet", func() {
	const (
		namespaceName1 = "namespace1"
		namespaceName2 = "namespace2"
		nodeName       = "node1"
		podLabelKey    = "podLabel"
		ip1            = "10.128.1.1"
		ip2            = "10.128.1.2"
		ip3            = "10.128.1.3"
		ip4            = "10.128.1.4"
	)
	var (
		fakeController *FakeController
	)

	ginkgo.BeforeEach(func() {
		// Restore global default values before each testcase
		config.PrepareTestConfig()
		fakeController = NewFakeController()
	})

	ginkgo.AfterEach(func() {
		if fakeController.watchFactory != nil {
			// fakeController was inited
			fakeController.shutdown()
		}
	})

	startController := func(namespaces []v1.Namespace) {
		fakeController.start(
			&v1.NamespaceList{
				Items: namespaces,
			},
		)
		err := fakeController.StartSelectorBasedHandlers()
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	}

	ginkgo.It("validates selectors", func() {
		startController(nil)
		backRef := "backRef"

		// create peer with invalid Operator
		peer := knet.NetworkPolicyPeer{
			PodSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "key",
					Operator: "",
					Values:   []string{"value"},
				}},
			},
		}
		// try to add invalid peer
		peerASKey, _, _, err := fakeController.podAddressSetController.EnsureAddressSet(
			peer.PodSelector, peer.NamespaceSelector, namespaceName1, backRef)
		// error should happen on handler add
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("is not a valid label selector operator"))
		// address set will not be created
		peerASIDs := GetPodSelectorAddrSetDbIDs(peerASKey, fakeController.controllerName)
		fakeController.asf.EventuallyExpectNoAddressSet(peerASIDs)

		// add nil pod selector
		peerASKey, _, _, err = fakeController.podAddressSetController.EnsureAddressSet(
			nil, peer.NamespaceSelector, namespaceName1, backRef)
		// error should happen on handler add
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("pod selector is nil"))
		// address set will not be created
		peerASIDs = GetPodSelectorAddrSetDbIDs(peerASKey, fakeController.controllerName)
		fakeController.asf.EventuallyExpectNoAddressSet(peerASIDs)

		// namespace selector is nil and namespace is empty
		peerASKey, _, _, err = fakeController.podAddressSetController.EnsureAddressSet(
			peer.PodSelector, nil, "", backRef)
		// error should happen on handler add
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("namespace selector is nil and namespace is empty"))
		// address set will not be created
		peerASIDs = GetPodSelectorAddrSetDbIDs(peerASKey, fakeController.controllerName)
		fakeController.asf.EventuallyExpectNoAddressSet(peerASIDs)
	})
	ginkgo.It("creates one address set for multiple users with the same selector", func() {
		startController(nil)
		podSelector := &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"name": "label1",
			},
		}

		backRef1 := "networkPolicy1"
		backRef2 := "networkPolicy2"
		peerASKey1, _, _, err := fakeController.podAddressSetController.EnsureAddressSet(
			podSelector, nil, namespaceName1, backRef1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		peerASKey2, _, _, err := fakeController.podAddressSetController.EnsureAddressSet(
			podSelector, nil, namespaceName1, backRef2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(peerASKey2).To(gomega.Equal(peerASKey1))

		peerASIDs := GetPodSelectorAddrSetDbIDs(peerASKey1, fakeController.controllerName)
		fakeController.asf.EventuallyExpectEmptyAddressSetExist(peerASIDs)
		// expect only 1 peer address set
		fakeController.asf.ExpectNumberOfAddressSets(1)
	})
	table.DescribeTable("adds selected pod ips to the address set",
		func(peer knet.NetworkPolicyPeer, staticNamespace string, addrSetIPs []string) {
			namespace1 := *newNamespace(namespaceName1)
			namespace2 := *newNamespace(namespaceName2)
			ns1pod1 := newPod(namespace1.Name, "ns1pod1", nodeName, ip1)
			ns1pod2 := newPod(namespace1.Name, "ns1pod2", nodeName, ip2)
			ns2pod1 := newPod(namespace2.Name, "ns2pod1", nodeName, ip3)
			ns2pod2 := newPod(namespace2.Name, "ns2pod2", nodeName, ip4)
			podsList := []v1.Pod{}
			for _, pod := range []*v1.Pod{ns1pod1, ns1pod2, ns2pod1, ns2pod2} {
				pod.Labels = map[string]string{podLabelKey: pod.Name}
				podsList = append(podsList, *pod)
			}
			fakeController.startWithDBSetup(libovsdbtest.TestSetup{}, nil,
				&v1.NamespaceList{
					Items: []v1.Namespace{namespace1, namespace2},
				},
				&v1.PodList{
					Items: podsList,
				},
			)

			peerASKey, _, _, err := fakeController.podAddressSetController.EnsureAddressSet(
				peer.PodSelector, peer.NamespaceSelector, staticNamespace, "backRef")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// address set should be created and pod ips added
			peerASIDs := GetPodSelectorAddrSetDbIDs(peerASKey, fakeController.controllerName)
			fakeController.asf.ExpectAddressSetWithIPs(peerASIDs, addrSetIPs)
		},
		table.Entry("all pods from a static namespace", knet.NetworkPolicyPeer{
			PodSelector:       &metav1.LabelSelector{},
			NamespaceSelector: nil,
		}, namespaceName1, []string{ip1, ip2}),
		table.Entry("selected pods from a static namespace", knet.NetworkPolicyPeer{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{podLabelKey: "ns1pod1"},
			},
			NamespaceSelector: nil,
		}, namespaceName1, []string{ip1}),
		table.Entry("all pods from all namespaces", knet.NetworkPolicyPeer{
			PodSelector:       &metav1.LabelSelector{},
			NamespaceSelector: &metav1.LabelSelector{},
		}, namespaceName1, []string{ip1, ip2, ip3, ip4}),
		table.Entry("selected pods from all namespaces", knet.NetworkPolicyPeer{
			PodSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      podLabelKey,
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"ns1pod1", "ns2pod1"},
					},
				},
			},
			NamespaceSelector: &metav1.LabelSelector{},
		}, namespaceName1, []string{ip1, ip3}),
		table.Entry("all pods from selected namespaces", knet.NetworkPolicyPeer{
			PodSelector: &metav1.LabelSelector{},
			NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"name": namespaceName2,
				},
			},
		}, namespaceName1, []string{ip3, ip4}),
		table.Entry("selected pods from selected namespaces", knet.NetworkPolicyPeer{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{podLabelKey: "ns2pod1"},
			},
			NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"name": namespaceName2,
				},
			},
		}, namespaceName1, []string{ip3}),
	)
	ginkgo.It("is cleaned up with DeleteAddressSet call", func() {
		// start ovn without any objects
		startController(nil)

		podSelector := &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"name": "label1",
			},
		}
		backRef1 := "networkPolicy1"
		// make asf return error on the next NewAddressSet call
		fakeController.asf.ErrOnNextNewASCall()
		peerASKey, _, _, err := fakeController.podAddressSetController.EnsureAddressSet(
			podSelector, nil, namespaceName1, backRef1)
		// error should happen on address set add
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(addressset.FakeASFError))
		// address set should not be created
		peerASIDs := GetPodSelectorAddrSetDbIDs(peerASKey, fakeController.controllerName)
		fakeController.asf.EventuallyExpectNoAddressSet(peerASIDs)
		// peerAddressSet should be present in the map with needsCleanup=true
		asObj, loaded := fakeController.podAddressSetController.podSelectorAddressSets.Load(peerASKey)
		gomega.Expect(loaded).To(gomega.BeTrue())
		gomega.Expect(asObj.needsCleanup).To(gomega.BeTrue())
		// delete invalid peer, check all resources are cleaned up
		err = fakeController.podAddressSetController.DeleteAddressSet(peerASKey, backRef1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// address set should still not exist
		fakeController.asf.EventuallyExpectNoAddressSet(peerASIDs)
		fakeController.asf.ExpectNumberOfAddressSets(0)
		// peerAddressSet should be deleted from the map
		_, loaded = fakeController.podAddressSetController.podSelectorAddressSets.Load(peerASKey)
		gomega.Expect(loaded).To(gomega.BeFalse())
	})
	ginkgo.It("is cleaned up with second GetPodSelectorAddressSet call", func() {
		// start ovn without any objects
		startController(nil)
		podSelector := &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"name": "label1",
			},
		}
		backRef1 := "networkPolicy1"

		// make asf return error on the next NewAddressSet call
		fakeController.asf.ErrOnNextNewASCall()
		peerASKey, _, _, err := fakeController.podAddressSetController.EnsureAddressSet(
			podSelector, nil, namespaceName1, backRef1)
		// error should happen on address set add
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(addressset.FakeASFError))
		// address set should not be created
		peerASIDs := GetPodSelectorAddrSetDbIDs(peerASKey, fakeController.controllerName)
		fakeController.asf.EventuallyExpectNoAddressSet(peerASIDs)
		// peerAddressSet should be present in the map with needsCleanup=true
		asObj, loaded := fakeController.podAddressSetController.podSelectorAddressSets.Load(peerASKey)
		gomega.Expect(loaded).To(gomega.BeTrue())
		gomega.Expect(asObj.needsCleanup).To(gomega.BeTrue())
		// run add again, NewAddressSet should succeed this time
		peerASKey, _, _, err = fakeController.podAddressSetController.EnsureAddressSet(
			podSelector, nil, namespaceName1, backRef1)
		// address set should be created
		fakeController.asf.ExpectEmptyAddressSet(peerASIDs)
		fakeController.asf.ExpectNumberOfAddressSets(1)
		// peerAddressSet should be present in the map with needsCleanup=false
		asObj, loaded = fakeController.podAddressSetController.podSelectorAddressSets.Load(peerASKey)
		gomega.Expect(loaded).To(gomega.BeTrue())
		gomega.Expect(asObj.needsCleanup).To(gomega.BeFalse())
	})
	ginkgo.It("on cleanup deletes unreferenced and leaves referenced address sets", func() {
		namespaceName := "namespace1"
		policyName := "networkpolicy1"
		staleNetpolIDs := getStaleNetpolAddrSetDbIDs(namespaceName, policyName, "ingress", "0", fakeController.controllerName)
		staleNetpolAS, _ := addressset.GetDbObjsForAS(staleNetpolIDs, []net.IP{net.ParseIP("1.1.1.1")})
		staleNetpolAS.UUID = staleNetpolAS.Name + "-UUID"
		unusedPodSelIDs := GetPodSelectorAddrSetDbIDs("pasName", fakeController.controllerName)
		unusedPodSelAS, _ := addressset.GetDbObjsForAS(unusedPodSelIDs, []net.IP{net.ParseIP("1.1.1.2")})
		unusedPodSelAS.UUID = unusedPodSelAS.Name + "-UUID"
		refNetpolIDs := getStaleNetpolAddrSetDbIDs(namespaceName, policyName, "egress", "0", fakeController.controllerName)
		refNetpolAS, _ := addressset.GetDbObjsForAS(refNetpolIDs, []net.IP{net.ParseIP("1.1.1.3")})
		refNetpolAS.UUID = refNetpolAS.Name + "-UUID"
		netpolACL := libovsdbops.BuildACL(
			"netpolACL",
			nbdb.ACLDirectionFromLport,
			ovntypes.EgressFirewallStartPriority,
			fmt.Sprintf("ip4.src == {$%s} && outport == @a13757631697825269621", refNetpolAS.Name),
			nbdb.ACLActionAllowRelated,
			ovntypes.OvnACLLoggingMeter,
			"",
			false,
			nil,
			map[string]string{
				"apply-after-lb": "true",
			},
		)
		netpolACL.UUID = "netpolACL-UUID"
		refPodSelIDs := GetPodSelectorAddrSetDbIDs("pasName2", fakeController.controllerName)
		refPodSelAS, _ := addressset.GetDbObjsForAS(refPodSelIDs, []net.IP{net.ParseIP("1.1.1.4")})
		refPodSelAS.UUID = refPodSelAS.Name + "-UUID"
		podSelACL := libovsdbops.BuildACL(
			"podSelACL",
			nbdb.ACLDirectionFromLport,
			ovntypes.EgressFirewallStartPriority,
			fmt.Sprintf("ip4.src == {$%s} && outport == @a13757631697825269621", refPodSelAS.Name),
			nbdb.ACLActionAllowRelated,
			ovntypes.OvnACLLoggingMeter,
			"",
			false,
			nil,
			map[string]string{
				"apply-after-lb": "true",
			},
		)
		podSelACL.UUID = "podSelACL-UUID"

		initialDb := []libovsdbtest.TestData{
			staleNetpolAS,
			unusedPodSelAS,
			refNetpolAS,
			netpolACL,
			refPodSelAS,
			podSelACL,
		}
		dbSetup := libovsdbtest.TestSetup{NBData: initialDb}
		fakeController.startWithDBSetup(dbSetup, nil)

		err := CleanupPodSelectorAddressSets(fakeController.nbClient, fakeController.controllerName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		finalDB := []libovsdbtest.TestData{
			refNetpolAS,
			netpolACL,
			refPodSelAS,
			podSelACL,
		}
		gomega.Eventually(fakeController.nbClient).Should(libovsdbtest.HaveData(finalDB))
	})
	ginkgo.It("reconciles a completed and deleted pod whose IP has been assigned to a running pod", func() {
		namespace1 := *newNamespace(namespaceName1)
		nodeName := "node1"
		podIP := "10.128.1.3"
		peer := knet.NetworkPolicyPeer{
			PodSelector: &metav1.LabelSelector{},
		}

		startController([]v1.Namespace{namespace1})

		_, _, _, err := fakeController.podAddressSetController.EnsureAddressSet(
			peer.PodSelector, peer.NamespaceSelector, namespace1.Name, "backRef")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Start a pod
		completedPod, err := fakeController.fakeClient.KubeClient.CoreV1().Pods(namespace1.Name).
			Create(
				context.TODO(),
				newPod(namespace1.Name, "completed-pod", nodeName, podIP),
				metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// pod should be added to the address set
		eventuallyExpectAddressSetsWithIP(fakeController, peer, namespace1.Name, podIP)

		// Spawn a pod with an IP address that collides with a completed pod (we don't watch pods in this test,
		// therefore the same ip is allowed)
		_, err = fakeController.fakeClient.KubeClient.CoreV1().Pods(namespace1.Name).
			Create(
				context.TODO(),
				newPod(namespace1.Name, "running-pod", nodeName, podIP),
				metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Mark the pod as Completed, so delete event will be generated
		completedPod.Status.Phase = v1.PodSucceeded
		completedPod, err = fakeController.fakeClient.KubeClient.CoreV1().Pods(completedPod.Namespace).
			Update(context.TODO(), completedPod, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// make sure the delete event is handled and address set is not changed
		time.Sleep(100 * time.Millisecond)
		// Running pod policy should not be affected by pod deletions
		eventuallyExpectAddressSetsWithIP(fakeController, peer, namespace1.Name, podIP)
	})
	ginkgo.It("reconciles a completed pod whose IP has been assigned to a running pod with non-matching namespace selector", func() {
		namespace1 := *newNamespace(namespaceName1)
		namespace2 := *newNamespace(namespaceName2)
		nodeName := "node1"
		podIP := "10.128.1.3"
		peer := knet.NetworkPolicyPeer{
			PodSelector: &metav1.LabelSelector{},
			NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"name": namespaceName1,
				},
			},
		}

		startController([]v1.Namespace{namespace1, namespace2})

		_, _, _, err := fakeController.podAddressSetController.EnsureAddressSet(
			peer.PodSelector, peer.NamespaceSelector, namespace1.Name, "backRef")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Start a pod
		completedPod, err := fakeController.fakeClient.KubeClient.CoreV1().Pods(namespace1.Name).
			Create(
				context.TODO(),
				newPod(namespace1.Name, "completed-pod", nodeName, podIP),
				metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// pod should be added to the address set
		eventuallyExpectAddressSetsWithIP(fakeController, peer, namespace1.Name, podIP)

		// Spawn a pod with an IP address that collides with a completed pod (we don't watch pods in this test,
		// therefore the same ip is allowed). This pod has another namespace that is not matched by the address set
		_, err = fakeController.fakeClient.KubeClient.CoreV1().Pods(namespace2.Name).
			Create(
				context.TODO(),
				newPod(namespace2.Name, "running-pod", nodeName, podIP),
				metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Mark the pod as Completed, so delete event will be generated
		completedPod.Status.Phase = v1.PodSucceeded
		completedPod, err = fakeController.fakeClient.KubeClient.CoreV1().Pods(completedPod.Namespace).
			Update(context.TODO(), completedPod, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// IP should be deleted from the address set on delete event, since the new pod with the same ip
		// should not be present in given address set
		eventuallyExpectEmptyAddressSetsExist(fakeController, peer, namespace1.Name)
	})
})
