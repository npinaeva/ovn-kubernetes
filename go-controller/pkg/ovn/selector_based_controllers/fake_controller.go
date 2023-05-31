package selector_based_controllers

import (
	"context"
	"sync"

	"github.com/onsi/gomega"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/selector_based_handler"
	ovntest "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/testing"
	libovsdbtest "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/testing/libovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	controllerName = "any-controller"
)

type FakeController struct {
	controllerName string
	fakeClient     *util.OVNMasterClientset
	watchFactory   *factory.WatchFactory
	stopChan       chan struct{}
	wg             *sync.WaitGroup
	asf            *addressset.FakeAddressSetFactory
	nbClient       libovsdbclient.Client
	sbClient       libovsdbclient.Client
	dbSetup        libovsdbtest.TestSetup
	nbsbCleanup    *libovsdbtest.Cleanup

	podAddressSetController *PodAddressSetController
	podPortGroupController  *PodPortGroupController

	podSelectorHandler       *selector_based_handler.EventBasedWatcher
	namespaceSelectorHandler *selector_based_handler.EventBasedWatcher
	podExtraHandlers         *selector_based_handler.PodExtraHandlersManager
	lspGetter                func(pod *kapi.Pod) []selector_based_handler.LSPInfo
}

// NOTE: the FakeAddressSetFactory is no longer needed and should no longer be used. starting to phase out FakeAddressSetFactory
func NewFakeController() *FakeController {
	return &FakeController{
		controllerName: controllerName,
		asf:            addressset.NewFakeAddressSetFactory(controllerName),
	}
}

func (o *FakeController) start(lspGetter func(pod *kapi.Pod) []selector_based_handler.LSPInfo, objects ...runtime.Object) {
	o.lspGetter = lspGetter
	fexec := ovntest.NewFakeExec()
	err := util.SetExec(fexec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	o.fakeClient = &util.OVNMasterClientset{
		KubeClient: fake.NewSimpleClientset(objects...),
	}
	o.init()
}

func (o *FakeController) startWithDBSetup(dbSetup libovsdbtest.TestSetup, lspGetter func(pod *kapi.Pod) []selector_based_handler.LSPInfo,
	objects ...runtime.Object) {
	o.dbSetup = dbSetup
	o.start(lspGetter, objects...)
}

func (o *FakeController) shutdown() {
	o.watchFactory.Shutdown()
	close(o.stopChan)
	o.wg.Wait()
	o.nbsbCleanup.Cleanup()
}

func (o *FakeController) init() {
	var err error
	o.watchFactory, err = factory.NewMasterWatchFactory(o.fakeClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = o.watchFactory.Start()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	o.nbClient, o.sbClient, o.nbsbCleanup, err = libovsdbtest.NewNBSBTestHarness(o.dbSetup)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	o.stopChan = make(chan struct{})
	o.wg = &sync.WaitGroup{}
	o.podExtraHandlers = selector_based_handler.NewPodPostHandlersManager(o.watchFactory.PodInformer(), o.lspGetter)
	o.podSelectorHandler = selector_based_handler.NewEventBasedWatcher(factory.PodSelectorType, o.watchFactory.PodInformer(),
		o.watchFactory, o.stopChan, o.wg)
	o.namespaceSelectorHandler = selector_based_handler.NewEventBasedWatcher(factory.NamespaceSelectorType,
		o.watchFactory.NamespaceInformer().Informer(),
		o.watchFactory, o.stopChan, o.wg)
	o.podAddressSetController = NewPodAddressSetController(controllerName, o.asf, o.watchFactory, &util.DefaultNetInfo{},
		o.podSelectorHandler, o.namespaceSelectorHandler)
	o.podPortGroupController = NewPodPortGroupController(o.controllerName,
		o.nbClient, o.watchFactory, o.podExtraHandlers, o.namespaceSelectorHandler)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func (o *FakeController) StartSelectorBasedHandlers() error {
	err := o.podSelectorHandler.Watch()
	if err != nil {
		return err
	}
	return o.namespaceSelectorHandler.Watch()
}

func (o *FakeController) StopSelectorBasedHandlers() {
	o.podSelectorHandler.Stop()
	o.namespaceSelectorHandler.Stop()
}

func resetNBClient(ctx context.Context, nbClient libovsdbclient.Client) {
	if nbClient.Connected() {
		nbClient.Close()
	}
	gomega.Eventually(func() bool {
		return nbClient.Connected()
	}).Should(gomega.BeFalse())
	err := nbClient.Connect(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Eventually(func() bool {
		return nbClient.Connected()
	}).Should(gomega.BeTrue())
	_, err = nbClient.MonitorAll(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}
