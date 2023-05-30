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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	controllerName = "any-controller"
)

type FakeController struct {
	controllerName           string
	fakeClient               *util.OVNMasterClientset
	watcher                  *factory.WatchFactory
	stopChan                 chan struct{}
	wg                       *sync.WaitGroup
	asf                      *addressset.FakeAddressSetFactory
	nbClient                 libovsdbclient.Client
	sbClient                 libovsdbclient.Client
	dbSetup                  libovsdbtest.TestSetup
	nbsbCleanup              *libovsdbtest.Cleanup
	podAddressSetController  *PodAddressSetController
	podSelectorHandler       *selector_based_handler.EventBasedWatcher
	namespaceSelectorHandler *selector_based_handler.EventBasedWatcher
}

// NOTE: the FakeAddressSetFactory is no longer needed and should no longer be used. starting to phase out FakeAddressSetFactory
func NewFakeController() *FakeController {
	return &FakeController{
		controllerName: controllerName,
		asf:            addressset.NewFakeAddressSetFactory(controllerName),
	}
}

func (o *FakeController) start(objects ...runtime.Object) {
	fexec := ovntest.NewFakeExec()
	err := util.SetExec(fexec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	o.fakeClient = &util.OVNMasterClientset{
		KubeClient: fake.NewSimpleClientset(objects...),
	}
	o.init()
}

func (o *FakeController) startWithDBSetup(dbSetup libovsdbtest.TestSetup, objects ...runtime.Object) {
	o.dbSetup = dbSetup
	o.start(objects...)
}

func (o *FakeController) shutdown() {
	o.watcher.Shutdown()
	close(o.stopChan)
	o.wg.Wait()
	o.nbsbCleanup.Cleanup()
}

func (o *FakeController) init() {
	var err error
	o.watcher, err = factory.NewMasterWatchFactory(o.fakeClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = o.watcher.Start()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	o.nbClient, o.sbClient, o.nbsbCleanup, err = libovsdbtest.NewNBSBTestHarness(o.dbSetup)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	o.stopChan = make(chan struct{})
	o.wg = &sync.WaitGroup{}
	o.podSelectorHandler = selector_based_handler.NewEventBasedWatcher(factory.PodSelectorType, o.watcher.PodInformer(),
		o.watcher, o.stopChan, o.wg)
	o.namespaceSelectorHandler = selector_based_handler.NewEventBasedWatcher(factory.NamespaceSelectorType, o.watcher.NamespaceInformer().Informer(),
		o.watcher, o.stopChan, o.wg)
	o.podAddressSetController = NewPodAddressSetController(controllerName, o.asf, o.watcher, &util.DefaultNetInfo{},
		o.podSelectorHandler, o.namespaceSelectorHandler)
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
