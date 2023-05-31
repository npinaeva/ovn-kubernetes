package selector_based_controllers

import (
	"fmt"
	"sync"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/selector_based_handler"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"

	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/errors"
	kerrorsutil "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
)

// PodSelectorPortGroup should always be accessed with oc.podSelectorPortGroups key lock
type PodSelectorPortGroup struct {
	// unique key that identifies given PodSelectorPortGroup
	key string

	// backRefs is a map of objects that use this address set.
	// keys must be unique for all possible users, e.g. for NetworkPolicy use (np *networkPolicy) getKeyWithKind().
	// Must only be changed with oc.podSelectorPortGroups Lock.
	backRefs map[string]bool

	// handlerIdx is either pod or namespace selector handler idx
	// only one of them should be filled
	podHandlerIdx int
	nsHandlerIdx  int

	podSelector       labels.Selector
	namespaceSelector labels.Selector
	// namespace is used when namespaceSelector is nil to set static namespace
	namespace string
	// if needsCleanup is true, try to cleanup before doing any other ops,
	// is cleanup returns error, return error for the op
	needsCleanup bool
	pgDbIDs      *libovsdbops.DbObjectIDs

	// handlerResources holds the data that is used and updated by the handlers.
	handlerResources *PodSelectorPortGroupHandlerInfo
}

type PodPortGroupController struct {
	podSelectorPortGroups    *syncmap.SyncMap[*PodSelectorPortGroup]
	controllerName           string
	nbClient                 libovsdbclient.Client
	watchFactory             *factory.WatchFactory
	podExtraHandlers         *selector_based_handler.PodExtraHandlersManager
	namespaceSelectorHandler *selector_based_handler.EventBasedWatcher
}

func NewPodPortGroupController(controllerName string, nbClient libovsdbclient.Client,
	watchFactory *factory.WatchFactory, podExtraHandlers *selector_based_handler.PodExtraHandlersManager,
	namespaceSelectorHandler *selector_based_handler.EventBasedWatcher) *PodPortGroupController {
	return &PodPortGroupController{
		podSelectorPortGroups:    syncmap.NewSyncMap[*PodSelectorPortGroup](),
		controllerName:           controllerName,
		nbClient:                 nbClient,
		watchFactory:             watchFactory,
		podExtraHandlers:         podExtraHandlers,
		namespaceSelectorHandler: namespaceSelectorHandler,
	}
}

func getPodSelectorPortGroupDbIDs(pspgKey, controller string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.PortGroupPodSelector, controller, map[libovsdbops.ExternalIDKey]string{
		// pod selector port groups are cluster-scoped, only need name
		libovsdbops.ObjectNameKey: pspgKey,
	})
}

// EnsureSharedPortGroup returns address set for requested (podSelector, namespaceSelector, namespace).
// If namespaceSelector is nil, namespace will be used with podSelector statically.
// podSelector should not be nil, use metav1.LabelSelector{} to match all pods.
// namespaceSelector can only be nil when namespace is set, use metav1.LabelSelector{} to match all namespaces.
//
// backRef is the key that should be used for cleanup.
// if err != nil, cleanup is required by calling DeletePortGroup or EnsureAddressSet again.
// psAddrSetHashV4, psAddrSetHashV6 may be set to empty string if address set for that ipFamily wasn't created.
func (c *PodPortGroupController) EnsureSharedPortGroup(podSelector, namespaceSelector *metav1.LabelSelector,
	namespace, backRef string) (pgKey, pgName string, err error) {

	pgKey = GetPodSelectorKey(podSelector, namespaceSelector, namespace)
	pgName, err = c.createPortGroup(podSelector, namespaceSelector, namespace, pgKey, backRef)
	return
}

func (c *PodPortGroupController) CreatePortGroup(podSelector, namespaceSelector *metav1.LabelSelector,
	namespace string, portGroupExternalIDs *libovsdbops.DbObjectIDs) (pgKey string, err error) {

	pgKey = portGroupExternalIDs.String()
	_, err = c.createPortGroup(podSelector, namespaceSelector, namespace, pgKey, "")
	return
}

func (c *PodPortGroupController) createPortGroup(podSelector, namespaceSelector *metav1.LabelSelector,
	namespace string, pgKey, backRef string) (pgName string, err error) {
	podSel, nsSel, err := verifyNamespacedObjSelectors(podSelector, namespaceSelector, namespace)
	if err != nil {
		err = fmt.Errorf("failed to parse pod selector: %w", err)
		return
	}
	err = c.podSelectorPortGroups.DoWithLock(pgKey, func(key string) error {
		psPortGroup, found := c.podSelectorPortGroups.Load(key)
		if !found {
			psPortGroup = &PodSelectorPortGroup{
				key:               key,
				backRefs:          map[string]bool{},
				podSelector:       podSel,
				namespaceSelector: nsSel,
				namespace:         namespace,
				pgDbIDs:           getPodSelectorPortGroupDbIDs(key, c.controllerName),
				podHandlerIdx:     selector_based_handler.NoHandler,
				nsHandlerIdx:      selector_based_handler.NoHandler,
			}
			err = psPortGroup.init(c)
			// save object anyway for future use or cleanup
			c.podSelectorPortGroups.LoadOrStore(key, psPortGroup)
			if err != nil {
				psPortGroup.needsCleanup = true
				return fmt.Errorf("failed to init pod selector address set %s: %v", pgKey, err)
			}
		}
		if psPortGroup.needsCleanup {
			cleanupErr := psPortGroup.destroy(c)
			if cleanupErr != nil {
				return fmt.Errorf("failed to cleanup pod selector address set %s: %v", pgKey, err)
			}
			// psPortGroup.destroy will set psPortGroup.needsCleanup to false if no error was returned
			// try to init again
			err = psPortGroup.init(c)
			if err != nil {
				psPortGroup.needsCleanup = true
				return fmt.Errorf("failed to init pod selector address set %s after cleanup: %v", pgKey, err)
			}
		}
		// psPortGroup is successfully inited, and doesn't need cleanup
		if backRef != "" {
			psPortGroup.backRefs[backRef] = true
		}
		pgName = libovsdbops.GetPortGroupName(psPortGroup.pgDbIDs)
		return nil
	})
	return
}

func (c *PodPortGroupController) DeleteSharedPortGroup(pgKey, backRef string) error {
	return c.podSelectorPortGroups.DoWithLock(pgKey, func(key string) error {
		psAddrSet, found := c.podSelectorPortGroups.Load(key)
		if !found {
			return nil
		}
		delete(psAddrSet.backRefs, backRef)
		if len(psAddrSet.backRefs) == 0 {
			err := psAddrSet.destroy(c)
			if err != nil {
				// psAddrSet.destroy will set psAddrSet.needsCleanup to true in case of error,
				// cleanup should be retried later
				return fmt.Errorf("failed to destroy pod selector address set %s: %v", pgKey, err)
			}
			c.podSelectorPortGroups.Delete(key)
		}
		return nil
	})
}

func (c *PodPortGroupController) DeletePortGroup(pgKey string) error {
	return c.DeleteSharedPortGroup(pgKey, "")
}

func (psas *PodSelectorPortGroup) init(c *PodPortGroupController) error {
	// create pod handler resources before starting the handlers
	if psas.handlerResources == nil {
		psas.handlerResources = &PodSelectorPortGroupHandlerInfo{
			portGroupName:     libovsdbops.GetPortGroupName(psas.pgDbIDs),
			key:               psas.key,
			podSelector:       psas.podSelector,
			namespaceSelector: psas.namespaceSelector,
			namespace:         psas.namespace,
		}
	}

	pg := libovsdbops.BuildPortGroup(psas.pgDbIDs, nil, nil)
	// CreatePortGroups only create port group is it doesn't exist, otherwise does nothing
	// ProcessExisting function for namespace or pod handler will update ports
	err := libovsdbops.CreatePortGroups(c.nbClient, pg)
	if err != nil {
		return fmt.Errorf("failed to create port group: %v", err)
	}

	if psas.podHandlerIdx == selector_based_handler.NoHandler && psas.nsHandlerIdx == selector_based_handler.NoHandler {
		var err error
		if psas.namespaceSelector != nil {
			// selected namespaces, use namespace handler
			psas.nsHandlerIdx, err = c.addNamespaceSelectorHandler(psas.handlerResources, psas.namespaceSelector)
		} else {
			psas.podHandlerIdx, err = c.addPodSelectorHandler(psas.handlerResources, psas.podSelector, psas.namespace, true)
		}
		if err != nil {
			return err
		}
		klog.Infof("Created shared address set for pod selector %s", psas.key)
	}

	return nil
}

func (psas *PodSelectorPortGroup) destroy(c *PodPortGroupController) error {
	klog.Infof("Deleting shared address set for pod selector %s", psas.key)
	psas.needsCleanup = true
	// stop the handler first
	if psas.podHandlerIdx != selector_based_handler.NoHandler {
		c.podExtraHandlers.DeleteHandler(psas.podHandlerIdx)
		psas.podHandlerIdx = selector_based_handler.NoHandler
	}
	if psas.nsHandlerIdx != selector_based_handler.NoHandler {
		c.namespaceSelectorHandler.DeleteHandler(psas.nsHandlerIdx)
		psas.nsHandlerIdx = selector_based_handler.NoHandler
	}
	if psas.handlerResources != nil {
		err := psas.handlerResources.destroy(c)
		if err != nil {
			return fmt.Errorf("failed to delete handler resources: %w", err)
		}
	}
	psas.needsCleanup = false
	return nil
}

// namespace = "" means all namespaces
// podSelector = nil means all pods
func (c *PodPortGroupController) addPodSelectorHandler(handlerInfo *PodSelectorPortGroupHandlerInfo,
	podSelector labels.Selector, namespace string, syncPorts bool) (int, error) {
	podHandler := newPodHandlerPortGroup(handlerInfo, c, syncPorts)

	idx, err := c.podExtraHandlers.AddHandler(namespace, podSelector, podHandler)
	if err != nil {
		return selector_based_handler.NoHandler, fmt.Errorf("failed adding pod Selector handler: %w", err)
	}
	return idx, nil
}

// addNamespaceSelectorHandler starts a watchFactory for NamespaceSelectorType.
// Add event for every existing namespace will be executed sequentially first, and an error will be
// returned if something fails.
func (c *PodPortGroupController) addNamespaceSelectorHandler(handlerInfo *PodSelectorPortGroupHandlerInfo, namespaceSelector labels.Selector) (int, error) {
	// start watching namespaces selected by the namespace selector nsSel;
	// upon namespace add event, start watching pods in that namespace selected
	// by the label selector podSel
	nsHandler := newNamespaceHandlerPortGroup(handlerInfo, c)

	idx, err := c.namespaceSelectorHandler.AddHandler("", namespaceSelector, nsHandler)
	if err != nil {
		return selector_based_handler.NoHandler, fmt.Errorf("failed adding namespace Selector handler: %w", err)
	}
	return idx, nil
}

type PodSelectorPortGroupHandlerInfo struct {
	// resources updated by podHandler
	portGroupName string
	// namespaced pod handlers, the only type of handler that can be dynamically deleted without deleting the whole
	// PodSelectorPortGroup. When namespace is deleted, podHandler for that namespace should be deleted too.
	// Can be used by multiple namespace handlers in parallel for different keys
	// namespace(string): handlerIdx
	namespacedPodHandlerIdxs sync.Map

	// read-only fields
	// unique key that identifies given PodSelectorPortGroup
	key               string
	podSelector       labels.Selector
	namespaceSelector labels.Selector
	// namespace is used when namespaceSelector is nil to set static namespace
	namespace string
}

// idempotent
func (handlerInfo *PodSelectorPortGroupHandlerInfo) destroy(c *PodPortGroupController) error {
	// stop the handlers first, then delete resources they may need
	handlerInfo.namespacedPodHandlerIdxs.Range(func(_, value interface{}) bool {
		c.podExtraHandlers.DeleteHandler(value.(int))
		return true
	})
	handlerInfo.namespacedPodHandlerIdxs = sync.Map{}
	if handlerInfo.portGroupName != "" {
		err := libovsdbops.DeletePortGroups(c.nbClient, handlerInfo.portGroupName)
		if err != nil {
			return err
		}
		handlerInfo.portGroupName = ""
	}
	return nil
}

// Implements selector_based_handler.PodExtraHandler
type podHandlerPortGroup struct {
	controller      *PodPortGroupController
	handlerInfo     *PodSelectorPortGroupHandlerInfo
	processExisting func(lspGetter selector_based_handler.LSPGetter, objs []interface{}) error
}

func newPodHandlerPortGroup(handlerInfo *PodSelectorPortGroupHandlerInfo,
	controller *PodPortGroupController, syncPorts bool) *podHandlerPortGroup {
	handler := &podHandlerPortGroup{
		controller:  controller,
		handlerInfo: handlerInfo,
	}
	if syncPorts {
		handler.processExisting = handler.ProcessExistingSyncPorts
	} else {
		handler.processExisting = handler.ProcessExistingAdd
	}
	return handler
}

func (podHandler *podHandlerPortGroup) ProcessExisting(lspGetter selector_based_handler.LSPGetter, objs []interface{}) error {
	return podHandler.processExisting(lspGetter, objs)
}

func (podHandler *podHandlerPortGroup) ProcessExistingSyncPorts(lspGetter selector_based_handler.LSPGetter, objs []interface{}) error {
	pods := make([]*kapi.Pod, 0, len(objs))
	for _, obj := range objs {
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("")
		}
		pods = append(pods, pod)
	}
	podHandlerInfo := podHandler.handlerInfo

	lspUUIDs, err := podHandlerInfo.getLSPUUIDs(lspGetter, pods...)
	if err != nil {
		// we can only reset port group ports when we had no errors
		return fmt.Errorf("")
	}

	err = libovsdbops.UpdatePortGroupPorts(podHandler.controller.nbClient, podHandlerInfo.portGroupName, lspUUIDs...)
	return err
}

func (podHandler *podHandlerPortGroup) ProcessExistingAdd(lspGetter selector_based_handler.LSPGetter, objs []interface{}) error {
	pods := make([]*kapi.Pod, 0, len(objs))
	for _, obj := range objs {
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("")
		}
		pods = append(pods, pod)
	}
	return podHandler.controller.handleAddPod(lspGetter, podHandler.handlerInfo, pods...)
}

func (podHandler *podHandlerPortGroup) AddPodOps(pod *kapi.Pod, portUUID string) (ops []ovsdb.Operation, err error) {
	lspGetter := func(pod *kapi.Pod) []selector_based_handler.LSPInfo {
		return []selector_based_handler.LSPInfo{
			{
				portUUID,
				nil,
			},
		}
	}
	return podHandler.controller.getAddPodsOps(lspGetter, podHandler.handlerInfo, pod)
}

func (podHandler *podHandlerPortGroup) AddPod(pod *kapi.Pod, lspGetter selector_based_handler.LSPGetter) error {
	return podHandler.controller.handleAddPod(lspGetter, podHandler.handlerInfo, pod)
}

func (podHandler *podHandlerPortGroup) DeletePodOps(pod *kapi.Pod, portUUID string) (ops []ovsdb.Operation, err error) {
	lspGetter := func(pod *kapi.Pod) []selector_based_handler.LSPInfo {
		return []selector_based_handler.LSPInfo{
			{
				portUUID,
				nil,
			},
		}
	}
	return podHandler.controller.getDelPodsOps(lspGetter, podHandler.handlerInfo, pod)
}

func (podHandler *podHandlerPortGroup) DeletePod(pod *kapi.Pod, portNamesGetter selector_based_handler.LSPGetter) error {
	return podHandler.controller.handleDeletePod(portNamesGetter, podHandler.handlerInfo, pod)
}

func (handlerInfo *PodSelectorPortGroupHandlerInfo) getLSPUUIDs(lspGetter selector_based_handler.LSPGetter,
	pods ...*kapi.Pod) (lspUUIDs []string, err error) {

	lspUUIDs = []string{}
	errs := []error{}

	for _, pod := range pods {
		for _, lspInfo := range lspGetter(pod) {
			if lspInfo.Err != nil {
				errs = append(errs, fmt.Errorf("failed to get LSP info for pod  %s/%s: %w", pod.Namespace, pod.Name, lspInfo.Err))
				continue
			}

			// LSP get succeeded and LSP is up to fresh
			klog.V(5).Infof("Fresh LSP %s/%s for port group %s found in cache",
				pod.Namespace, pod.Name, handlerInfo.portGroupName)
			lspUUIDs = append(lspUUIDs, lspInfo.UUID)
		}
	}
	err = errors.NewAggregate(errs)
	return
}

// getAddPodsOps may return both error and ops, that can only happen when multiple pors are passed, meaning that
// some pods were handler correctly and some were not.
func (c *PodPortGroupController) getAddPodsOps(lspGetter selector_based_handler.LSPGetter,
	podHandlerInfo *PodSelectorPortGroupHandlerInfo, pods ...*kapi.Pod) ([]ovsdb.Operation, error) {

	lspUUIDs, preserveErr := podHandlerInfo.getLSPUUIDs(lspGetter, pods...)
	// handle error in the end, this is best-effort to add pods that succeeded
	var ops []ovsdb.Operation
	if len(lspUUIDs) > 0 {
		// add pods to policy port group
		var opsErr error
		ops, opsErr = libovsdbops.AddPortsToPortGroupOps(c.nbClient, nil, podHandlerInfo.portGroupName, lspUUIDs...)
		if opsErr != nil {
			return nil, fmt.Errorf("unable to get ops to add new pod to port group %s: %v", podHandlerInfo.portGroupName, preserveErr)
		}
	}
	return ops, preserveErr
}

// handleAddPod adds the IP address of a pod that has been
// selected by PodSelectorPortGroup.
func (c *PodPortGroupController) handleAddPod(lspGetter selector_based_handler.LSPGetter,
	podHandlerInfo *PodSelectorPortGroupHandlerInfo, pods ...*kapi.Pod) error {
	//if config.Metrics.EnableScaleMetrics {
	//	start := time.Now()
	//	defer func() {
	//		duration := time.Since(start)
	//		metrics.RecordPodSelectorAddrSetPodEvent("add", duration)
	//	}()
	//}
	ops, err := c.getAddPodsOps(lspGetter, podHandlerInfo, pods...)

	if err != nil {
		return err
	}

	_, err = libovsdbops.TransactAndCheck(c.nbClient, ops)
	return err
}

func (c *PodPortGroupController) getDelPodsOps(lspGetter selector_based_handler.LSPGetter,
	podHandlerInfo *PodSelectorPortGroupHandlerInfo, pods ...*kapi.Pod) ([]ovsdb.Operation, error) {
	lspUUIDs, preserveErr := podHandlerInfo.getLSPUUIDs(lspGetter, pods...)
	// handle error in the end, this is best-effort to add pods that succeeded
	var ops []ovsdb.Operation
	if len(lspUUIDs) > 0 {
		// del pods from policy port group
		var opsErr error
		ops, opsErr = libovsdbops.DeletePortsFromPortGroupOps(c.nbClient, nil, podHandlerInfo.portGroupName, lspUUIDs...)
		if opsErr != nil {
			return nil, fmt.Errorf("unable to get ops to delete pods  from port group %s: %v", podHandlerInfo.portGroupName, preserveErr)
		}
	}
	return ops, preserveErr
}

// handleDeletePod removes the IP address of a pod that no longer
// matches a selector
func (c *PodPortGroupController) handleDeletePod(lspGetter selector_based_handler.LSPGetter,
	podHandlerInfo *PodSelectorPortGroupHandlerInfo, pod *kapi.Pod) error {
	//if !bnc.IsSecondary() && config.Metrics.EnableScaleMetrics {
	//	start := time.Now()
	//	defer func() {
	//		duration := time.Since(start)
	//		metrics.RecordNetpolLocalPodEvent("delete", duration)
	//	}()
	//}
	ops, err := c.getDelPodsOps(lspGetter, podHandlerInfo, pod)

	if err != nil {
		return err
	}

	_, err = libovsdbops.TransactAndCheck(c.nbClient, ops)
	return err
}

// Implements selector_based_handler.EventBasedHandler
type namespaceHandlerPortGroup struct {
	controller  *PodPortGroupController
	handlerInfo *PodSelectorPortGroupHandlerInfo
}

func newNamespaceHandlerPortGroup(handlerInfo *PodSelectorPortGroupHandlerInfo,
	controller *PodPortGroupController) *namespaceHandlerPortGroup {
	return &namespaceHandlerPortGroup{
		controller:  controller,
		handlerInfo: handlerInfo,
	}
}

func (namespaceHandler *namespaceHandlerPortGroup) ProcessExisting(objs []interface{}) error {
	// the only way to delete stale ports for namespace selector is to reset ports to nil, and let every
	// namespaced pod handler add its ports.
	podHandlerInfo := namespaceHandler.handlerInfo

	err := libovsdbops.UpdatePortGroupPorts(namespaceHandler.controller.nbClient, podHandlerInfo.portGroupName, []string{}...)
	if err != nil {
		return fmt.Errorf("")
	}
	for _, obj := range objs {
		err := namespaceHandler.controller.handleNamespaceAdd(namespaceHandler.handlerInfo, obj)
		if err != nil {
			return err
		}
	}
	return nil
}

func (namespaceHandler *namespaceHandlerPortGroup) OnAdd(obj interface{}) error {
	return namespaceHandler.controller.handleNamespaceAdd(namespaceHandler.handlerInfo, obj)
}

func (namespaceHandler *namespaceHandlerPortGroup) OnUpdate(oldObj, newObj interface{}) error {
	// no update
	return nil
}

func (namespaceHandler *namespaceHandlerPortGroup) OnDelete(obj interface{}) error {
	return namespaceHandler.controller.handleNamespaceDel(namespaceHandler.handlerInfo, obj)
}

func (c *PodPortGroupController) handleNamespaceAdd(handlerInfo *PodSelectorPortGroupHandlerInfo, obj interface{}) error {
	//if config.Metrics.EnableScaleMetrics {
	//	start := time.Now()
	//	defer func() {
	//		duration := time.Since(start)
	//		metrics.RecordPodSelectorAddrSetNamespaceEvent("add", duration)
	//	}()
	//}
	namespace := obj.(*kapi.Namespace)
	// start watching pods in this namespace and selected by the label selector in extraParameters.podSelector
	idx, err := c.addPodSelectorHandler(handlerInfo, handlerInfo.podSelector, namespace.Name, false)
	if err != nil {
		return err
	}
	handlerInfo.namespacedPodHandlerIdxs.Store(namespace.Name, idx)
	return nil
}

func (c *PodPortGroupController) handleNamespaceDel(podHandlerInfo *PodSelectorPortGroupHandlerInfo, obj interface{}) error {
	//if config.Metrics.EnableScaleMetrics {
	//	start := time.Now()
	//	defer func() {
	//		duration := time.Since(start)
	//		metrics.RecordPodSelectorAddrSetNamespaceEvent("delete", duration)
	//	}()
	//}
	// when the namespace labels no longer apply
	// stop pod handler,
	var errs []error
	namespace := obj.(*kapi.Namespace)

	if handlerIdx, ok := podHandlerInfo.namespacedPodHandlerIdxs.Load(namespace.Name); ok {
		c.podExtraHandlers.DeleteHandler(handlerIdx.(int))
		podHandlerInfo.namespacedPodHandlerIdxs.Delete(namespace.Name)
	}

	pods, err := c.watchFactory.GetPods(namespace.Name)
	if err != nil {
		return fmt.Errorf("failed to get namespace %s pods: %v", namespace.Namespace, err)
	}
	for _, pod := range pods {
		// call functions from oc.handleDeletePod
		if err = c.handleDeletePod(c.podExtraHandlers.LSPGetter(), podHandlerInfo, pod); err != nil {
			errs = append(errs, err)
		}
	}
	return kerrorsutil.NewAggregate(errs)
}

func CleanupPodSelectorPortGroups(nbClient libovsdbclient.Client, controllerName string) error {
	predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.PortGroupPodSelector, controllerName, nil)
	// consider port groups without acls to be unused
	predicate := libovsdbops.GetPredicate[*nbdb.PortGroup](predicateIDs, func(item *nbdb.PortGroup) bool {
		return len(item.ACLs) == 0
	})

	ops, err := libovsdbops.DeletePortGroupsWithPredicateOps(nbClient, nil, predicate)
	if err != nil {
		return fmt.Errorf("failed to find port groups with predicate: %w", err)
	}
	// delete unused port groups
	_, err = libovsdbops.TransactAndCheck(nbClient, ops)
	if err != nil {
		return fmt.Errorf("faile to trasact db ops: %v", err)
	}
	return nil
}

func GetPortGroup(controllerName string, podSelector, namespaceSelector *metav1.LabelSelector,
	namespace string) *nbdb.PortGroup {
	pgKey := GetPodSelectorKey(podSelector, namespaceSelector, namespace)
	pgDbIDs := getPodSelectorPortGroupDbIDs(pgKey, controllerName)
	pg := libovsdbops.BuildPortGroup(pgDbIDs, nil, nil)
	return pg
}

func GetPortGroupDbIDs(controllerName string, podSelector, namespaceSelector *metav1.LabelSelector,
	namespace string) *libovsdbops.DbObjectIDs {
	pgKey := GetPodSelectorKey(podSelector, namespaceSelector, namespace)
	return getPodSelectorPortGroupDbIDs(pgKey, controllerName)
}

func GetPortGroupName(controllerName string, podSelector, namespaceSelector *metav1.LabelSelector,
	namespace string) string {
	pg := GetPortGroup(controllerName, podSelector, namespaceSelector, namespace)
	return pg.Name
}
