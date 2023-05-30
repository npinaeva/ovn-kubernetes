package ovn

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/selector_based_handler"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	kerrorsutil "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

// PodSelectorAddressSet should always be accessed with oc.podSelectorAddressSets key lock
type PodSelectorAddressSet struct {
	// unique key that identifies given PodSelectorAddressSet
	key string

	// backRefs is a map of objects that use this address set.
	// keys must be unique for all possible users, e.g. for NetworkPolicy use (np *networkPolicy) getKeyWithKind().
	// Must only be changed with oc.podSelectorAddressSets Lock.
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
	addrSetDbIDs *libovsdbops.DbObjectIDs

	// handlerResources holds the data that is used and updated by the handlers.
	handlerResources *PodSelectorAddrSetHandlerInfo
}

type PodAddressSetController struct {
	podSelectorAddressSets   *syncmap.SyncMap[*PodSelectorAddressSet]
	controllerName           string
	addressSetFactory        addressset.AddressSetFactory
	watchFactory             *factory.WatchFactory
	netInfo                  util.NetInfo
	podSelectorHandler       *selector_based_handler.EventBasedWatcher
	namespaceSelectorHandler *selector_based_handler.EventBasedWatcher
}

func NewPodAddressSetController(controllerName string, addressSetFactory addressset.AddressSetFactory,
	watchFactory *factory.WatchFactory, netInfo util.NetInfo, podSelectorHandler,
	namespaceSelectorHandler *selector_based_handler.EventBasedWatcher) *PodAddressSetController {
	return &PodAddressSetController{
		podSelectorAddressSets:   syncmap.NewSyncMap[*PodSelectorAddressSet](),
		controllerName:           controllerName,
		addressSetFactory:        addressSetFactory,
		watchFactory:             watchFactory,
		netInfo:                  netInfo,
		podSelectorHandler:       podSelectorHandler,
		namespaceSelectorHandler: namespaceSelectorHandler,
	}
}

// EnsureAddressSet returns address set for requested (podSelector, namespaceSelector, namespace).
// If namespaceSelector is nil, namespace will be used with podSelector statically.
// podSelector should not be nil, use metav1.LabelSelector{} to match all pods.
// namespaceSelector can only be nil when namespace is set, use metav1.LabelSelector{} to match all namespaces.
// podSelector = metav1.LabelSelector{} + static namespace may be replaced with namespace address set,
// podSelector = metav1.LabelSelector{} + namespaceSelector may be replaced with a set of namespace address sets,
// but both cases will work here too.
//
// backRef is the key that should be used for cleanup.
// if err != nil, cleanup is required by calling DeleteAddressSet or EnsureAddressSet again.
// psAddrSetHashV4, psAddrSetHashV6 may be set to empty string if address set for that ipFamily wasn't created.
func (c *PodAddressSetController) EnsureAddressSet(podSelector, namespaceSelector *metav1.LabelSelector,
	namespace, backRef string) (addrSetKey, psAddrSetHashV4, psAddrSetHashV6 string, err error) {
	if podSelector == nil {
		err = fmt.Errorf("pod selector is nil")
		return
	}
	if namespaceSelector == nil && namespace == "" {
		err = fmt.Errorf("namespace selector is nil and namespace is empty")
		return
	}
	if namespaceSelector != nil {
		// namespace will be ignored in this case
		namespace = ""
	}
	var nsSel, podSel labels.Selector
	if namespaceSelector != nil {
		nsSel, err = metav1.LabelSelectorAsSelector(namespaceSelector)
		if err != nil {
			err = fmt.Errorf("can't parse namespace selector %v: %w", namespaceSelector, err)
			return
		}
	}

	podSel, err = metav1.LabelSelectorAsSelector(podSelector)
	if err != nil {
		err = fmt.Errorf("can't parse pod selector %v: %w", podSelector, err)
		return
	}
	addrSetKey = getPodSelectorKey(podSelector, namespaceSelector, namespace)
	err = c.podSelectorAddressSets.DoWithLock(addrSetKey, func(key string) error {
		psAddrSet, found := c.podSelectorAddressSets.Load(key)
		if !found {
			psAddrSet = &PodSelectorAddressSet{
				key:               key,
				backRefs:          map[string]bool{},
				podSelector:       podSel,
				namespaceSelector: nsSel,
				namespace:         namespace,
				addrSetDbIDs:      getPodSelectorAddrSetDbIDs(addrSetKey, c.controllerName),
				podHandlerIdx:     selector_based_handler.NoHandler,
				nsHandlerIdx:      selector_based_handler.NoHandler,
			}
			err = psAddrSet.init(c)
			// save object anyway for future use or cleanup
			c.podSelectorAddressSets.LoadOrStore(key, psAddrSet)
			if err != nil {
				psAddrSet.needsCleanup = true
				return fmt.Errorf("failed to init pod selector address set %s: %v", addrSetKey, err)
			}
		}
		if psAddrSet.needsCleanup {
			cleanupErr := psAddrSet.destroy(c)
			if cleanupErr != nil {
				return fmt.Errorf("failed to cleanup pod selector address set %s: %v", addrSetKey, err)
			}
			// psAddrSet.destroy will set psAddrSet.needsCleanup to false if no error was returned
			// try to init again
			err = psAddrSet.init(c)
			if err != nil {
				psAddrSet.needsCleanup = true
				return fmt.Errorf("failed to init pod selector address set %s after cleanup: %v", addrSetKey, err)
			}
		}
		// psAddrSet is successfully inited, and doesn't need cleanup
		psAddrSet.backRefs[backRef] = true
		psAddrSetHashV4, psAddrSetHashV6, err = psAddrSet.handlerResources.GetASHashNames()
		return err
	})
	if err != nil {
		return
	}
	return
}

func (c *PodAddressSetController) DeleteAddressSet(addrSetKey, backRef string) error {
	return c.podSelectorAddressSets.DoWithLock(addrSetKey, func(key string) error {
		psAddrSet, found := c.podSelectorAddressSets.Load(key)
		if !found {
			return nil
		}
		delete(psAddrSet.backRefs, backRef)
		if len(psAddrSet.backRefs) == 0 {
			err := psAddrSet.destroy(c)
			if err != nil {
				// psAddrSet.destroy will set psAddrSet.needsCleanup to true in case of error,
				// cleanup should be retried later
				return fmt.Errorf("failed to destroy pod selector address set %s: %v", addrSetKey, err)
			}
			c.podSelectorAddressSets.Delete(key)
		}
		return nil
	})
}

func (psas *PodSelectorAddressSet) init(c *PodAddressSetController) error {
	// create pod handler resources before starting the handlers
	if psas.handlerResources == nil {
		as, err := c.addressSetFactory.NewAddressSet(psas.addrSetDbIDs, nil)
		if err != nil {
			return err
		}
		psas.handlerResources = &PodSelectorAddrSetHandlerInfo{
			addressSet:        as,
			key:               psas.key,
			podSelector:       psas.podSelector,
			namespaceSelector: psas.namespaceSelector,
			namespace:         psas.namespace,
			netInfo:           c.netInfo,
		}
	}

	if psas.podHandlerIdx == selector_based_handler.NoHandler && psas.nsHandlerIdx == selector_based_handler.NoHandler {
		var err error
		var podHandlerIdx, nsHandlerIdx int
		if psas.namespace != "" {
			// static namespace
			if psas.podSelector.Empty() {
				// nil selector means no filtering
				podHandlerIdx, err = c.addPodSelectorHandler(psas.handlerResources, nil, psas.namespace)
			} else {
				// namespaced pod selector
				podHandlerIdx, err = c.addPodSelectorHandler(psas.handlerResources, psas.podSelector, psas.namespace)
			}
		} else if psas.namespaceSelector.Empty() {
			// any namespace
			if psas.podSelector.Empty() {
				// all cluster pods
				podHandlerIdx, err = c.addPodSelectorHandler(psas.handlerResources, nil, "")
			} else {
				// global pod selector
				podHandlerIdx, err = c.addPodSelectorHandler(psas.handlerResources, psas.podSelector, "")
			}
		} else {
			// selected namespaces, use namespace handler
			nsHandlerIdx, err = c.addNamespaceSelectorHandler(psas.handlerResources, psas.namespaceSelector)
		}
		if err != nil {
			return err
		}
		if podHandlerIdx != selector_based_handler.NoHandler {
			psas.podHandlerIdx = podHandlerIdx
		}
		if nsHandlerIdx != selector_based_handler.NoHandler {
			psas.nsHandlerIdx = nsHandlerIdx
		}
		klog.Infof("Created shared address set for pod selector %s", psas.key)
	}

	return nil
}

func (psas *PodSelectorAddressSet) destroy(c *PodAddressSetController) error {
	klog.Infof("Deleting shared address set for pod selector %s", psas.key)
	psas.needsCleanup = true
	// stop the handler first
	if psas.podHandlerIdx != selector_based_handler.NoHandler {
		c.podSelectorHandler.DeleteHandler(psas.podHandlerIdx)
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
func (c *PodAddressSetController) addPodSelectorHandler(handlerInfo *PodSelectorAddrSetHandlerInfo, podSelector labels.Selector, namespace string) (int, error) {
	podHandler := newPodHandler(handlerInfo, c)

	idx, err := c.podSelectorHandler.AddHandler(namespace, podSelector, podHandler)
	if err != nil {
		return selector_based_handler.NoHandler, fmt.Errorf("failed adding pod Selector handler: %w", err)
	}
	return idx, nil
}

// addNamespaceSelectorHandler starts a watcher for NamespaceSelectorType.
// Add event for every existing namespace will be executed sequentially first, and an error will be
// returned if something fails.
func (c *PodAddressSetController) addNamespaceSelectorHandler(handlerInfo *PodSelectorAddrSetHandlerInfo, namespaceSelector labels.Selector) (int, error) {
	// start watching namespaces selected by the namespace selector nsSel;
	// upon namespace add event, start watching pods in that namespace selected
	// by the label selector podSel
	nsHandler := newNamespaceHandler(handlerInfo, c)

	idx, err := c.namespaceSelectorHandler.AddHandler("", namespaceSelector, nsHandler)
	if err != nil {
		return selector_based_handler.NoHandler, fmt.Errorf("failed adding namespace Selector handler: %w", err)
	}
	return idx, nil
}

type PodSelectorAddrSetHandlerInfo struct {
	// resources updated by podHandler
	addressSet addressset.AddressSet
	// namespaced pod handlers, the only type of handler that can be dynamically deleted without deleting the whole
	// PodSelectorAddressSet. When namespace is deleted, podHandler for that namespace should be deleted too.
	// Can be used by multiple namespace handlers in parallel for different keys
	// namespace(string): handlerIdx
	namespacedPodHandlerIdxs sync.Map

	// read-only fields
	// unique key that identifies given PodSelectorAddressSet
	key               string
	podSelector       labels.Selector
	namespaceSelector labels.Selector
	// namespace is used when namespaceSelector is nil to set static namespace
	namespace string

	netInfo util.NetInfo
}

// idempotent
func (handlerInfo *PodSelectorAddrSetHandlerInfo) destroy(c *PodAddressSetController) error {
	// stop the handlers first, then delete resources they may need
	handlerInfo.namespacedPodHandlerIdxs.Range(func(_, value interface{}) bool {
		c.podSelectorHandler.DeleteHandler(value.(int))
		return true
	})
	handlerInfo.namespacedPodHandlerIdxs = sync.Map{}
	if handlerInfo.addressSet != nil {
		err := handlerInfo.addressSet.Destroy()
		if err != nil {
			return err
		}
		handlerInfo.addressSet = nil
	}
	return nil
}

func (handlerInfo *PodSelectorAddrSetHandlerInfo) GetASHashNames() (string, string, error) {
	v4Hash, v6Hash := handlerInfo.addressSet.GetASHashNames()
	return v4Hash, v6Hash, nil
}

// addPods will get all currently assigned ips for given pods, and add them to the address set.
// If pod ips change, this function should be called again.
// must be called with PodSelectorAddrSetHandlerInfo read lock
func (handlerInfo *PodSelectorAddrSetHandlerInfo) addPods(pods ...*v1.Pod) error {
	if handlerInfo.addressSet == nil {
		return fmt.Errorf("pod selector AddressSet %s is nil, cannot add pod(s)", handlerInfo.key)
	}

	ips := []net.IP{}
	for _, pod := range pods {
		podIPs, err := util.GetPodIPsOfNetwork(pod, handlerInfo.netInfo)
		if err != nil {
			return err
		}
		ips = append(ips, podIPs...)
	}
	return handlerInfo.addressSet.AddIPs(ips)
}

func (handlerInfo *PodSelectorAddrSetHandlerInfo) deletePod(pod *v1.Pod) error {
	ips, err := util.GetPodIPsOfNetwork(pod, handlerInfo.netInfo)
	if err != nil {
		// if pod ips can't be fetched on delete, we don't expect that information about ips will ever be updated,
		// therefore just log the error and return.
		klog.Infof("Failed to get pod IPs %s/%s to delete from pod selector address set: %w", pod.Namespace, pod.Name, err)
		return nil
	}
	return handlerInfo.addressSet.DeleteIPs(ips)
}

// handlePodAddUpdate adds the IP address of a pod that has been
// selected by PodSelectorAddressSet.
func handlePodAddUpdate(podHandlerInfo *PodSelectorAddrSetHandlerInfo, objs ...interface{}) error {
	if config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordPodSelectorAddrSetPodEvent("add", duration)
		}()
	}
	pods := make([]*kapi.Pod, 0, len(objs))
	for _, obj := range objs {
		pod := obj.(*kapi.Pod)
		if pod.Spec.NodeName == "" {
			// update event will be received for this pod later, no ips should be assigned yet
			continue
		}
		pods = append(pods, pod)
	}
	// podHandlerInfo.addPods must be called with PodSelectorAddressSet RLock.
	err := podHandlerInfo.addPods(pods...)
	return err
}

// handlePodDelete removes the IP address of a pod that no longer
// matches a selector
func (c *PodAddressSetController) handlePodDelete(podHandlerInfo *PodSelectorAddrSetHandlerInfo, obj interface{}) error {
	if config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordPodSelectorAddrSetPodEvent("delete", duration)
		}()
	}

	pod := obj.(*kapi.Pod)
	if pod.Spec.NodeName == "" {
		klog.Infof("Pod %s/%s not scheduled on any node, skipping it", pod.Namespace, pod.Name)
		return nil
	}
	collidingPodName, err := c.podSelectorPodNeedsDelete(pod, podHandlerInfo)
	if err != nil {
		return fmt.Errorf("failed to check if ip is reused for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if collidingPodName != "" {
		// the same ip is used by another pod in the same address set, leave ip
		klog.Infof("Pod %s/%s won't be deleted from the address set %s, since another pod %s is using its ip",
			pod.Namespace, pod.Name, podHandlerInfo.key, collidingPodName)
		return nil
	}
	// podHandlerInfo.deletePod must be called with PodSelectorAddressSet RLock.
	if err := podHandlerInfo.deletePod(pod); err != nil {
		return err
	}
	return nil
}

// podSelectorPodNeedsDelete is designed to avoid problems with completed pods. Delete event for a completed pod may
// come much later than an Update(completed) event, which will be handled as delete event. RetryFramework takes care of
// that by using terminatedObjects cache, In case ovn-k get restarted, this information will be lost and the delete
// event for completed pod may be handled twice. The only problem with that is if another pod is already re-using ip
// of completed pod, then that ip should stay in the address set in case new pod is selected by the PodSelectorAddressSet.
// returns collidingPod namespace+name if the ip shouldn't be removed, because it is reused.
// Must be called with PodSelectorAddressSet.RLock.
func (c *PodAddressSetController) podSelectorPodNeedsDelete(pod *kapi.Pod, podHandlerInfo *PodSelectorAddrSetHandlerInfo) (string, error) {
	if !util.PodCompleted(pod) {
		return "", nil
	}
	ips, err := util.GetPodIPsOfNetwork(pod, c.netInfo)
	if err != nil {
		return "", fmt.Errorf("can't get pod IPs %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	// completed pod be deleted a long time ago, check if there is a new pod with that same ip
	collidingPod, err := findPodWithIPAddresses(ips, c.watchFactory, c.netInfo)
	if err != nil {
		return "", fmt.Errorf("lookup for pods with the same IPs [%s] failed: %w", util.JoinIPs(ips, " "), err)
	}
	if collidingPod == nil {
		return "", nil
	}
	collidingPodName := collidingPod.Namespace + "/" + collidingPod.Name

	v4ips, v6ips := podHandlerInfo.addressSet.GetIPs()
	addrSetIPs := sets.NewString(append(v4ips, v6ips...)...)
	podInAddrSet := false
	for _, podIP := range ips {
		if addrSetIPs.Has(podIP.String()) {
			podInAddrSet = true
			break
		}
	}
	if !podInAddrSet {
		return "", nil
	}
	// we found a colliding pod and pod ip is still in the address set.
	// If the IP is used by another Pod that is targeted by the same selector, don't remove the IP from the address set
	if !podHandlerInfo.podSelector.Matches(labels.Set(collidingPod.Labels)) {
		return "", nil
	}

	// pod selector matches, check namespace match
	if podHandlerInfo.namespace != "" {
		if collidingPod.Namespace == podHandlerInfo.namespace {
			// namespace matches the static namespace, leave ip
			return collidingPodName, nil
		}
	} else {
		// namespace selector is present
		if podHandlerInfo.namespaceSelector.Empty() {
			// matches all namespaces, leave ip
			return collidingPodName, nil
		} else {
			// get namespace to match labels
			ns, err := c.watchFactory.GetNamespace(collidingPod.Namespace)
			if err != nil {
				return "", fmt.Errorf("failed to get namespace %s for pod with the same ip: %w", collidingPod.Namespace, err)
			}
			// if colliding pod's namespace doesn't match labels, then we can safely delete pod
			if !podHandlerInfo.namespaceSelector.Matches(labels.Set(ns.Labels)) {
				return "", nil
			} else {
				return collidingPodName, nil
			}
		}
	}
	return "", nil
}

func (c *PodAddressSetController) handleNamespaceAdd(handlerInfo *PodSelectorAddrSetHandlerInfo, obj interface{}) error {
	if config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordPodSelectorAddrSetNamespaceEvent("add", duration)
		}()
	}
	namespace := obj.(*kapi.Namespace)
	// start watching pods in this namespace and selected by the label selector in extraParameters.podSelector
	idx, err := c.addPodSelectorHandler(handlerInfo, handlerInfo.podSelector, namespace.Name)
	if err != nil {
		return err
	}
	handlerInfo.namespacedPodHandlerIdxs.Store(namespace.Name, idx)
	return nil
}

func (c *PodAddressSetController) handleNamespaceDel(podHandlerInfo *PodSelectorAddrSetHandlerInfo, obj interface{}) error {
	if config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordPodSelectorAddrSetNamespaceEvent("delete", duration)
		}()
	}
	// when the namespace labels no longer apply
	// stop pod handler,
	// remove the namespaces pods from the address_set
	var errs []error
	namespace := obj.(*kapi.Namespace)

	if handlerIdx, ok := podHandlerInfo.namespacedPodHandlerIdxs.Load(namespace.Name); ok {
		c.podSelectorHandler.DeleteHandler(handlerIdx.(int))
		podHandlerInfo.namespacedPodHandlerIdxs.Delete(namespace.Name)
	}

	pods, err := c.watchFactory.GetPods(namespace.Name)
	if err != nil {
		return fmt.Errorf("failed to get namespace %s pods: %v", namespace.Namespace, err)
	}
	for _, pod := range pods {
		// call functions from oc.handlePodDelete
		if err = podHandlerInfo.deletePod(pod); err != nil {
			errs = append(errs, err)
		}
	}
	return kerrorsutil.NewAggregate(errs)
}

type podHandler struct {
	controller  *PodAddressSetController
	handlerInfo *PodSelectorAddrSetHandlerInfo
}

func newPodHandler(handlerInfo *PodSelectorAddrSetHandlerInfo,
	controller *PodAddressSetController) *podHandler {
	return &podHandler{
		controller:  controller,
		handlerInfo: handlerInfo,
	}
}

func (handlerInfo *podHandler) ProcessExisting(objs []interface{}) error {
	// ignore returned error, since any pod that wasn't properly handled will be retried individually.
	_ = handlePodAddUpdate(handlerInfo.handlerInfo, objs...)
	return nil
}

func (handlerInfo *podHandler) OnAdd(obj interface{}) error {
	return handlePodAddUpdate(handlerInfo.handlerInfo, obj)
}

func (handlerInfo *podHandler) OnUpdate(oldObj, newObj interface{}) error {
	return handlePodAddUpdate(handlerInfo.handlerInfo, newObj)
}

func (handlerInfo *podHandler) OnDelete(obj interface{}) error {
	return handlerInfo.controller.handlePodDelete(handlerInfo.handlerInfo, obj)
}

type namespaceHandler struct {
	controller  *PodAddressSetController
	handlerInfo *PodSelectorAddrSetHandlerInfo
}

func newNamespaceHandler(handlerInfo *PodSelectorAddrSetHandlerInfo,
	controller *PodAddressSetController) *namespaceHandler {
	return &namespaceHandler{
		controller:  controller,
		handlerInfo: handlerInfo,
	}
}

func (handlerInfo *namespaceHandler) ProcessExisting(objs []interface{}) error {
	for _, obj := range objs {
		err := handlerInfo.controller.handleNamespaceAdd(handlerInfo.handlerInfo, obj)
		if err != nil {
			return err
		}
	}
	return nil
}

func (handlerInfo *namespaceHandler) OnAdd(obj interface{}) error {
	return handlerInfo.controller.handleNamespaceAdd(handlerInfo.handlerInfo, obj)
}

func (handlerInfo *namespaceHandler) OnUpdate(oldObj, newObj interface{}) error {
	// no update
	return nil
}

func (handlerInfo *namespaceHandler) OnDelete(obj interface{}) error {
	return handlerInfo.controller.handleNamespaceDel(handlerInfo.handlerInfo, obj)
}

func getPodSelectorAddrSetDbIDs(psasKey, controller string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.AddressSetPodSelector, controller, map[libovsdbops.ExternalIDKey]string{
		// pod selector address sets are cluster-scoped, only need name
		libovsdbops.ObjectNameKey: psasKey,
	})
}

// sortedLSRString is based on *LabelSelectorRequirement.String(),
// but adds sorting for Values
func sortedLSRString(lsr *metav1.LabelSelectorRequirement) string {
	if lsr == nil {
		return "nil"
	}
	lsrValues := make([]string, 0, len(lsr.Values))
	lsrValues = append(lsrValues, lsr.Values...)
	sort.Strings(lsrValues)
	s := strings.Join([]string{`LSR{`,
		`Key:` + fmt.Sprintf("%v", lsr.Key) + `,`,
		`Operator:` + fmt.Sprintf("%v", lsr.Operator) + `,`,
		`Values:` + fmt.Sprintf("%v", lsrValues) + `,`,
		`}`,
	}, "")
	return s
}

// shortLabelSelectorString is based on *LabelSelector.String(),
// but makes sure to generate the same string for equivalent selectors (by additional sorting).
// It also tries to reduce return string length, since this string will be put to the db ad ExternalID.
func shortLabelSelectorString(sel *metav1.LabelSelector) string {
	if sel == nil {
		return "nil"
	}
	var repeatedStringForMatchExpressions, mapStringForMatchLabels string
	if len(sel.MatchExpressions) > 0 {
		repeatedStringForMatchExpressions = "ME:{"
		matchExpressions := make([]string, 0, len(sel.MatchExpressions))
		for _, f := range sel.MatchExpressions {
			matchExpressions = append(matchExpressions, sortedLSRString(&f))
		}
		// sort match expressions to not depend on MatchExpressions order
		sort.Strings(matchExpressions)
		repeatedStringForMatchExpressions += strings.Join(matchExpressions, ",")
		repeatedStringForMatchExpressions += "}"
	} else {
		repeatedStringForMatchExpressions = ""
	}
	keysForMatchLabels := make([]string, 0, len(sel.MatchLabels))
	for k := range sel.MatchLabels {
		keysForMatchLabels = append(keysForMatchLabels, k)
	}
	sort.Strings(keysForMatchLabels)
	if len(keysForMatchLabels) > 0 {
		mapStringForMatchLabels = "ML:{"
		for _, k := range keysForMatchLabels {
			mapStringForMatchLabels += fmt.Sprintf("%v: %v,", k, sel.MatchLabels[k])
		}
		mapStringForMatchLabels += "}"
	} else {
		mapStringForMatchLabels = ""
	}
	s := "LS{"
	if mapStringForMatchLabels != "" {
		s += mapStringForMatchLabels + ","
	}
	if repeatedStringForMatchExpressions != "" {
		s += repeatedStringForMatchExpressions + ","
	}
	s += "}"
	return s
}

func getPodSelectorKey(podSelector, namespaceSelector *metav1.LabelSelector, namespace string) string {
	var namespaceKey string
	if namespaceSelector == nil {
		// namespace is static
		namespaceKey = namespace
	} else {
		namespaceKey = shortLabelSelectorString(namespaceSelector)
	}
	return namespaceKey + "_" + shortLabelSelectorString(podSelector)
}

func (bnc *BaseNetworkController) cleanupPodSelectorAddressSets() error {
	err := bnc.deleteStaleNetpolPeerAddrSets()
	if err != nil {
		return fmt.Errorf("can't delete stale netpol address sets %w", err)
	}

	predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.AddressSetPodSelector, bnc.controllerName, nil)
	return deleteAddrSetsWithoutACLRef(predicateIDs, bnc.nbClient)
}

// network policies will start using new shared address sets after the initial Add events handling.
// On the next restart old address sets will be unreferenced and can be safely deleted.
func (bnc *BaseNetworkController) deleteStaleNetpolPeerAddrSets() error {
	predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.AddressSetNetworkPolicy, bnc.controllerName, nil)
	return deleteAddrSetsWithoutACLRef(predicateIDs, bnc.nbClient)
}

func deleteAddrSetsWithoutACLRef(predicateIDs *libovsdbops.DbObjectIDs,
	nbClient libovsdbclient.Client) error {
	// fill existing address set names
	addrSetReferenced := map[string]bool{}
	predicate := libovsdbops.GetPredicate[*nbdb.AddressSet](predicateIDs, func(item *nbdb.AddressSet) bool {
		addrSetReferenced[item.Name] = false
		return false
	})

	_, err := libovsdbops.FindAddressSetsWithPredicate(nbClient, predicate)
	if err != nil {
		return fmt.Errorf("failed to find address sets with predicate: %w", err)
	}
	// set addrSetReferenced[addrSetName] = true if referencing acl exists
	_, err = libovsdbops.FindACLsWithPredicate(nbClient, func(item *nbdb.ACL) bool {
		for addrSetName := range addrSetReferenced {
			if strings.Contains(item.Match, addrSetName) {
				addrSetReferenced[addrSetName] = true
			}
		}
		return false
	})
	if err != nil {
		return fmt.Errorf("cannot find ACLs referencing address set: %v", err)
	}
	ops := []ovsdb.Operation{}
	for addrSetName, isReferenced := range addrSetReferenced {
		if !isReferenced {
			// no references for stale address set, delete
			ops, err = libovsdbops.DeleteAddressSetsOps(nbClient, ops, &nbdb.AddressSet{
				Name: addrSetName,
			})
			if err != nil {
				return fmt.Errorf("failed to get delete address set ops: %w", err)
			}
		}
	}
	// update acls to not reference stale address sets
	_, err = libovsdbops.TransactAndCheck(nbClient, ops)
	if err != nil {
		return fmt.Errorf("faile to trasact db ops: %v", err)
	}
	return nil
}
