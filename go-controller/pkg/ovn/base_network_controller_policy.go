package ovn

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrorsutil "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"
)

const (
	// arpAllowPolicyMatch is the match used when creating default allow ARP ACLs for a namespace
	arpAllowPolicyMatch   = "(arp || nd)"
	allowHairpinningACLID = "allow-hairpinning"
	// ovnStatelessNetPolAnnotationName is an annotation on K8s Network Policy resource to specify that all
	// the resulting OVN ACLs must be created as stateless
	ovnStatelessNetPolAnnotationName = "k8s.ovn.org/acl-stateless"
	netpolControllerBackRef          = "netpol-controller"
)

type networkPolicy struct {
	// For now networkPolicy has
	// 3 types of global events (those use bnc.networkPolicies to get networkPolicy object)
	// 1. Create network policy - create networkPolicy resources,
	// enable local events, and Update namespace loglevel event
	// 2. Update namespace loglevel - update ACLs for sharedPortGroup and portGroup
	// 3. Delete network policy - disable local events, and Update namespace loglevel event,
	// send deletion signal to already running event handlers, delete resources
	//
	// 2 types of local events (those use the same networkPolicy object there were created for):
	// 1. localPod events - update portGroup, sharedPortGroup and localPods
	// 2. peerNamespace events - add/delete gressPolicy address set, update ACLs for portGroup
	//
	// Delete network policy conflict with all other handlers, therefore we need to make sure it only runs
	// when no other handlers are executing, and that no other handlers will try to work with networkPolicy after
	// Delete network policy was called. This can be done with RWLock, if Delete network policy takes Write lock
	// and sets deleted field to true, and all other handlers take RLock and return immediately if deleted is true.
	// Create network Policy can also take Write lock while it is creating required resources.
	//
	// ACL updates are handled separately for different fields: namespace event will affect ACL.Log and ACL.Severity,
	// while peerNamespace events will affect ACL.Match. Therefore, no additional locking is required.
	//
	// We also need to make sure handlers of the same type can be executed in parallel, if this is not true, every
	// event handler can have it own additional lock to sync handlers of the same type.
	//
	// Allowed order of locking is bnc.networkPolicies key Lock -> networkPolicy.Lock
	// Don't take RLock from the same goroutine twice, it can lead to deadlock.
	sync.RWMutex

	name            string
	namespace       string
	ingressPolicies []*gressPolicy
	egressPolicies  []*gressPolicy
	isIngress       bool
	isEgress        bool

	podSelector metav1.LabelSelector

	// network policy owns only 1 local pod handler
	localPodHandler *factory.Handler
	// peer namespace handlers
	nsHandlerList []*factory.Handler
	// peerAddressSets stores PodSelectorAddressSet keys for peers that this network policy was successfully added to.
	// Required for cleanup.
	peerAddressSets []string

	// localPods is a map of pods affected by this policy.
	// It is used to update defaultDeny port group port counters, when deleting network policy.
	// Port should only be added here if it was successfully added to default deny port group,
	// and local port group in db.
	// localPods may be updated by multiple pod handlers at the same time,
	// therefore it uses a sync map to handle simultaneous access.
	// map of portName(string): portUUID(string)
	localPods sync.Map

	portGroupName string
	portGroupKey  string
	// this is a signal for related event handlers that they are/should be stopped.
	// it will be set to true before any networkPolicy infrastructure is deleted,
	// therefore every handler can either do its work and be sure all required resources are there,
	// or this value will be set to true and handler can't proceed.
	// Use networkPolicy.RLock to read this field and hold it for the whole event handling.
	deleted bool
}

func NewNetworkPolicy(policy *knet.NetworkPolicy) *networkPolicy {
	policyTypeIngress, policyTypeEgress := getPolicyType(policy)
	np := &networkPolicy{
		name:            policy.Name,
		namespace:       policy.Namespace,
		ingressPolicies: make([]*gressPolicy, 0),
		egressPolicies:  make([]*gressPolicy, 0),
		isIngress:       policyTypeIngress,
		isEgress:        policyTypeEgress,
		nsHandlerList:   make([]*factory.Handler, 0),
		localPods:       sync.Map{},
	}
	return np
}

// syncNetworkPoliciesCommon syncs logical entities associated with existing network policies.
// It serves both networkpolicies (for default network) and multi-networkpolicies (for secondary networks)
// TODO update
func (bnc *BaseNetworkController) syncNetworkPoliciesCommon(expectedPolicies map[string]map[string]bool) error {
	// find network policies that don't exist in k8s anymore, but still present in the dbs, and cleanup.
	// Peer address sets and network policy's acls will be cleaned up.
	// Delete acls first, since address sets may be referenced in these acls, and
	// cause SyntaxError in ovn-controller, if address sets deleted first, but acls still reference them.

	// cleanup acls
	// netpol-owned port groups first
	stalePGNames := sets.Set[string]{}
	predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.PortGroupNetworkPolicy, bnc.controllerName, nil)
	p := libovsdbops.GetPredicate[*nbdb.PortGroup](predicateIDs, func(item *nbdb.PortGroup) bool {
		namespace, policyName, err := parsePGPolicyKey(item.ExternalIDs[libovsdbops.ObjectNameKey.String()])
		if err != nil {
			klog.Errorf("Failed to sync stale network policy %v: port group IDs parsing failed: %w",
				item.ExternalIDs[libovsdbops.ObjectNameKey.String()], err)
			return false
		}
		if !expectedPolicies[namespace][policyName] {
			// policy doesn't exist on k8s, cleanup
			stalePGNames.Insert(item.Name)
		}
		return false
	})
	_, err := libovsdbops.FindPortGroupsWithPredicate(bnc.nbClient, p)
	if err != nil {
		return fmt.Errorf("cannot find NetworkPolicy port groups: %v", err)
	}

	// default deny port groups
	predicateIDs = libovsdbops.NewDbObjectIDs(libovsdbops.PortGroupNetpolNamespace, bnc.controllerName, nil)
	p = libovsdbops.GetPredicate[*nbdb.PortGroup](predicateIDs, func(item *nbdb.PortGroup) bool {
		namespace := item.ExternalIDs[libovsdbops.ObjectNameKey.String()]
		if _, ok := expectedPolicies[namespace]; !ok {
			// no policies in that namespace are found, delete default deny port group
			stalePGNames.Insert(item.Name)
		}
		return false
	})
	_, err = libovsdbops.FindPortGroupsWithPredicate(bnc.nbClient, p)
	if err != nil {
		return fmt.Errorf("cannot find default deny NetworkPolicy port groups: %v", err)
	}

	if len(stalePGNames) > 0 {
		err = libovsdbops.DeletePortGroups(bnc.nbClient, sets.List[string](stalePGNames)...)
		if err != nil {
			return fmt.Errorf("error removing stale port groups %v: %v", stalePGNames, err)
		}
		klog.Infof("Network policy sync cleaned up %d stale port groups", len(stalePGNames))
	}

	return nil
}

func getAllowFromNodeACLDbIDs(nodeName, mgmtPortIP, controller string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolNode, controller,
		map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey: nodeName,
			libovsdbops.IpKey:         mgmtPortIP,
		})
}

// There is no delete function for this ACL type, because the ACL is applied on a node switch.
// When the node is deleted, switch will be deleted by the node sync, and the dependent ACLs will be
// garbage-collected.
func (bnc *BaseNetworkController) addAllowACLFromNode(nodeName string, mgmtPortIP net.IP) error {
	ipFamily := "ip4"
	if utilnet.IsIPv6(mgmtPortIP) {
		ipFamily = "ip6"
	}
	match := fmt.Sprintf("%s.src==%s", ipFamily, mgmtPortIP.String())
	dbIDs := getAllowFromNodeACLDbIDs(nodeName, mgmtPortIP.String(), bnc.controllerName)
	nodeACL := BuildACL(dbIDs, types.DefaultAllowPriority, match,
		nbdb.ACLActionAllowRelated, nil, lportIngress)

	ops, err := libovsdbops.CreateOrUpdateACLsOps(bnc.nbClient, nil, nodeACL)
	if err != nil {
		return fmt.Errorf("failed to create or update ACL %v: %v", nodeACL, err)
	}

	ops, err = libovsdbops.AddACLsToLogicalSwitchOps(bnc.nbClient, ops, nodeName, nodeACL)
	if err != nil {
		return fmt.Errorf("failed to add ACL %v to switch %s: %v", nodeACL, nodeName, err)
	}

	_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
	if err != nil {
		return err
	}

	return nil
}

// must be called with namespace lock
func (bnc *BaseNetworkController) updateACLLoggingForPolicy(np *networkPolicy, aclLogging *ACLLoggingLevels) error {
	np.RLock()
	defer np.RUnlock()
	if np.deleted {
		return nil
	}

	// Predicate for given network policy ACLs
	predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetworkPolicy, bnc.controllerName, map[libovsdbops.ExternalIDKey]string{
		libovsdbops.ObjectNameKey: getACLPolicyKey(np.namespace, np.name),
	})
	p := libovsdbops.GetPredicate[*nbdb.ACL](predicateIDs, nil)
	return UpdateACLLoggingWithPredicate(bnc.nbClient, p, aclLogging)
}

// handleNetPolNamespaceUpdate should update all network policies related to given namespace.
// Must be called with namespace Lock, should be retriable
func (bnc *BaseNetworkController) handleNetPolNamespaceUpdate(namespace string, aclLogging *ACLLoggingLevels, relatedNetworkPolicies map[string]bool) error {
	// now update network policy specific ACLs
	klog.V(5).Infof("Setting network policy ACLs for ns: %s", namespace)
	// all network policy port groups are based on a namespaced pod selector,
	// therefore all acls for a namespace is a set of shared port groups used by network policies in that namespace
	pgKeys := sets.Set[string]{}
	for npKey := range relatedNetworkPolicies {
		// this function doesn't return errors
		_ = bnc.networkPolicies.DoWithLock(npKey, func(key string) error {
			np, found := bnc.networkPolicies.Load(npKey)
			if !found {
				klog.Errorf("Netpol was deleted from cache, but not from namespace related objects")
				return nil
			}
			pgKeys.Insert(np.portGroupKey)
			return nil
		})

	}
	for _, pgKey := range pgKeys.UnsortedList() {
		err := bnc.netpolSharedPGController.UpdateACLLogging(pgKey, aclLogging)
		if err != nil {
			return fmt.Errorf("unable to update ACL for shared netpol port group %s: %v", pgKey, err)
		}
	}

	klog.Infof("ACL for namespace: %s, updated to new log level: %s", namespace, aclLogging.Allow)
	return nil
}

// getPolicyType returns whether the policy is of type ingress and/or egress
func getPolicyType(policy *knet.NetworkPolicy) (bool, bool) {
	var policyTypeIngress bool
	var policyTypeEgress bool

	for _, policyType := range policy.Spec.PolicyTypes {
		if policyType == knet.PolicyTypeIngress {
			policyTypeIngress = true
		} else if policyType == knet.PolicyTypeEgress {
			policyTypeEgress = true
		}
	}

	return policyTypeIngress, policyTypeEgress
}

func (bnc *BaseNetworkController) getNetworkPolicyPortGroupDbIDs(namespace, name string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.PortGroupNetworkPolicy, bnc.controllerName,
		map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey: fmt.Sprintf("%s_%s", namespace, name),
		})
}

func parsePGPolicyKey(pgPolicyKey string) (string, string, error) {
	s := strings.Split(pgPolicyKey, "_")
	if len(s) != 2 {
		return "", "", fmt.Errorf("failed to parse network policy port group key %s, "+
			"expected format <policyNamespace>_<policyName>", pgPolicyKey)
	}
	return s[0], s[1], nil
}

func (bnc *BaseNetworkController) getNetworkPolicyPGName(namespace, name string) string {
	return libovsdbops.GetPortGroupName(bnc.getNetworkPolicyPortGroupDbIDs(namespace, name))
}

type policyHandler struct {
	gress             *gressPolicy
	namespaceSelector *metav1.LabelSelector
}

// createNetworkPolicy creates a network policy, should be retriable.
// If network policy with given key exists, it will try to clean it up first, and return an error if it fails.
// No need to log network policy key here, because caller of createNetworkPolicy should prepend error message with
// that information.
// TODO delete stale acls on initial sync
func (bnc *BaseNetworkController) createNetworkPolicy(policy *knet.NetworkPolicy, aclLogging *ACLLoggingLevels) (*networkPolicy, error) {
	// To avoid existing connections disruption, make sure to apply allow ACLs before applying deny ACLs.
	// This requires to start peer handlers before local pod handlers.
	// 1. Cleanup old policy if it failed to be created
	// 2. Build gress policies, create addressSets for peers
	// 3. Add policy to default deny port group.
	// 4. Build policy ACLs and port group. All the local pods that this policy
	// selects will be eventually added to this port group.
	// Pods are not added to default deny port groups yet, this is just a preparation step
	// 5. Unlock networkPolicy before starting pod handlers to avoid deadlock
	// since pod handlers take np.RLock
	// 6. Start peer handlers to update all allow rules first
	// 7. Start local pod handlers, that will update networkPolicy and default deny port groups with selected pods.

	npKey := getPolicyKey(policy)
	var np *networkPolicy
	var policyHandlers []*policyHandler

	// network policy will be annotated with this
	// annotation -- [ "k8s.ovn.org/acl-stateless": "true"] for the ingress/egress
	// policies to be added as stateless OVN ACL's.
	// if the above annotation is not present or set to false in network policy,
	// then corresponding egress/ingress policies will be added as stateful OVN ACL's.
	var statelessNetPol bool
	if config.OVNKubernetesFeature.EnableStatelessNetPol {
		// look for stateless annotation if the statlessNetPol feature flag is enabled
		val, ok := policy.Annotations[ovnStatelessNetPolAnnotationName]
		if ok && val == "true" {
			statelessNetPol = true
		}
	}

	err := bnc.networkPolicies.DoWithLock(npKey, func(npKey string) error {
		oldNP, found := bnc.networkPolicies.Load(npKey)
		if found {
			// 1. Cleanup old policy if it failed to be created
			if cleanupErr := bnc.cleanupNetworkPolicy(oldNP); cleanupErr != nil {
				return fmt.Errorf("cleanup for retrying network policy create failed: %v", cleanupErr)
			}
		}
		np, found = bnc.networkPolicies.LoadOrStore(npKey, NewNetworkPolicy(policy))
		if found {
			// that should never happen, because successful cleanup will delete np from bnc.networkPolicies
			return fmt.Errorf("network policy is found in the system, "+
				"while it should've been cleaned up, obj: %+v", np)
		}
		np.Lock()
		npLocked := true
		// we unlock np in the middle of this function, use npLocked to track if it was already unlocked explicitly
		defer func() {
			if npLocked {
				np.Unlock()
			}
		}()
		// no need to check np.deleted, since the object has just been created
		// now we have a new np stored in bnc.networkPolicies
		var err error

		if aclLogging.Deny != "" || aclLogging.Allow != "" {
			klog.Infof("ACL logging for network policy %s in namespace %s set to deny=%s, allow=%s",
				policy.Name, policy.Namespace, aclLogging.Deny, aclLogging.Allow)
		}

		// 2. Build gress policies, create addressSets for peers

		// Consider both ingress and egress rules of the policy regardless of this
		// policy type. A pod is isolated as long as as it is selected by any
		// namespace policy. Since we don't process all namespace policies on a
		// given policy update that might change the isolation status of a selected
		// pod, we have created the allow ACLs derived from the policy rules in case
		// the selected pods become isolated in the future even if that is not their
		// current status.

		// Go through each ingress rule.  For each ingress rule, create an
		// addressSet for the peer pods.
		for i, ingressJSON := range policy.Spec.Ingress {
			klog.V(5).Infof("Network policy ingress is %+v", ingressJSON)

			ingress := newGressPolicy(knet.PolicyTypeIngress, i, policy.Namespace, policy.Name, bnc.controllerName, statelessNetPol, bnc.NetInfo)
			// append ingress policy to be able to cleanup created address set
			// see cleanupNetworkPolicy for details
			np.ingressPolicies = append(np.ingressPolicies, ingress)

			// Each ingress rule can have multiple ports to which we allow traffic.
			for _, portJSON := range ingressJSON.Ports {
				ingress.addPortPolicy(&portJSON)
			}

			for _, fromJSON := range ingressJSON.From {
				handler, err := bnc.setupGressPolicy(np, ingress, fromJSON)
				if err != nil {
					return err
				}
				if handler != nil {
					policyHandlers = append(policyHandlers, handler)
				}
			}
		}

		// Go through each egress rule.  For each egress rule, create an
		// addressSet for the peer pods.
		for i, egressJSON := range policy.Spec.Egress {
			klog.V(5).Infof("Network policy egress is %+v", egressJSON)

			egress := newGressPolicy(knet.PolicyTypeEgress, i, policy.Namespace, policy.Name, bnc.controllerName, statelessNetPol, bnc.NetInfo)
			// append ingress policy to be able to cleanup created address set
			// see cleanupNetworkPolicy for details
			np.egressPolicies = append(np.egressPolicies, egress)

			// Each egress rule can have multiple ports to which we allow traffic.
			for _, portJSON := range egressJSON.Ports {
				egress.addPortPolicy(&portJSON)
			}

			for _, toJSON := range egressJSON.To {
				handler, err := bnc.setupGressPolicy(np, egress, toJSON)
				if err != nil {
					return err
				}
				if handler != nil {
					policyHandlers = append(policyHandlers, handler)
				}
			}
		}
		klog.Infof("Policy %s added to peer address sets %v", npKey, np.peerAddressSets)

		// 5. Unlock network policy before starting pod handlers to avoid deadlock,
		// since pod handlers take np.RLock
		np.Unlock()
		npLocked = false

		// 6. Start peer handlers to update all allow rules first
		for _, handler := range policyHandlers {
			// For each peer namespace selector, we create a watcher that
			// populates ingress.peerAddressSets
			err = bnc.addPeerNamespaceHandler(handler.namespaceSelector, handler.gress, np)
			if err != nil {
				return fmt.Errorf("failed to start peer handler: %v", err)
			}
		}

		//7. Start local pod handlers, that will update networkPolicy and default deny port groups with selected pods.
		err = bnc.netpolSharedPGController.AddPolicyACLs(np, aclLogging)
		if err != nil {
			return err
		}

		return nil
	})
	return np, err
}

func (bnc *BaseNetworkController) setupGressPolicy(np *networkPolicy, gp *gressPolicy,
	peer knet.NetworkPolicyPeer) (*policyHandler, error) {
	// Add IPBlock to ingress network policy
	if peer.IPBlock != nil {
		gp.addIPBlock(peer.IPBlock)
		return nil, nil
	}
	if peer.PodSelector == nil && peer.NamespaceSelector == nil {
		// undefined behaviour
		klog.Errorf("setupGressPolicy failed: all fields unset")
		return nil, nil
	}
	gp.hasPeerSelector = true

	podSelector := peer.PodSelector
	if podSelector == nil {
		// nil pod selector is equivalent to empty pod selector, which selects all
		podSelector = &metav1.LabelSelector{}
	}
	podSel, _ := metav1.LabelSelectorAsSelector(podSelector)
	nsSel, _ := metav1.LabelSelectorAsSelector(peer.NamespaceSelector)

	if podSel.Empty() && (peer.NamespaceSelector == nil || !nsSel.Empty()) {
		// namespace-based filtering
		if peer.NamespaceSelector == nil {
			// nil namespace selector means same namespace
			_, err := gp.addNamespaceAddressSet(np.namespace, bnc.addressSetFactory)
			if err != nil {
				return nil, fmt.Errorf("failed to add namespace address set for gress policy: %w", err)
			}
		} else if !nsSel.Empty() {
			// namespace selector, use namespace address sets
			handler := &policyHandler{
				gress:             gp,
				namespaceSelector: peer.NamespaceSelector,
			}
			return handler, nil
		}
	} else {
		// use podSelector address set
		// np.namespace will be used when fromJSON.NamespaceSelector = nil
		asKey, ipv4as, ipv6as, err := bnc.podAddressSetController.EnsureAddressSet(
			podSelector, peer.NamespaceSelector, np.namespace, np.getKeyWithKind())
		// even if GetPodSelectorAddressSet failed, add key for future cleanup or retry.
		np.peerAddressSets = append(np.peerAddressSets, asKey)
		if err != nil {
			return nil, fmt.Errorf("failed to ensure pod selector address set %s: %v", asKey, err)
		}
		gp.addPeerAddressSets(ipv4as, ipv6as)
	}
	return nil, nil
}

// addNetworkPolicy creates and applies OVN ACLs to pod logical switch
// ports from Kubernetes NetworkPolicy objects using OVN Port Groups
// if addNetworkPolicy fails, create or delete operation can be retried
func (bnc *BaseNetworkController) addNetworkPolicy(policy *knet.NetworkPolicy) error {
	klog.Infof("Adding network policy %s for network %s", getPolicyKey(policy), bnc.GetNetworkName())
	if !bnc.IsSecondary() && config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolEvent("add", duration)
		}()
	}

	// To not hold nsLock for the whole process on network policy creation, we do the following:
	// 1. save required namespace information to use for netpol create
	// 2. create network policy without ns Lock
	// 3. subscribe to namespace events with ns Lock

	// 1. save required namespace information to use for netpol create,
	npKey := getPolicyKey(policy)
	aclLogging := bnc.GetNamespaceACLLogging(policy.Namespace)

	// 2. create network policy without ns Lock, cleanup on failure
	var np *networkPolicy
	var err error

	np, err = bnc.createNetworkPolicy(policy, aclLogging)
	defer func() {
		if err != nil {
			klog.Infof("Create network policy %s failed, try to cleanup", npKey)
			// try to cleanup network policy straight away
			// it will be retried later with add/delete network policy handlers if it fails
			cleanupErr := bnc.networkPolicies.DoWithLock(npKey, func(npKey string) error {
				np, ok := bnc.networkPolicies.Load(npKey)
				if !ok {
					klog.Infof("Deleting policy %s that is already deleted", npKey)
					return nil
				}
				return bnc.cleanupNetworkPolicy(np)
			})
			if cleanupErr != nil {
				klog.Infof("Cleanup for failed create network policy %s returned an error: %v",
					npKey, cleanupErr)
			}
		}
	}()
	if err != nil {
		return fmt.Errorf("failed to create Network Policy %s: %v", npKey, err)
	}
	klog.Infof("Create network policy %s resources completed, update namespace loglevel", npKey)

	// 3. subscribe to namespace updates
	nsSync := func(latestACLLogging *ACLLoggingLevels) error {
		// check if namespace information related to network policy has changed,
		// network policy only reacts to namespace update ACL log level.
		// Run handleNetPolNamespaceUpdate sequence, but only for 1 newly added policy.
		if err := bnc.netpolSharedPGController.UpdateACLLogging(np.portGroupKey, latestACLLogging); err != nil {
			return fmt.Errorf("network policy %s failed to be created: update default deny ACLs failed: %v", npKey, err)
		}
		return nil
	}

	err = bnc.subscribeToNamespaceUpdates(np.namespace, npKey, nsSync)
	return err
}

// buildNetworkPolicyACLs builds the ACLS associated with the 'gress policies
// of the provided network policy.
func buildNetworkPolicyACLs(np *networkPolicy, aclLogging *ACLLoggingLevels) []*nbdb.ACL {
	acls := []*nbdb.ACL{}
	for _, gp := range np.ingressPolicies {
		acl := gp.buildLocalPodACLs(np.portGroupName, aclLogging)
		acls = append(acls, acl...)
	}
	for _, gp := range np.egressPolicies {
		acl := gp.buildLocalPodACLs(np.portGroupName, aclLogging)
		acls = append(acls, acl...)
	}

	return acls
}

// deleteNetworkPolicy removes a network policy
// It only uses Namespace and Name from given network policy
func (bnc *BaseNetworkController) deleteNetworkPolicy(policy *knet.NetworkPolicy) error {
	npKey := getPolicyKey(policy)
	klog.Infof("Deleting network policy %s", npKey)
	if config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolEvent("delete", duration)
		}()
	}
	// First lock and update namespace
	bnc.unsubscribeFromNamespaceUpdates(policy.Namespace, npKey)
	// Next cleanup network policy
	err := bnc.networkPolicies.DoWithLock(npKey, func(npKey string) error {
		np, ok := bnc.networkPolicies.Load(npKey)
		if !ok {
			klog.Infof("Deleting policy %s that is already deleted", npKey)
			return nil
		}
		if err := bnc.cleanupNetworkPolicy(np); err != nil {
			return fmt.Errorf("deleting policy %s failed: %v", npKey, err)
		}
		return nil
	})
	return err
}

// cleanupNetworkPolicy should be retriable
// It takes and releases networkPolicy lock.
// It updates bnc.networkPolicies on success, should be called with bnc.networkPolicies key locked.
// No need to log network policy key here, because caller of cleanupNetworkPolicy should prepend error message with
// that information.
func (bnc *BaseNetworkController) cleanupNetworkPolicy(np *networkPolicy) error {
	npKey := np.getKey()
	klog.Infof("Cleaning up network policy %s", npKey)
	np.Lock()
	defer np.Unlock()

	// signal to local pod/peer handlers to ignore new events
	np.deleted = true

	// stop handlers, retriable
	bnc.shutdownHandlers(np)
	var err error

	// delete from peer address set
	for i, asKey := range np.peerAddressSets {
		if err := bnc.podAddressSetController.DeleteAddressSet(asKey, np.getKeyWithKind()); err != nil {
			// remove deleted address sets from the list
			np.peerAddressSets = np.peerAddressSets[i:]
			return fmt.Errorf("failed to delete network policy from peer address set %s: %v", asKey, err)
		}
	}
	np.peerAddressSets = nil

	err = bnc.netpolSharedPGController.DelPolicyACLs(np)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	// finally, delete netpol from existing networkPolicies
	// this is the signal that cleanup was successful
	bnc.networkPolicies.Delete(npKey)
	return nil
}

type NetworkPolicyExtraParameters struct {
	np *networkPolicy
	gp *gressPolicy
}

func (bnc *BaseNetworkController) handlePeerNamespaceSelectorAdd(np *networkPolicy, gp *gressPolicy, objs ...interface{}) error {
	if !bnc.IsSecondary() && config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolPeerNamespaceEvent("add", duration)
		}()
	}
	np.RLock()
	defer np.RUnlock()
	if np.deleted {
		return nil
	}
	updated := false
	var errors []error
	for _, obj := range objs {
		namespace := obj.(*kapi.Namespace)
		// addNamespaceAddressSet is safe for concurrent use, doesn't require additional synchronization
		nsUpdated, err := gp.addNamespaceAddressSet(namespace.Name, bnc.addressSetFactory)
		if err != nil {
			errors = append(errors, err)
		} else if nsUpdated {
			updated = true
		}
	}
	if updated {
		err := bnc.peerNamespaceUpdate(np, gp)
		if err != nil {
			errors = append(errors, err)
		}
	}
	return kerrorsutil.NewAggregate(errors)

}

func (bnc *BaseNetworkController) handlePeerNamespaceSelectorDel(np *networkPolicy, gp *gressPolicy, objs ...interface{}) error {
	if !bnc.IsSecondary() && config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolPeerNamespaceEvent("delete", duration)
		}()
	}
	np.RLock()
	defer np.RUnlock()
	if np.deleted {
		return nil
	}
	updated := false
	for _, obj := range objs {
		namespace := obj.(*kapi.Namespace)
		// delNamespaceAddressSet is safe for concurrent use, doesn't require additional synchronization
		if gp.delNamespaceAddressSet(namespace.Name) {
			updated = true
		}
	}
	// unlock networkPolicy, before calling peerNamespaceUpdate
	if updated {
		return bnc.peerNamespaceUpdate(np, gp)
	}
	return nil
}

// must be called with np.Lock
func (bnc *BaseNetworkController) peerNamespaceUpdate(np *networkPolicy, gp *gressPolicy) error {
	// buildLocalPodACLs is safe for concurrent use, see function comment for details
	// we don't care about logLevels, since this function only updated Match
	acls := gp.buildLocalPodACLs(np.portGroupName, &ACLLoggingLevels{})
	ops, err := libovsdbops.UpdateACLsMatchOps(bnc.nbClient, nil, acls...)
	if err != nil {
		return err
	}
	_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
	return err
}

// addPeerNamespaceHandler starts a watcher for PeerNamespaceSelectorType.
// Sync function and Add event for every existing namespace will be executed sequentially first, and an error will be
// returned if something fails.
// PeerNamespaceSelectorType uses handlePeerNamespaceSelectorAdd on Add,
// and handlePeerNamespaceSelectorDel on Delete.
func (bnc *BaseNetworkController) addPeerNamespaceHandler(
	namespaceSelector *metav1.LabelSelector,
	gress *gressPolicy, np *networkPolicy) error {

	// NetworkPolicy is validated by the apiserver; this can't fail.
	sel, _ := metav1.LabelSelectorAsSelector(namespaceSelector)

	// start watching namespaces selected by the namespace selector
	syncFunc := func(objs []interface{}) error {
		// ignore returned error, since any namespace that wasn't properly handled will be retried individually.
		_ = bnc.handlePeerNamespaceSelectorAdd(np, gress, objs...)
		return nil
	}
	retryPeerNamespaces := bnc.newNetpolRetryFramework(
		factory.PeerNamespaceSelectorType,
		syncFunc,
		&NetworkPolicyExtraParameters{gp: gress, np: np},
	)

	namespaceHandler, err := retryPeerNamespaces.WatchResourceFiltered("", sel)
	if err != nil {
		klog.Errorf("WatchResource failed for addPeerNamespaceHandler: %v", err)
		return err
	}

	np.nsHandlerList = append(np.nsHandlerList, namespaceHandler)
	return nil
}

func (bnc *BaseNetworkController) shutdownHandlers(np *networkPolicy) {
	if np.localPodHandler != nil {
		bnc.watchFactory.RemovePodHandler(np.localPodHandler)
		np.localPodHandler = nil
	}
	for _, handler := range np.nsHandlerList {
		bnc.watchFactory.RemoveNamespaceHandler(handler)
	}
	np.nsHandlerList = make([]*factory.Handler, 0)
}

// The following 2 functions should return the same key for network policy based on k8s on internal networkPolicy object
func getPolicyKey(policy *knet.NetworkPolicy) string {
	return fmt.Sprintf("%v/%v", policy.Namespace, policy.Name)
}

func (np *networkPolicy) getKey() string {
	return fmt.Sprintf("%v/%v", np.namespace, np.name)
}

func (np *networkPolicy) getKeyWithKind() string {
	return fmt.Sprintf("%v/%v/%v", "NetworkPolicy", np.namespace, np.name)
}

// PortGroupHasPorts returns true if a port group contains all given ports
func PortGroupHasPorts(nbClient libovsdbclient.Client, pgName string, portUUIDs []string) bool {
	pg := &nbdb.PortGroup{
		Name: pgName,
	}
	pg, err := libovsdbops.GetPortGroup(nbClient, pg)
	if err != nil {
		return false
	}

	return sets.NewString(pg.Ports...).HasAll(portUUIDs...)
}

// getStaleNetpolAddrSetDbIDs returns the ids for address sets that were owned by network policy before we
// switched to shared address sets with PodSelectorAddressSet. Should only be used for sync and testing.
func getStaleNetpolAddrSetDbIDs(policyNamespace, policyName, policyType, idx, controller string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.AddressSetNetworkPolicy, controller, map[libovsdbops.ExternalIDKey]string{
		libovsdbops.ObjectNameKey: policyNamespace + "_" + policyName,
		// direction and idx uniquely identify address set (= gress policy rule)
		libovsdbops.PolicyDirectionKey: strings.ToLower(policyType),
		libovsdbops.GressIdxKey:        idx,
	})
}

func (bnc *BaseNetworkController) getNetpolDefaultACLDbIDs(direction string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolDefault, bnc.controllerName,
		map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey:      allowHairpinningACLID,
			libovsdbops.PolicyDirectionKey: direction,
		})
}
