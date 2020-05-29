package ovn

import (
	"fmt"
	"net"
	"sync"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

type namespacePolicy struct {
	sync.Mutex
	name            string
	namespace       string
	ingressPolicies []*gressPolicy
	egressPolicies  []*gressPolicy
	podHandlerList  []*factory.Handler
	nsHandlerList   []*factory.Handler
	localPods       map[string]*lpInfo //pods effected by this policy
	portGroupUUID   string             //uuid for OVN port_group
	portGroupName   string
	deleted         bool //deleted policy
}

func NewNamespacePolicy(policy *knet.NetworkPolicy) *namespacePolicy {
	np := &namespacePolicy{
		name:            policy.Name,
		namespace:       policy.Namespace,
		ingressPolicies: make([]*gressPolicy, 0),
		egressPolicies:  make([]*gressPolicy, 0),
		podHandlerList:  make([]*factory.Handler, 0),
		nsHandlerList:   make([]*factory.Handler, 0),
		localPods:       make(map[string]*lpInfo),
	}
	return np
}

const (
	toLport   = "to-lport"
	fromLport = "from-lport"
	noneMatch = "None"
	// Default deny acl rule priority
	defaultDenyPriority = "1000"
	// Default allow acl rule priority
	defaultAllowPriority = "1001"
	// IP Block except deny acl rule priority
	ipBlockDenyPriority = "1010"
	// Default multicast deny acl rule priority
	defaultMcastDenyPriority = "1011"
	// Default multicast allow acl rule priority
	defaultMcastAllowPriority = "1012"
)

func (oc *Controller) syncNetworkPolicies(networkPolicies []interface{}) {
	expectedPolicies := make(map[string]map[string]bool)
	for _, npInterface := range networkPolicies {
		policy, ok := npInterface.(*knet.NetworkPolicy)
		if !ok {
			klog.Errorf("Spurious object in syncNetworkPolicies: %v",
				npInterface)
			continue
		}

		if nsMap, ok := expectedPolicies[policy.Namespace]; ok {
			nsMap[policy.Name] = true
		} else {
			expectedPolicies[policy.Namespace] = map[string]bool{
				policy.Name: true,
			}
		}
	}

	err := oc.forEachAddressSetUnhashedName(func(addrSetName, namespaceName,
		policyName string) {
		if policyName != "" &&
			!expectedPolicies[namespaceName][policyName] {
			// policy doesn't exist on k8s. Delete the port group
			portGroupName := fmt.Sprintf("%s_%s", namespaceName, policyName)
			hashedLocalPortGroup := hashedPortGroup(portGroupName)
			deletePortGroup(hashedLocalPortGroup)

			// delete the address sets for this policy from OVN
			deleteAddressSet(hashedAddressSet(addrSetName))
		}
	})
	if err != nil {
		klog.Errorf("Error in syncing network policies: %v", err)
	}
}

func addAllowACLFromNode(logicalSwitch string, mgmtPortIP net.IP) error {
	match := fmt.Sprintf("%s.src==%s", ipMatch(), mgmtPortIP.String())
	_, stderr, err := util.RunOVNNbctl("--may-exist", "acl-add", logicalSwitch,
		"to-lport", defaultAllowPriority, match, "allow-related")
	if err != nil {
		return fmt.Errorf("failed to create the node acl for "+
			"logical_switch=%s, stderr: %q (%v)", logicalSwitch, stderr, err)
	}

	return nil
}

func getACLMatch(portGroupName, match string, policyType knet.PolicyType) string {
	var aclMatch string
	if policyType == knet.PolicyTypeIngress {
		aclMatch = "outport == @" + portGroupName
	} else {
		aclMatch = "inport == @" + portGroupName
	}

	if match != "" {
		aclMatch += " && " + match
	}

	return "match=\"" + aclMatch + "\""
}

func addACLPortGroup(portGroupUUID, portGroupName, direction, priority, match, action string, policyType knet.PolicyType) error {
	match = getACLMatch(portGroupName, match, policyType)
	uuid, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=_uuid", "find", "ACL", match, "action="+action,
		fmt.Sprintf("external-ids:default-deny-policy-type=%s", policyType))
	if err != nil {
		return fmt.Errorf("find failed to get the default deny rule for "+
			"policy type %s stderr: %q (%v)", policyType, stderr, err)
	}

	if uuid != "" {
		return nil
	}

	_, stderr, err = util.RunOVNNbctl("--id=@acl", "create", "acl",
		fmt.Sprintf("priority=%s", priority),
		fmt.Sprintf("direction=%s", direction), match, "action="+action,
		fmt.Sprintf("external-ids:default-deny-policy-type=%s", policyType),
		"--", "add", "port_group", portGroupUUID,
		"acls", "@acl")
	if err != nil {
		return fmt.Errorf("error executing create ACL command for "+
			"policy type %s stderr: %q (%v)", policyType, stderr, err)
	}
	return nil
}

func deleteACLPortGroup(portGroupName, direction, priority, match, action string, policyType knet.PolicyType) error {
	match = getACLMatch(portGroupName, match, policyType)
	uuid, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=_uuid", "find", "ACL", match, "action="+action,
		fmt.Sprintf("external-ids:default-deny-policy-type=%s", policyType))
	if err != nil {
		return fmt.Errorf("find failed to get the rule for "+
			"policy type %s stderr: %q (%v)", policyType, stderr, err)
	}

	if uuid == "" {
		return nil
	}

	_, stderr, err = util.RunOVNNbctl("remove", "port_group",
		portGroupName, "acls", uuid)
	if err != nil {
		return fmt.Errorf("remove failed to delete the rule for "+
			"port_group=%s, stderr: %q (%v)", portGroupName, stderr, err)
	}

	return nil
}

func addToPortGroup(portGroup string, portInfo *lpInfo) error {
	_, stderr, err := util.RunOVNNbctl("--if-exists", "remove",
		"port_group", portGroup, "ports", portInfo.uuid, "--",
		"add", "port_group", portGroup, "ports", portInfo.uuid)
	if err != nil {
		return fmt.Errorf("failed to add logicalPort %s to portGroup %s "+
			"stderr: %q (%v)", portInfo.name, portGroup, stderr, err)
	}
	return nil
}

func deleteFromPortGroup(portGroup string, portInfo *lpInfo) error {
	_, stderr, err := util.RunOVNNbctl("--if-exists", "remove",
		"port_group", portGroup, "ports", portInfo.uuid)
	if err != nil {
		return fmt.Errorf("failed to delete logicalPort %s to portGroup %s "+
			"stderr: %q (%v)", portInfo.name, portGroup, stderr, err)
	}
	return nil
}

func (oc *Controller) createDefaultDenyPortGroup(policyType knet.PolicyType) error {
	var portGroupName string
	if policyType == knet.PolicyTypeIngress {
		if oc.portGroupIngressDeny != "" {
			return nil
		}
		portGroupName = "ingressDefaultDeny"
	} else if policyType == knet.PolicyTypeEgress {
		if oc.portGroupEgressDeny != "" {
			return nil
		}
		portGroupName = "egressDefaultDeny"
	}
	portGroupUUID, err := createPortGroup(portGroupName, portGroupName)
	if err != nil {
		return fmt.Errorf("Failed to create port_group for %s (%v)",
			portGroupName, err)
	}
	err = addACLPortGroup(portGroupUUID, portGroupName, toLport,
		defaultDenyPriority, "", "drop", policyType)
	if err != nil {
		return fmt.Errorf("Failed to create default deny ACL for port group %v", err)
	}

	err = addACLPortGroup(portGroupUUID, portGroupName, toLport,
		defaultAllowPriority, "arp", "allow", policyType)
	if err != nil {
		return fmt.Errorf("Failed to create default allow ARP ACL for port group %v", err)
	}

	if policyType == knet.PolicyTypeIngress {
		oc.portGroupIngressDeny = portGroupUUID
	} else if policyType == knet.PolicyTypeEgress {
		oc.portGroupEgressDeny = portGroupUUID
	}
	return nil
}

// Creates the match string used for ACLs allowing incoming multicast into a
// namespace, that is, from IPs that are in the namespace's address set.
func getMulticastACLMatch(ns string) string {
	nsAddressSet := hashedAddressSet(ns)
	return "ip4.src == $" + nsAddressSet + " && ip4.mcast"
}

// Creates a policy to allow multicast traffic within 'ns':
// - a port group containing all logical ports associated with 'ns'
// - one "from-lport" ACL allowing egress multicast traffic from the pods
//   in 'ns'
// - one "to-lport" ACL allowing ingress multicast traffic to pods in 'ns'.
//   This matches only traffic originated by pods in 'ns' (based on the
//   namespace address set).
func (oc *Controller) createMulticastAllowPolicy(ns string, nsInfo *namespaceInfo) error {
	err := nsInfo.updateNamespacePortGroup(ns)
	if err != nil {
		return err
	}

	err = addACLPortGroup(nsInfo.portGroupUUID, hashedPortGroup(ns), fromLport,
		defaultMcastAllowPriority, "ip4.mcast", "allow",
		knet.PolicyTypeEgress)
	if err != nil {
		return fmt.Errorf("Failed to create allow egress multicast ACL for %s (%v)",
			ns, err)
	}

	err = addACLPortGroup(nsInfo.portGroupUUID, hashedPortGroup(ns), toLport,
		defaultMcastAllowPriority, getMulticastACLMatch(ns), "allow",
		knet.PolicyTypeIngress)
	if err != nil {
		return fmt.Errorf("Failed to create allow ingress multicast ACL for %s (%v)",
			ns, err)
	}

	// Add all ports from this namespace to the multicast allow group.
	for _, portName := range nsInfo.addressSet {
		if portInfo, err := oc.logicalPortCache.get(portName); err != nil {
			klog.Errorf(err.Error())
		} else if err := podAddAllowMulticastPolicy(ns, portInfo); err != nil {
			klog.Warningf("failed to add port %s to port group ACL: %v", portName, err)
		}
	}

	return nil
}

func deleteMulticastACLs(ns, portGroupHash string) error {
	err := deleteACLPortGroup(portGroupHash, fromLport,
		defaultMcastAllowPriority, "ip4.mcast", "allow",
		knet.PolicyTypeEgress)
	if err != nil {
		return fmt.Errorf("Failed to delete allow egress multicast ACL for %s (%v)",
			ns, err)
	}

	err = deleteACLPortGroup(portGroupHash, toLport,
		defaultMcastAllowPriority, getMulticastACLMatch(ns), "allow",
		knet.PolicyTypeIngress)
	if err != nil {
		return fmt.Errorf("Failed to delete allow ingress multicast ACL for %s (%v)",
			ns, err)
	}

	return nil
}

// Delete the policy to allow multicast traffic within 'ns'.
func deleteMulticastAllowPolicy(ns string, nsInfo *namespaceInfo) error {
	portGroupHash := hashedPortGroup(ns)

	err := deleteMulticastACLs(ns, portGroupHash)
	if err != nil {
		return err
	}

	_ = nsInfo.updateNamespacePortGroup(ns)
	return nil
}

// Creates a global default deny multicast policy:
// - one ACL dropping egress multicast traffic from all pods: this is to
//   protect OVN controller from processing IP multicast reports from nodes
//   that are not allowed to receive multicast raffic.
// - one ACL dropping ingress multicast traffic to all pods.
func createDefaultDenyMulticastPolicy() error {
	portGroupName := "mcastPortGroupDeny"
	portGroupUUID, err := createPortGroup(portGroupName, portGroupName)
	if err != nil {
		return fmt.Errorf("Failed to create port_group for %s (%v)",
			portGroupName, err)
	}

	// By default deny any egress multicast traffic from any pod. This drops
	// IP multicast membership reports therefore denying any multicast traffic
	// to be forwarded to pods.
	err = addACLPortGroup(portGroupUUID, portGroupName, fromLport,
		defaultMcastDenyPriority, "ip4.mcast", "drop", knet.PolicyTypeEgress)
	if err != nil {
		return fmt.Errorf("Failed to create default deny multicast egress ACL (%v)",
			err)
	}

	// By default deny any ingress multicast traffic to any pod.
	err = addACLPortGroup(portGroupUUID, portGroupName, toLport,
		defaultMcastDenyPriority, "ip4.mcast", "drop", knet.PolicyTypeIngress)
	if err != nil {
		return fmt.Errorf("Failed to create default deny multicast ingress ACL (%v)",
			err)
	}

	return nil
}

func podAddDefaultDenyMulticastPolicy(portInfo *lpInfo) error {
	if err := addToPortGroup("mcastPortGroupDeny", portInfo); err != nil {
		return fmt.Errorf("failed to add port %s to default multicast deny ACL: %v", portInfo.name, err)
	}
	return nil
}

func podDeleteDefaultDenyMulticastPolicy(portInfo *lpInfo) error {
	if err := deleteFromPortGroup("mcastPortGroupDeny", portInfo); err != nil {
		return fmt.Errorf("failed to delete port %s from default multicast deny ACL: %v", portInfo.name, err)
	}
	return nil
}

func podAddAllowMulticastPolicy(ns string, portInfo *lpInfo) error {
	return addToPortGroup(hashedPortGroup(ns), portInfo)
}

func podDeleteAllowMulticastPolicy(ns string, portInfo *lpInfo) error {
	return deleteFromPortGroup(hashedPortGroup(ns), portInfo)
}

func (oc *Controller) localPodAddDefaultDeny(
	policy *knet.NetworkPolicy, portInfo *lpInfo) {
	oc.lspMutex.Lock()
	defer oc.lspMutex.Unlock()

	err := oc.createDefaultDenyPortGroup(knet.PolicyTypeIngress)
	if err != nil {
		klog.Errorf(err.Error())
		return
	}
	err = oc.createDefaultDenyPortGroup(knet.PolicyTypeEgress)
	if err != nil {
		klog.Errorf(err.Error())
		return
	}

	// Default deny rule.
	// 1. Any pod that matches a network policy should get a default
	// ingress deny rule.  This is irrespective of whether there
	// is a ingress section in the network policy. But, if
	// PolicyTypes in the policy has only "egress" in it, then
	// it is a 'egress' only network policy and we should not
	// add any default deny rule for ingress.
	// 2. If there is any "egress" section in the policy or
	// the PolicyTypes has 'egress' in it, we add a default
	// egress deny rule.

	// Handle condition 1 above.
	if !(len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress) {
		if oc.lspIngressDenyCache[portInfo.name] == 0 {
			if err := addToPortGroup(oc.portGroupIngressDeny, portInfo); err != nil {
				klog.Warningf("failed to add port %s to ingress deny ACL: %v", portInfo.name, err)
			}
		}
		oc.lspIngressDenyCache[portInfo.name]++
	}

	// Handle condition 2 above.
	if (len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress) ||
		len(policy.Spec.Egress) > 0 || len(policy.Spec.PolicyTypes) == 2 {
		if oc.lspEgressDenyCache[portInfo.name] == 0 {
			if err := addToPortGroup(oc.portGroupEgressDeny, portInfo); err != nil {
				klog.Warningf("failed to add port %s to egress deny ACL: %v", portInfo.name, err)
			}
		}
		oc.lspEgressDenyCache[portInfo.name]++
	}
}

func (oc *Controller) localPodDelDefaultDeny(
	policy *knet.NetworkPolicy, portInfo *lpInfo) {
	oc.lspMutex.Lock()
	defer oc.lspMutex.Unlock()

	if !(len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress) {
		if oc.lspIngressDenyCache[portInfo.name] > 0 {
			oc.lspIngressDenyCache[portInfo.name]--
			if oc.lspIngressDenyCache[portInfo.name] == 0 {
				if err := deleteFromPortGroup(oc.portGroupIngressDeny, portInfo); err != nil {
					klog.Warningf("failed to remove port %s from ingress deny ACL: %v", portInfo.name, err)
				}
			}
		}
	}

	if (len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress) ||
		len(policy.Spec.Egress) > 0 || len(policy.Spec.PolicyTypes) == 2 {
		if oc.lspEgressDenyCache[portInfo.name] > 0 {
			oc.lspEgressDenyCache[portInfo.name]--
			if oc.lspEgressDenyCache[portInfo.name] == 0 {
				if err := deleteFromPortGroup(oc.portGroupEgressDeny, portInfo); err != nil {
					klog.Warningf("failed to remove port %s from egress deny ACL: %v", portInfo.name, err)
				}
			}
		}
	}
}

func (oc *Controller) handleLocalPodSelectorAddFunc(
	policy *knet.NetworkPolicy, np *namespacePolicy,
	obj interface{}) {
	pod := obj.(*kapi.Pod)

	if pod.Spec.NodeName == "" {
		return
	}

	// Get the logical port info
	logicalPort := podLogicalPortName(pod)
	portInfo, err := oc.logicalPortCache.get(logicalPort)
	if err != nil {
		klog.Errorf(err.Error())
		return
	}

	np.Lock()
	defer np.Unlock()

	if np.deleted {
		return
	}

	if _, ok := np.localPods[logicalPort]; ok {
		return
	}

	oc.localPodAddDefaultDeny(policy, portInfo)

	if np.portGroupUUID == "" {
		return
	}

	_, stderr, err := util.RunOVNNbctl("--if-exists", "remove",
		"port_group", np.portGroupUUID, "ports", portInfo.uuid, "--",
		"add", "port_group", np.portGroupUUID, "ports", portInfo.uuid)
	if err != nil {
		klog.Errorf("Failed to add logicalPort %s to portGroup %s "+
			"stderr: %q (%v)", logicalPort, np.portGroupUUID, stderr, err)
	}

	np.localPods[logicalPort] = portInfo
}

func (oc *Controller) handleLocalPodSelectorDelFunc(
	policy *knet.NetworkPolicy, np *namespacePolicy,
	obj interface{}) {
	pod := obj.(*kapi.Pod)

	if pod.Spec.NodeName == "" {
		return
	}

	// Get the logical port info
	logicalPort := podLogicalPortName(pod)
	portInfo, err := oc.logicalPortCache.get(logicalPort)
	if err != nil {
		klog.Errorf(err.Error())
		return
	}

	np.Lock()
	defer np.Unlock()

	if np.deleted {
		return
	}

	if _, ok := np.localPods[logicalPort]; !ok {
		return
	}
	delete(np.localPods, logicalPort)
	oc.localPodDelDefaultDeny(policy, portInfo)

	oc.lspMutex.Lock()
	delete(oc.lspIngressDenyCache, logicalPort)
	delete(oc.lspEgressDenyCache, logicalPort)
	oc.lspMutex.Unlock()

	if np.portGroupUUID == "" {
		return
	}

	_, stderr, err := util.RunOVNNbctl("--if-exists", "remove",
		"port_group", np.portGroupUUID, "ports", portInfo.uuid)
	if err != nil {
		klog.Errorf("Failed to delete logicalPort %s from portGroup %s "+
			"stderr: %q (%v)", portInfo.uuid, np.portGroupUUID, stderr, err)
	}
}

func (oc *Controller) handleLocalPodSelector(
	policy *knet.NetworkPolicy, np *namespacePolicy) {

	h, err := oc.watchFactory.AddFilteredPodHandler(policy.Namespace,
		&policy.Spec.PodSelector,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				oc.handleLocalPodSelectorAddFunc(policy, np, obj)
			},
			DeleteFunc: func(obj interface{}) {
				oc.handleLocalPodSelectorDelFunc(policy, np, obj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oc.handleLocalPodSelectorAddFunc(policy, np, newObj)
			},
		}, nil)
	if err != nil {
		klog.Errorf("error watching local pods for policy %s in namespace %s: %v",
			policy.Name, policy.Namespace, err)
		return
	}

	np.podHandlerList = append(np.podHandlerList, h)
}

// we only need to create an address set if there is a podSelector or namespaceSelector
func hasAnyLabelSelector(peers []knet.NetworkPolicyPeer) bool {
	for _, peer := range peers {
		if peer.PodSelector != nil || peer.NamespaceSelector != nil {
			return true
		}
	}
	return false
}

// addNetworkPolicy creates and applies OVN ACLs to pod logical switch
// ports from Kubernetes NetworkPolicy objects using OVN Port Groups
func (oc *Controller) addNetworkPolicy(policy *knet.NetworkPolicy) {
	klog.Infof("Adding network policy %s in namespace %s", policy.Name,
		policy.Namespace)

	nsInfo, err := oc.waitForNamespaceLocked(policy.Namespace)
	if err != nil {
		klog.Errorf("failed to wait for namespace %s event (%v)",
			policy.Namespace, err)
		return
	}
	_, alreadyExists := nsInfo.networkPolicies[policy.Name]
	if alreadyExists {
		nsInfo.Unlock()
		return
	}

	np := NewNamespacePolicy(policy)
	nsInfo.networkPolicies[policy.Name] = np
	np.Lock()
	nsInfo.Unlock()

	// Create a port group for the policy. All the pods that this policy
	// selects will be eventually added to this port group.
	readableGroupName := fmt.Sprintf("%s_%s", policy.Namespace, policy.Name)
	np.portGroupName = hashedPortGroup(readableGroupName)

	np.portGroupUUID, err = createPortGroup(readableGroupName, np.portGroupName)
	if err != nil {
		klog.Errorf("Failed to create port_group for network policy %s in "+
			"namespace %s", policy.Name, policy.Namespace)
		return
	}

	type policyHandler struct {
		gress             *gressPolicy
		namespaceSelector *metav1.LabelSelector
		podSelector       *metav1.LabelSelector
		addrSet           string
		peerMap           map[string]bool
	}
	var policyHandlers []policyHandler
	// Go through each ingress rule.  For each ingress rule, create an
	// addressSet for the peer pods.
	for i, ingressJSON := range policy.Spec.Ingress {
		klog.V(5).Infof("Network policy ingress is %+v", ingressJSON)

		ingress := newGressPolicy(knet.PolicyTypeIngress, i, policy.Namespace, policy.Name)

		// Each ingress rule can have multiple ports to which we allow traffic.
		for _, portJSON := range ingressJSON.Ports {
			ingress.addPortPolicy(&portJSON)
		}

		hashedLocalAddressSet := ""
		// peerPodAddressMap represents the IP addresses of all the peer pods
		// for this ingress.
		peerPodAddressMap := make(map[string]bool)
		if hasAnyLabelSelector(ingressJSON.From) {
			// localPeerPods represents all the peer pods in the same
			// namespace from which we need to allow traffic.
			localPeerPods := fmt.Sprintf("%s.%s.%s.%d", policy.Namespace,
				policy.Name, "ingress", i)

			hashedLocalAddressSet = hashedAddressSet(localPeerPods)
			createAddressSet(localPeerPods, hashedLocalAddressSet, nil)
			ingress.addAddressSet(hashedLocalAddressSet)
		}

		for _, fromJSON := range ingressJSON.From {
			// Add IPBlock to ingress network policy
			if fromJSON.IPBlock != nil {
				ingress.addIPBlock(fromJSON.IPBlock)
			}
		}

		ingress.localPodAddACL(np.portGroupName, np.portGroupUUID)

		for _, fromJSON := range ingressJSON.From {
			ingressPolicyHandler := policyHandler{
				gress:             ingress,
				namespaceSelector: fromJSON.NamespaceSelector,
				podSelector:       fromJSON.PodSelector,
				addrSet:           hashedLocalAddressSet,
				peerMap:           peerPodAddressMap,
			}
			policyHandlers = append(policyHandlers, ingressPolicyHandler)
		}
		np.ingressPolicies = append(np.ingressPolicies, ingress)
	}

	// Go through each egress rule.  For each egress rule, create an
	// addressSet for the peer pods.
	for i, egressJSON := range policy.Spec.Egress {
		klog.V(5).Infof("Network policy egress is %+v", egressJSON)

		egress := newGressPolicy(knet.PolicyTypeEgress, i, policy.Namespace, policy.Name)

		// Each egress rule can have multiple ports to which we allow traffic.
		for _, portJSON := range egressJSON.Ports {
			egress.addPortPolicy(&portJSON)
		}

		hashedLocalAddressSet := ""
		// peerPodAddressMap represents the IP addresses of all the peer pods
		// for this egress.
		peerPodAddressMap := make(map[string]bool)
		if hasAnyLabelSelector(egressJSON.To) {
			// localPeerPods represents all the peer pods in the same
			// namespace to which we need to allow traffic.
			localPeerPods := fmt.Sprintf("%s.%s.%s.%d", policy.Namespace,
				policy.Name, "egress", i)

			hashedLocalAddressSet = hashedAddressSet(localPeerPods)
			createAddressSet(localPeerPods, hashedLocalAddressSet, nil)
			egress.addAddressSet(hashedLocalAddressSet)
		}

		for _, toJSON := range egressJSON.To {
			// Add IPBlock to egress network policy
			if toJSON.IPBlock != nil {
				egress.addIPBlock(toJSON.IPBlock)
			}
		}

		egress.localPodAddACL(np.portGroupName, np.portGroupUUID)

		for _, toJSON := range egressJSON.To {
			egressPolicyHandler := policyHandler{
				gress:             egress,
				namespaceSelector: toJSON.NamespaceSelector,
				podSelector:       toJSON.PodSelector,
				addrSet:           hashedLocalAddressSet,
				peerMap:           peerPodAddressMap,
			}
			policyHandlers = append(policyHandlers, egressPolicyHandler)
		}
		np.egressPolicies = append(np.egressPolicies, egress)
	}
	np.Unlock()
	// For all the pods in the local namespace that this policy
	// effects, add them to the port group.
	oc.handleLocalPodSelector(policy, np)

	for _, handler := range policyHandlers {
		if handler.namespaceSelector != nil && handler.podSelector != nil {
			// For each rule that contains both peer namespace selector and
			// peer pod selector, we create a watcher for each matching namespace
			// that populates the addressSet
			oc.handlePeerNamespaceAndPodSelector(policy, handler.gress,
				handler.namespaceSelector, handler.podSelector,
				handler.addrSet, handler.peerMap, np)
		} else if handler.namespaceSelector != nil {
			// For each peer namespace selector, we create a watcher that
			// populates ingress.peerAddressSets
			oc.handlePeerNamespaceSelector(policy,
				handler.namespaceSelector, handler.gress, np)
		} else if handler.podSelector != nil {
			// For each peer pod selector, we create a watcher that
			// populates the addressSet
			oc.handlePeerPodSelector(policy, handler.podSelector,
				handler.addrSet, handler.peerMap, np)
		}
	}
}

// deletes the namespacePolicy for policy and returns it, locked
func (oc *Controller) deleteNetworkPolicyLocked(policy *knet.NetworkPolicy) *namespacePolicy {
	nsInfo := oc.getNamespaceLocked(policy.Namespace)
	if nsInfo == nil {
		return nil
	}
	defer nsInfo.Unlock()

	np := nsInfo.networkPolicies[policy.Name]
	if np == nil {
		return nil
	}
	np.Lock()

	delete(nsInfo.networkPolicies, policy.Name)
	np.deleted = true
	return np
}

func (oc *Controller) deleteNetworkPolicy(policy *knet.NetworkPolicy) {
	klog.Infof("Deleting network policy %s in namespace %s",
		policy.Name, policy.Namespace)

	np := oc.deleteNetworkPolicyLocked(policy)
	if np == nil {
		return
	}
	defer np.Unlock()

	// We should now stop all the handlers go routines.
	oc.shutdownHandlers(np)

	for _, portInfo := range np.localPods {
		oc.localPodDelDefaultDeny(policy, portInfo)
	}

	// Delete the port group
	deletePortGroup(np.portGroupName)

	// Go through each ingress rule.  For each ingress rule, delete the
	// addressSet for the local peer pods.
	for i := range np.ingressPolicies {
		localPeerPods := fmt.Sprintf("%s.%s.%s.%d", policy.Namespace,
			policy.Name, "ingress", i)
		hashedAddressSet := hashedAddressSet(localPeerPods)
		deleteAddressSet(hashedAddressSet)
	}
	// Go through each egress rule.  For each egress rule, delete the
	// addressSet for the local peer pods.
	for i := range np.egressPolicies {
		localPeerPods := fmt.Sprintf("%s.%s.%s.%d", policy.Namespace,
			policy.Name, "egress", i)
		hashedAddressSet := hashedAddressSet(localPeerPods)
		deleteAddressSet(hashedAddressSet)
	}
}

// handlePeerPodSelectorAddUpdate adds the IP address of a pod that has been
// selected as a peer by a NetworkPolicy's ingress/egress section to that
// ingress/egress address set
func (oc *Controller) handlePeerPodSelectorAddUpdate(np *namespacePolicy,
	addressMap map[string]bool, addressSet string, obj interface{}) {

	pod := obj.(*kapi.Pod)
	podAnnotation, err := util.UnmarshalPodAnnotation(pod.Annotations)
	if err != nil {
		return
	}
	// DUAL-STACK FIXME: handle multiple IPs
	ipAddress := podAnnotation.IPs[0].IP.String()
	if addressMap[ipAddress] {
		return
	}

	np.Lock()
	defer np.Unlock()
	if np.deleted {
		return
	}

	addressMap[ipAddress] = true
	addToAddressSet(addressSet, ipAddress)
}

func (oc *Controller) handlePeerPodSelectorDeleteACLRules(obj interface{}, gress *gressPolicy) {
	pod := obj.(*kapi.Pod)
	logicalPort := podLogicalPortName(pod)

	oc.lspMutex.Lock()
	delete(oc.lspIngressDenyCache, logicalPort)
	delete(oc.lspEgressDenyCache, logicalPort)
	oc.lspMutex.Unlock()
}

// handlePeerPodSelectorDelete removes the IP address of a pod that no longer
// matches a NetworkPolicy ingress/egress section's selectors from that
// ingress/egress address set
func (oc *Controller) handlePeerPodSelectorDelete(np *namespacePolicy,
	addressMap map[string]bool, addressSet string, obj interface{}) {

	pod := obj.(*kapi.Pod)
	podAnnotation, err := util.UnmarshalPodAnnotation(pod.Annotations)
	if err != nil {
		return
	}
	// DUAL-STACK FIXME: handle multiple IPs
	ipAddress := podAnnotation.IPs[0].IP.String()

	np.Lock()
	defer np.Unlock()
	if np.deleted {
		return
	}

	if !addressMap[ipAddress] {
		return
	}

	delete(addressMap, ipAddress)
	removeFromAddressSet(addressSet, ipAddress)
}

func (oc *Controller) handlePeerPodSelector(
	policy *knet.NetworkPolicy, podSelector *metav1.LabelSelector,
	addressSet string, addressMap map[string]bool, np *namespacePolicy) {

	h, err := oc.watchFactory.AddFilteredPodHandler(policy.Namespace,
		podSelector,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				oc.handlePeerPodSelectorAddUpdate(np, addressMap, addressSet, obj)
			},
			DeleteFunc: func(obj interface{}) {
				oc.handlePeerPodSelectorDelete(np, addressMap, addressSet, obj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oc.handlePeerPodSelectorAddUpdate(np, addressMap, addressSet, newObj)
			},
		}, nil)
	if err != nil {
		klog.Errorf("error watching peer pods for policy %s in namespace %s: %v",
			policy.Name, policy.Namespace, err)
		return
	}

	np.podHandlerList = append(np.podHandlerList, h)
}

func (oc *Controller) handlePeerNamespaceAndPodSelector(policy *knet.NetworkPolicy, gress *gressPolicy, namespaceSelector *metav1.LabelSelector, podSelector *metav1.LabelSelector, addressSet string, addressMap map[string]bool, np *namespacePolicy) {
	namespaceHandler, err := oc.watchFactory.AddFilteredNamespaceHandler("",
		namespaceSelector,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				namespace := obj.(*kapi.Namespace)
				np.Lock()
				alreadyDeleted := np.deleted
				np.Unlock()
				if alreadyDeleted {
					return
				}

				// The AddFilteredPodHandler call might call handlePeerPodSelectorAddUpdate
				// on existing pods so we can't be holding the lock at this point
				podHandler, err := oc.watchFactory.AddFilteredPodHandler(namespace.Name,
					podSelector,
					cache.ResourceEventHandlerFuncs{
						AddFunc: func(obj interface{}) {
							oc.handlePeerPodSelectorAddUpdate(np, addressMap, addressSet, obj)
						},
						DeleteFunc: func(obj interface{}) {
							oc.handlePeerPodSelectorDelete(np, addressMap, addressSet, obj)
							oc.handlePeerPodSelectorDeleteACLRules(obj, gress)
						},
						UpdateFunc: func(oldObj, newObj interface{}) {
							oc.handlePeerPodSelectorAddUpdate(np, addressMap, addressSet, newObj)
						},
					}, nil)
				if err != nil {
					klog.Errorf("error watching pods in namespace %s for policy %s: %v", namespace.Name, policy.Name, err)
					return
				}
				np.Lock()
				defer np.Unlock()
				if np.deleted {
					_ = oc.watchFactory.RemovePodHandler(podHandler)
					return
				}
				np.podHandlerList = append(np.podHandlerList, podHandler)
			},
			DeleteFunc: func(obj interface{}) {
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
			},
		}, nil)
	if err != nil {
		klog.Errorf("error watching namespaces for policy %s: %v",
			policy.Name, err)
		return
	}
	np.nsHandlerList = append(np.nsHandlerList, namespaceHandler)
}

func (oc *Controller) handlePeerNamespaceSelector(
	policy *knet.NetworkPolicy,
	namespaceSelector *metav1.LabelSelector,
	gress *gressPolicy, np *namespacePolicy) {

	h, err := oc.watchFactory.AddFilteredNamespaceHandler("",
		namespaceSelector,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				namespace := obj.(*kapi.Namespace)
				np.Lock()
				defer np.Unlock()
				if np.deleted {
					return
				}
				hashedAddressSet := hashedAddressSet(namespace.Name)
				oldL3Match, newL3Match, added := gress.addAddressSet(hashedAddressSet)
				if added {
					gress.localPodUpdateACL(oldL3Match, newL3Match, np.portGroupName)
				}
			},
			DeleteFunc: func(obj interface{}) {
				namespace := obj.(*kapi.Namespace)
				np.Lock()
				defer np.Unlock()
				if np.deleted {
					return
				}
				hashedAddressSet := hashedAddressSet(namespace.Name)
				oldL3Match, newL3Match, removed := gress.delAddressSet(hashedAddressSet)
				if removed {
					gress.localPodUpdateACL(oldL3Match, newL3Match, np.portGroupName)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
			},
		}, nil)
	if err != nil {
		klog.Errorf("error watching namespaces for policy %s: %v",
			policy.Name, err)
		return
	}

	np.nsHandlerList = append(np.nsHandlerList, h)
}

func (oc *Controller) shutdownHandlers(np *namespacePolicy) {
	for _, handler := range np.podHandlerList {
		_ = oc.watchFactory.RemovePodHandler(handler)
	}
	for _, handler := range np.nsHandlerList {
		_ = oc.watchFactory.RemoveNamespaceHandler(handler)
	}
}
