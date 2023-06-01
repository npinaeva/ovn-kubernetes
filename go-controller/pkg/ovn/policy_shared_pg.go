package ovn

import (
	"fmt"
	"time"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

type netpolDefaultDenyACLType string

const (
	// netpolDefaultDenyACLType is used to distinguish default deny and arp allow acls create for the same port group
	defaultDenyACL netpolDefaultDenyACLType = "defaultDeny"
	arpAllowACL    netpolDefaultDenyACLType = "arpAllow"
)

// defaultDenyPortGroups is a shared object and should be used by only 1 thread at a time
type defaultDenyPortGroups struct {
	// portName: map[portName]sets.String(policyNames)
	// store policies that are using every port in the map
	// these maps should be atomically updated with db operations
	// if adding a port to db for a policy fails, map shouldn't be changed
	ingressPortToPolicies map[string]sets.Set[string]
	egressPortToPolicies  map[string]sets.Set[string]
	// policies is a map of policies that use this port group
	// policy keys must be unique, and it can be retrieved with (np *networkPolicy) getKey()
	policies map[string]bool
}

// addPortsForPolicy adds port-policy association for default deny port groups and
// returns lists of new ports to add to the default deny port groups.
// If port should be added to ingress and/or egress default deny port group depends on policy spec.
func (sharedPGs *defaultDenyPortGroups) addPortsForPolicy(np *networkPolicy,
	portNamesToUUIDs map[string]string) (ingressDenyPorts, egressDenyPorts []string) {
	ingressDenyPorts = []string{}
	egressDenyPorts = []string{}

	if np.isIngress {
		for portName, portUUID := range portNamesToUUIDs {
			// if this is the first NP referencing this pod, then we
			// need to add it to the port group.
			if sharedPGs.ingressPortToPolicies[portName].Len() == 0 {
				ingressDenyPorts = append(ingressDenyPorts, portUUID)
				sharedPGs.ingressPortToPolicies[portName] = sets.Set[string]{}
			}
			// increment the reference count.
			sharedPGs.ingressPortToPolicies[portName].Insert(np.getKey())
		}
	}
	if np.isEgress {
		for portName, portUUID := range portNamesToUUIDs {
			if sharedPGs.egressPortToPolicies[portName].Len() == 0 {
				// again, reference count is 0, so add to port
				egressDenyPorts = append(egressDenyPorts, portUUID)
				sharedPGs.egressPortToPolicies[portName] = sets.Set[string]{}
			}
			// bump reference count
			sharedPGs.egressPortToPolicies[portName].Insert(np.getKey())
		}
	}
	return
}

// deletePortsForPolicy deletes port-policy association for default deny port groups,
// and returns lists of port UUIDs to delete from the default deny port groups.
// If port should be deleted from ingress and/or egress default deny port group depends on policy spec.
func (sharedPGs *defaultDenyPortGroups) deletePortsForPolicy(np *networkPolicy,
	portNamesToUUIDs map[string]string) (ingressDenyPorts, egressDenyPorts []string) {
	ingressDenyPorts = []string{}
	egressDenyPorts = []string{}

	if np.isIngress {
		for portName, portUUID := range portNamesToUUIDs {
			// Delete and Len can be used for zero-value nil set
			sharedPGs.ingressPortToPolicies[portName].Delete(np.getKey())
			if sharedPGs.ingressPortToPolicies[portName].Len() == 0 {
				ingressDenyPorts = append(ingressDenyPorts, portUUID)
				delete(sharedPGs.ingressPortToPolicies, portName)
			}
		}
	}
	if np.isEgress {
		for portName, portUUID := range portNamesToUUIDs {
			sharedPGs.egressPortToPolicies[portName].Delete(np.getKey())
			if sharedPGs.egressPortToPolicies[portName].Len() == 0 {
				egressDenyPorts = append(egressDenyPorts, portUUID)
				delete(sharedPGs.egressPortToPolicies, portName)
			}
		}
	}
	return
}

type NetpolSharedPortGroupsController struct {
	// map of existing shared port groups for network policies
	// port group exists in the db if and only if port group key is present in this map
	// key is namespace
	sharedNetpolPortGroups *syncmap.SyncMap[*defaultDenyPortGroups]

	nbClient       libovsdbclient.Client
	controllerName string

	addConfigDurationRecord func(kind, namespace, name string) ([]ovsdb.Operation, func(), time.Time, error)
}

func NewNetpolSharedPortGroupController(nbClient libovsdbclient.Client, controllerName string,
	addConfigDurationRecord func(kind, namespace, name string) ([]ovsdb.Operation, func(), time.Time, error)) *NetpolSharedPortGroupsController {
	return &NetpolSharedPortGroupsController{
		sharedNetpolPortGroups:  syncmap.NewSyncMap[*defaultDenyPortGroups](),
		nbClient:                nbClient,
		controllerName:          controllerName,
		addConfigDurationRecord: addConfigDurationRecord,
	}
}

func (bnc *NetpolSharedPortGroupsController) getDefaultDenyPolicyACLIDs(ns string, aclDir aclDirection,
	defaultACLType netpolDefaultDenyACLType) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolNamespace, bnc.controllerName,
		map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey: ns,
			// in the same namespace there can be 2 default deny port groups, egress and ingress,
			// every port group has default deny and arp allow acl.
			libovsdbops.PolicyDirectionKey: string(aclDir),
			libovsdbops.TypeKey:            string(defaultACLType),
		})
}

func (bnc *NetpolSharedPortGroupsController) getDefaultDenyPolicyPortGroupIDs(ns string, aclDir aclDirection) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.PortGroupNetpolNamespace, bnc.controllerName,
		map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey: ns,
			// in the same namespace there can be 2 default deny port groups, egress and ingress,
			// every port group has default deny and arp allow acl.
			libovsdbops.PolicyDirectionKey: string(aclDir),
		})
}

func (bnc *NetpolSharedPortGroupsController) defaultDenyPortGroupName(namespace string, aclDir aclDirection) string {
	return libovsdbops.GetPortGroupName(bnc.getDefaultDenyPolicyPortGroupIDs(namespace, aclDir))
}

func (bnc *NetpolSharedPortGroupsController) buildDenyACLs(namespace, pgName string, aclLogging *ACLLoggingLevels,
	aclDir aclDirection) (denyACL, allowACL *nbdb.ACL) {
	denyMatch := getACLMatch(pgName, "", aclDir)
	allowMatch := getACLMatch(pgName, arpAllowPolicyMatch, aclDir)
	aclPipeline := aclDirectionToACLPipeline(aclDir)

	denyACL = BuildACL(bnc.getDefaultDenyPolicyACLIDs(namespace, aclDir, defaultDenyACL),
		types.DefaultDenyPriority, denyMatch, nbdb.ACLActionDrop, aclLogging, aclPipeline)
	allowACL = BuildACL(bnc.getDefaultDenyPolicyACLIDs(namespace, aclDir, arpAllowACL),
		types.DefaultAllowPriority, allowMatch, nbdb.ACLActionAllow, nil, aclPipeline)
	return
}

func (bnc *NetpolSharedPortGroupsController) AddPolicy(np *networkPolicy, aclLogging *ACLLoggingLevels) error {
	return bnc.sharedNetpolPortGroups.DoWithLock(np.namespace, func(pgKey string) error {
		sharedPGs, loaded := bnc.sharedNetpolPortGroups.LoadOrStore(pgKey, &defaultDenyPortGroups{
			ingressPortToPolicies: map[string]sets.Set[string]{},
			egressPortToPolicies:  map[string]sets.Set[string]{},
			policies:              map[string]bool{},
		})
		if !loaded {
			// create port groups with acls
			err := bnc.createDefaultDenyPGAndACLs(np.namespace, np.name, aclLogging)
			if err != nil {
				bnc.sharedNetpolPortGroups.Delete(pgKey)
				return fmt.Errorf("failed to create default deny port groups: %v", err)
			}
		}
		sharedPGs.policies[np.getKey()] = true
		return nil
	})
}

func (bnc *NetpolSharedPortGroupsController) DelPolicy(np *networkPolicy) error {
	return bnc.sharedNetpolPortGroups.DoWithLock(np.namespace, func(pgKey string) error {
		sharedPGs, found := bnc.sharedNetpolPortGroups.Load(pgKey)
		if !found {
			return nil
		}
		delete(sharedPGs.policies, np.getKey())
		if len(sharedPGs.policies) == 0 {
			// last policy was deleted, delete port group
			err := bnc.deleteDefaultDenyPGAndACLs(np.namespace)
			if err != nil {
				return fmt.Errorf("failed to delete defaul deny port group: %v", err)
			}
			bnc.sharedNetpolPortGroups.Delete(pgKey)
		}
		return nil
	})
}

// createDefaultDenyPGAndACLs creates the default port groups and acls for a namespace
// must be called with defaultDenyPortGroups lock
func (bnc *NetpolSharedPortGroupsController) createDefaultDenyPGAndACLs(namespace, policy string, aclLogging *ACLLoggingLevels) error {
	ingressPGIDs := bnc.getDefaultDenyPolicyPortGroupIDs(namespace, aclIngress)
	ingressPGName := libovsdbops.GetPortGroupName(ingressPGIDs)
	ingressDenyACL, ingressAllowACL := bnc.buildDenyACLs(namespace, ingressPGName, aclLogging, aclIngress)
	egressPGIDs := bnc.getDefaultDenyPolicyPortGroupIDs(namespace, aclEgress)
	egressPGName := libovsdbops.GetPortGroupName(egressPGIDs)
	egressDenyACL, egressAllowACL := bnc.buildDenyACLs(namespace, egressPGName, aclLogging, aclEgress)
	ops, err := libovsdbops.CreateOrUpdateACLsOps(bnc.nbClient, nil, ingressDenyACL, ingressAllowACL, egressDenyACL, egressAllowACL)
	if err != nil {
		return err
	}

	ingressPG := libovsdbops.BuildPortGroup(ingressPGIDs, nil, []*nbdb.ACL{ingressDenyACL, ingressAllowACL})
	egressPG := libovsdbops.BuildPortGroup(egressPGIDs, nil, []*nbdb.ACL{egressDenyACL, egressAllowACL})
	ops, err = libovsdbops.CreateOrUpdatePortGroupsOps(bnc.nbClient, ops, ingressPG, egressPG)
	if err != nil {
		return err
	}

	recordOps, txOkCallBack, _, err := bnc.addConfigDurationRecord("networkpolicy", namespace, policy)
	if err != nil {
		klog.Errorf("Failed to record config duration: %v", err)
	}
	ops = append(ops, recordOps...)
	_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
	if err != nil {
		return err
	}
	txOkCallBack()

	return nil
}

// deleteDefaultDenyPGAndACLs deletes the default port groups and acls for a namespace
// must be called with defaultDenyPortGroups lock
func (bnc *NetpolSharedPortGroupsController) deleteDefaultDenyPGAndACLs(namespace string) error {
	ingressPGName := bnc.defaultDenyPortGroupName(namespace, aclIngress)
	egressPGName := bnc.defaultDenyPortGroupName(namespace, aclEgress)

	ops, err := libovsdbops.DeletePortGroupsOps(bnc.nbClient, nil, ingressPGName, egressPGName)
	if err != nil {
		return err
	}
	// No need to delete ACLs, since they will be garbage collected with deleted port groups
	_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("failed to transact deleteDefaultDenyPGAndACLs: %v", err)
	}

	return nil
}

func (bnc *NetpolSharedPortGroupsController) UpdateACLLogging(ns string, aclLogging *ACLLoggingLevels) error {
	return bnc.sharedNetpolPortGroups.DoWithLock(ns, func(pgKey string) error {
		_, loaded := bnc.sharedNetpolPortGroups.Load(pgKey)
		if !loaded {
			// shared port group doesn't exist, nothing to update
			return nil
		}
		predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolNamespace, bnc.controllerName,
			map[libovsdbops.ExternalIDKey]string{
				libovsdbops.ObjectNameKey: ns,
				libovsdbops.TypeKey:       string(defaultDenyACL),
			})
		p := libovsdbops.GetPredicate[*nbdb.ACL](predicateIDs, nil)
		defaultDenyACLs, err := libovsdbops.FindACLsWithPredicate(bnc.nbClient, p)
		if err != nil {
			return fmt.Errorf("failed to find netpol default deny acls for namespace %s: %v", ns, err)
		}
		if err := UpdateACLLogging(bnc.nbClient, defaultDenyACLs, aclLogging); err != nil {
			return fmt.Errorf("unable to update ACL logging for namespace %s: %w", ns, err)
		}
		return nil
	})
}

// AddPorts adds ports to default deny port groups.
// It also can take existing ops e.g. to add port to network policy port group and transact it.
// It only adds new ports that do not already exist in the deny port groups.
func (bnc *NetpolSharedPortGroupsController) AddPorts(np *networkPolicy, portNamesToUUIDs map[string]string, ops []ovsdb.Operation) error {
	var err error
	ingressDenyPGName := bnc.defaultDenyPortGroupName(np.namespace, aclIngress)
	egressDenyPGName := bnc.defaultDenyPortGroupName(np.namespace, aclEgress)

	pgKey := np.namespace
	// this lock guarantees that sharedPortGroup counters will be updated atomically
	// with adding port to port group in db.
	bnc.sharedNetpolPortGroups.LockKey(pgKey)
	pgLocked := true
	defer func() {
		if pgLocked {
			bnc.sharedNetpolPortGroups.UnlockKey(pgKey)
		}
	}()
	sharedPGs, ok := bnc.sharedNetpolPortGroups.Load(pgKey)
	if !ok {
		// Port group doesn't exist
		return fmt.Errorf("port groups for ns %s don't exist", np.namespace)
	}

	ingressDenyPorts, egressDenyPorts := sharedPGs.addPortsForPolicy(np, portNamesToUUIDs)
	// counters were updated, update back to initial values on error
	defer func() {
		if err != nil {
			sharedPGs.deletePortsForPolicy(np, portNamesToUUIDs)
		}
	}()

	if len(ingressDenyPorts) != 0 || len(egressDenyPorts) != 0 {
		// db changes required
		ops, err = libovsdbops.AddPortsToPortGroupOps(bnc.nbClient, ops, ingressDenyPGName, ingressDenyPorts...)
		if err != nil {
			return fmt.Errorf("unable to get add ports to %s port group ops: %v", ingressDenyPGName, err)
		}

		ops, err = libovsdbops.AddPortsToPortGroupOps(bnc.nbClient, ops, egressDenyPGName, egressDenyPorts...)
		if err != nil {
			return fmt.Errorf("unable to get add ports to %s port group ops: %v", egressDenyPGName, err)
		}
	} else {
		// shared pg was updated and doesn't require db changes, no need to hold the lock
		bnc.sharedNetpolPortGroups.UnlockKey(pgKey)
		pgLocked = false
	}
	_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("unable to transact add ports to default deny port groups: %v", err)
	}
	return nil
}

// DeletePorts deletes ports from default deny port groups.
// Set useLocalPods = true, when deleting networkPolicy to remove all its ports from defaultDeny port groups.
// It also can take existing ops e.g. to delete ports from network policy port group and transact it.
func (bnc *NetpolSharedPortGroupsController) DeletePorts(np *networkPolicy, portNamesToUUIDs map[string]string, useLocalPods bool,
	ops []ovsdb.Operation) error {
	var err error
	if useLocalPods {
		portNamesToUUIDs = map[string]string{}
		np.localPods.Range(func(key, value interface{}) bool {
			portNamesToUUIDs[key.(string)] = value.(string)
			return true
		})
	}
	if len(portNamesToUUIDs) != 0 {
		ingressDenyPGName := bnc.defaultDenyPortGroupName(np.namespace, aclIngress)
		egressDenyPGName := bnc.defaultDenyPortGroupName(np.namespace, aclEgress)

		pgKey := np.namespace
		// this lock guarantees that sharedPortGroup counters will be updated atomically
		// with adding port to port group in db.
		bnc.sharedNetpolPortGroups.LockKey(pgKey)
		pgLocked := true
		defer func() {
			if pgLocked {
				bnc.sharedNetpolPortGroups.UnlockKey(pgKey)
			}
		}()
		sharedPGs, ok := bnc.sharedNetpolPortGroups.Load(pgKey)
		if !ok {
			// Port group doesn't exist, nothing to clean up
			klog.Infof("Skip delete ports from default deny port group: port group doesn't exist")
		} else {
			ingressDenyPorts, egressDenyPorts := sharedPGs.deletePortsForPolicy(np, portNamesToUUIDs)
			// counters were updated, update back to initial values on error
			defer func() {
				if err != nil {
					sharedPGs.addPortsForPolicy(np, portNamesToUUIDs)
				}
			}()

			if len(ingressDenyPorts) != 0 || len(egressDenyPorts) != 0 {
				// db changes required
				ops, err = libovsdbops.DeletePortsFromPortGroupOps(bnc.nbClient, ops, ingressDenyPGName, ingressDenyPorts...)
				if err != nil {
					return fmt.Errorf("unable to get del ports from %s port group ops: %v", ingressDenyPGName, err)
				}

				ops, err = libovsdbops.DeletePortsFromPortGroupOps(bnc.nbClient, ops, egressDenyPGName, egressDenyPorts...)
				if err != nil {
					return fmt.Errorf("unable to get del ports from %s port group ops: %v", egressDenyPGName, err)
				}
			} else {
				// shared pg was updated and doesn't require db changes, no need to hold the lock
				bnc.sharedNetpolPortGroups.UnlockKey(pgKey)
				pgLocked = false
			}
		}
	}
	_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("unable to transact del ports from default deny port groups: %v", err)
	}

	return nil
}
