package ovn

import (
	"fmt"
	"time"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/selector_based_controllers"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"k8s.io/klog/v2"
)

type netpolDefaultDenyACLType string

const (
	// netpolDefaultDenyACLType is used to distinguish default deny and arp allow acls create for the same port group
	defaultDenyACL netpolDefaultDenyACLType = "defaultDeny"
	arpAllowACL    netpolDefaultDenyACLType = "arpAllow"
)

// sharedPortGroup is a shared object and should be used by only 1 thread at a time
type sharedPortGroup struct {
	ingressPolicies map[string]bool
	egressPolicies  map[string]bool
	pgName          string
	pgKey           string
}

type NetpolSharedPortGroupsController struct {
	// map of existing shared port groups for network policies
	// port group exists in the db if and only if port group key is present in this map
	// key is namespace
	sharedNetpolPortGroups *syncmap.SyncMap[*sharedPortGroup]

	nbClient       libovsdbclient.Client
	controllerName string

	addConfigDurationRecord func(kind, namespace, name string) ([]ovsdb.Operation, func(), time.Time, error)
	podPortGroupController  *selector_based_controllers.PodPortGroupController
}

func NewNetpolSharedPortGroupController(nbClient libovsdbclient.Client, controllerName string,
	addConfigDurationRecord func(kind, namespace, name string) ([]ovsdb.Operation, func(), time.Time, error),
	podPortGroupController *selector_based_controllers.PodPortGroupController) *NetpolSharedPortGroupsController {
	return &NetpolSharedPortGroupsController{
		sharedNetpolPortGroups:  syncmap.NewSyncMap[*sharedPortGroup](),
		nbClient:                nbClient,
		controllerName:          controllerName,
		addConfigDurationRecord: addConfigDurationRecord,
		podPortGroupController:  podPortGroupController,
	}
}

func (c *NetpolSharedPortGroupsController) AddPolicyACLs(np *networkPolicy, aclLogging *ACLLoggingLevels) error {
	pgKey := selector_based_controllers.GetPodSelectorKey(&np.podSelector, nil, np.namespace)
	return c.sharedNetpolPortGroups.DoWithLock(pgKey, func(pgKey string) error {
		sharedPG, loaded := c.sharedNetpolPortGroups.LoadOrStore(pgKey, &sharedPortGroup{
			ingressPolicies: map[string]bool{},
			egressPolicies:  map[string]bool{},
		})
		if !loaded {
			// create port group
			pgKey, pgName, err := c.podPortGroupController.EnsureSharedPortGroup(
				&np.podSelector, nil, np.namespace, netpolControllerBackRef)
			if err != nil {
				return fmt.Errorf("failed to create port group by controller: %w", err)
			}
			sharedPG.pgName = pgName
			sharedPG.pgKey = pgKey
		}
		np.portGroupKey = sharedPG.pgKey
		np.portGroupName = sharedPG.pgName

		// update counters
		newACLs := []*nbdb.ACL{}
		if np.isIngress {
			sharedPG.ingressPolicies[np.getKey()] = true
			if len(sharedPG.ingressPolicies) == 1 {
				// add ingress default deny acl
				ingressDenyACL, ingressAllowACL := c.buildDenyACLs(sharedPG.pgName, aclLogging, aclIngress)
				newACLs = append(newACLs, ingressDenyACL, ingressAllowACL)
			}
		}
		if np.isEgress {
			sharedPG.egressPolicies[np.getKey()] = true
			if len(sharedPG.egressPolicies) == 1 {
				// add egress deafult deny acl
				egressDenyACL, egressAllowACL := c.buildDenyACLs(sharedPG.pgName, aclLogging, aclEgress)
				newACLs = append(newACLs, egressDenyACL, egressAllowACL)
			}
		}

		// build policy acls
		acls := buildNetworkPolicyACLs(np, aclLogging)
		newACLs = append(newACLs, acls...)

		// we only consider the case where port group is same between updates
		existingPred := libovsdbops.GetPredicate[*nbdb.ACL](libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetworkPolicy, c.controllerName,
			map[libovsdbops.ExternalIDKey]string{
				libovsdbops.ObjectNameKey: getACLPolicyKey(np.namespace, np.name),
			}), nil)
		existingACLs, err := libovsdbops.FindACLsWithPredicate(c.nbClient, existingPred)
		if err != nil {
			return fmt.Errorf("")
		}

		ops, err := libovsdbops.CreateOrUpdateACLsOps(c.nbClient, nil, newACLs...)
		if err != nil {
			return fmt.Errorf("failed to get create or update ACL ops: %w", err)
		}

		ops, err = libovsdbops.AddACLsToPortGroupOps(c.nbClient, ops, sharedPG.pgName, newACLs...)
		if err != nil {
			return fmt.Errorf("failed to get add ACL to port group ops: %w", err)
		}

		if len(existingACLs) > 0 {
			// we assume all existing acls should belong to the same port group
			// 1. Find existing port group acls are attached to
			// 2. if pg is the same, find acls diff, delete stale acls
			// 3. if pg is different, dereference all stale acls

			// Most likely the pod selector for netpol didn't change, check if current port group has the acl
			pg, err := libovsdbops.GetPortGroup(c.nbClient, &nbdb.PortGroup{
				Name: sharedPG.pgName,
			})
			if err != nil {
				return fmt.Errorf("")
			}
			foundACL := false
			// Use the first acl from existing to check
			for _, aclUUID := range pg.ACLs {
				if existingACLs[0].UUID == aclUUID {
					foundACL = true
					break
				}
			}


			pgPred := libovsdbops.GetPredicate[*nbdb.PortGroup](
				libovsdbops.NewDbObjectIDs(libovsdbops.PortGroupPodSelector, c.controllerName, nil), func(item *nbdb.PortGroup) bool {
					if
				})
			existingPortGroup := libovsdbops.FindPortGroupsWithPredicate(c.nbClient, )
			// CreateOrUpdateACLsOps will assign UUIDs to all existing acls
			staleACLs := []*nbdb.ACL{}
			for _, existingACL := range existingACLs {

			}

		}

		ops, err = libovsdbops.DeleteACLsFromPortGroupOps(c.nbClient, ops, sharedPG.pgName, newACLs...)
		if err != nil {
			return fmt.Errorf("failed to get delete stale ACLs from port group ops: %w", err)
		}

		recordOps, txOkCallBack, _, err := c.addConfigDurationRecord("networkpolicy", np.namespace, np.name)
		if err != nil {
			klog.Errorf("Failed to record config duration: %v", err)
		}
		ops = append(ops, recordOps...)

		_, err = libovsdbops.TransactAndCheck(c.nbClient, ops)
		if err != nil {
			return fmt.Errorf("failed to run ovsdb txn to add ports to port group: %v", err)
		}
		txOkCallBack()

		return nil
	})
}

func (c *NetpolSharedPortGroupsController) updateExistingACLs(existingACLs, newACLs []*nbdb.ACL, newPGName string) error {
	// we assume all existing acls should belong to the same port group
	// 1. Find existing port group acls are attached to
	// 2. if pg is the same, find acls diff, delete stale acls
	// 3. if pg is different, dereference all stale acls

	// Most likely the pod selector for netpol didn't change, check if current port group has the acl
	pg, err := libovsdbops.GetPortGroup(c.nbClient, &nbdb.PortGroup{
		Name: newPGName,
	})
	if err != nil {
		return fmt.Errorf("")
	}
	existingPGName := ""
	// Use the first acl from existing to check
	for _, aclUUID := range pg.ACLs {
		if existingACLs[0].UUID == aclUUID {
			existingPGName = newPGName
			break
		}
	}
	if existingPGName == "" {
		// port group has changed, find the old one
		pgPred := libovsdbops.GetPredicate[*nbdb.PortGroup](
			libovsdbops.NewDbObjectIDs(libovsdbops.PortGroupPodSelector, c.controllerName, nil), func(item *nbdb.PortGroup) bool {
				for _, aclUUID := range item.ACLs {
					if existingACLs[0].UUID == aclUUID {
						existingPGName = item.Name
						break
					}
				}
				return false
			})
		_, err = libovsdbops.FindPortGroupsWithPredicate(c.nbClient, pgPred)
		if err != nil {
			return fmt.Errorf("")
		}
		if existingPGName == "" {
			// should never happen
			return fmt.Errorf("")
		}
	}

	if existingPGName == newPGName {
		// find ACLs that need to be deleted
		// newACLs must have UUIDs assigned
		staleACLs := []*nbdb.ACL{}
		for _, existingACL := range existingACLs {

		}
	} else {
		// dereference all existing acls from the previous port group
	}


}

func (c *NetpolSharedPortGroupsController) DelPolicyACLs(np *networkPolicy) error {
	pgKey := selector_based_controllers.GetPodSelectorKey(&np.podSelector, nil, np.namespace)
	return c.sharedNetpolPortGroups.DoWithLock(pgKey, func(pgKey string) error {
		sharedPG, found := c.sharedNetpolPortGroups.Load(pgKey)
		if !found {
			return nil
		}
		aclsToDelete := []*nbdb.ACL{}

		if np.isIngress {
			delete(sharedPG.ingressPolicies, np.getKey())
			if len(sharedPG.ingressPolicies) == 0 {
				// delete ingress default deny acl
				ingressDenyACL, ingressAllowACL := c.getDenyACLExternalIDs(sharedPG.pgName, aclIngress)
				dbACLs, err := libovsdbops.FindACLs(c.nbClient, []*nbdb.ACL{ingressDenyACL, ingressAllowACL})
				if err != nil {
					return fmt.Errorf("")
				}
				aclsToDelete = append(aclsToDelete, dbACLs...)
			}
		}
		if np.isEgress {
			delete(sharedPG.egressPolicies, np.getKey())
			if len(sharedPG.egressPolicies) == 0 {
				// delete egress deafult deny acl
				egressDenyACL, egressAllowACL := c.getDenyACLExternalIDs(sharedPG.pgName, aclEgress)
				dbACLs, err := libovsdbops.FindACLs(c.nbClient, []*nbdb.ACL{egressDenyACL, egressAllowACL})
				if err != nil {
					return fmt.Errorf("")
				}
				aclsToDelete = append(aclsToDelete, dbACLs...)
			}
		}
		predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetworkPolicy, c.controllerName,
			map[libovsdbops.ExternalIDKey]string{
				libovsdbops.ObjectNameKey: getACLPolicyKey(np.namespace, np.name),
			})
		p := libovsdbops.GetPredicate[*nbdb.ACL](predicateIDs, nil)
		dbACLs, err := libovsdbops.FindACLsWithPredicate(c.nbClient, p)
		if err != nil {
			return fmt.Errorf("")
		}
		aclsToDelete = append(aclsToDelete, dbACLs...)

		err = libovsdbops.DeleteACLsFromPortGroups(c.nbClient, []string{sharedPG.pgName}, aclsToDelete...)
		if err != nil {
			return fmt.Errorf("")
		}

		if len(sharedPG.ingressPolicies) == 0 && len(sharedPG.egressPolicies) == 0 {
			// last policy was deleted, delete port group
			err = c.podPortGroupController.DeleteSharedPortGroup(sharedPG.pgKey, netpolControllerBackRef)
			if err != nil {
				return fmt.Errorf("")
			}

			c.sharedNetpolPortGroups.Delete(pgKey)
		}
		return nil
	})
}

func (c *NetpolSharedPortGroupsController) getDenyACLExternalIDs(pgName string, aclDir aclDirection) (denyACLID, allowACLID *nbdb.ACL) {
	denyACL := &nbdb.ACL{}
	denyACL.ExternalIDs = c.getDefaultDenyPolicyACLIDs(pgName, aclDir, defaultDenyACL).GetExternalIDs()
	allowACL := &nbdb.ACL{}
	allowACL.ExternalIDs = c.getDefaultDenyPolicyACLIDs(pgName, aclDir, arpAllowACL).GetExternalIDs()
	return denyACL, allowACL
}

func (c *NetpolSharedPortGroupsController) getDefaultDenyPolicyACLIDs(pgKey string, aclDir aclDirection,
	defaultACLType netpolDefaultDenyACLType) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolSharedPG, c.controllerName,
		map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey: pgKey,
			// in the same namespace there can be 2 default deny port groups, egress and ingress,
			// every port group has default deny and arp allow acl.
			libovsdbops.PolicyDirectionKey: string(aclDir),
			libovsdbops.TypeKey:            string(defaultACLType),
		})
}

func (c *NetpolSharedPortGroupsController) buildDenyACLs(pgName string, aclLogging *ACLLoggingLevels,
	aclDir aclDirection) (denyACL, allowACL *nbdb.ACL) {
	denyMatch := getACLMatch(pgName, "", aclDir)
	allowMatch := getACLMatch(pgName, arpAllowPolicyMatch, aclDir)
	aclPipeline := aclDirectionToACLPipeline(aclDir)

	denyACL = BuildACL(c.getDefaultDenyPolicyACLIDs(pgName, aclDir, defaultDenyACL),
		types.DefaultDenyPriority, denyMatch, nbdb.ACLActionDrop, aclLogging, aclPipeline)
	allowACL = BuildACL(c.getDefaultDenyPolicyACLIDs(pgName, aclDir, arpAllowACL),
		types.DefaultAllowPriority, allowMatch, nbdb.ACLActionAllow, nil, aclPipeline)
	return
}

func (c *NetpolSharedPortGroupsController) UpdateACLLogging(pgKey string, aclLogging *ACLLoggingLevels) error {
	return c.sharedNetpolPortGroups.DoWithLock(pgKey, func(pgKey string) error {
		sharedPG, loaded := c.sharedNetpolPortGroups.Load(pgKey)
		if !loaded {
			// shared port group doesn't exist, nothing to update
			return nil
		}
		pg, err := libovsdbops.GetPortGroup(c.nbClient, &nbdb.PortGroup{
			Name: sharedPG.pgName,
		})
		if err != nil {
			return fmt.Errorf("")
		}

		acls := make([]*nbdb.ACL, 0, len(pg.ACLs))
		for _, aclUUID := range pg.ACLs {
			acls = append(acls, &nbdb.ACL{UUID: aclUUID})
		}
		defaultDenyACLs, err := libovsdbops.FindACLs(c.nbClient, acls)
		if err != nil {
			return fmt.Errorf("failed to find netpol default deny acls for namespace %s: %v", pgKey, err)
		}
		if err := UpdateACLLogging(c.nbClient, defaultDenyACLs, aclLogging); err != nil {
			return fmt.Errorf("unable to update ACL logging for namespace %s: %w", pgKey, err)
		}
		return nil
	})
}
