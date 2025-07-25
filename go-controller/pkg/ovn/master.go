package ovn

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	libovsdbclient "github.com/ovn-kubernetes/libovsdb/client"

	hotypes "github.com/ovn-org/ovn-kubernetes/go-controller/hybrid-overlay/pkg/types"
	houtil "github.com/ovn-org/ovn-kubernetes/go-controller/hybrid-overlay/pkg/util"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	libovsdbops "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdb/ops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/sbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	utilerrors "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util/errors"
)

const (
	// IdledServiceAnnotationSuffix is a constant string representing the suffix of the Service annotation key
	// whose value indicates the time stamp in RFC3339 format when a Service was idled
	IdledServiceAnnotationSuffix   = "idled-at"
	OvnNodeAnnotationRetryInterval = 100 * time.Millisecond
	OvnNodeAnnotationRetryTimeout  = 1 * time.Second
)

type GatewayConfig struct {
	annoConfig                 *util.L3GatewayConfig
	hostSubnets                []*net.IPNet
	clusterSubnets             []*net.IPNet
	gwLRPJoinIPs               []*net.IPNet
	hostAddrs                  []string
	externalIPs                []net.IP
	ovnClusterLRPToJoinIfAddrs []*net.IPNet
}

// SetupMaster creates the central router and load-balancers for the network
func (oc *DefaultNetworkController) SetupMaster() error {
	// Create default Control Plane Protection (COPP) entry for routers
	defaultCOPPUUID, err := EnsureDefaultCOPP(oc.nbClient)
	if err != nil {
		return fmt.Errorf("unable to create router control plane protection: %w", err)
	}
	oc.defaultCOPPUUID = defaultCOPPUUID

	logicalRouter, err := oc.newClusterRouter()
	if err != nil {
		return err
	}

	if err := oc.setupClusterPortGroups(); err != nil {
		return err
	}

	if err := oc.syncDefaultMulticastPolicies(); err != nil {
		return err
	}

	// Create OVNJoinSwitch that will be used to connect gateway routers to the distributed router.
	return oc.gatewayTopologyFactory.NewJoinSwitch(logicalRouter, oc.GetNetInfo(), oc.ovnClusterLRPToJoinIfAddrs)
}

func (oc *DefaultNetworkController) newClusterRouter() (*nbdb.LogicalRouter, error) {
	if oc.multicastSupport {
		return oc.gatewayTopologyFactory.NewClusterRouterWithMulticastSupport(
			oc.GetNetworkScopedClusterRouterName(),
			oc.GetNetInfo(),
			oc.defaultCOPPUUID,
		)
	}
	return oc.gatewayTopologyFactory.NewClusterRouter(
		oc.GetNetworkScopedClusterRouterName(),
		oc.GetNetInfo(),
		oc.defaultCOPPUUID,
	)
}

func (oc *DefaultNetworkController) syncNodeManagementPortDefault(node *corev1.Node, switchName string, hostSubnets []*net.IPNet) error {
	mgmtPortIPs, err := oc.syncNodeManagementPort(node, switchName, oc.GetNetworkScopedClusterRouterName(), hostSubnets)
	if err == nil {
		return oc.setupUDNACLs(mgmtPortIPs)
	}
	return err
}

func (oc *DefaultNetworkController) nodeGatewayConfig(node *corev1.Node) (*GatewayConfig, error) {
	l3GatewayConfig, err := util.ParseNodeL3GatewayAnnotation(node)
	if err != nil {
		return nil, err
	}

	externalIPs := make([]net.IP, len(l3GatewayConfig.IPAddresses))
	for i, ip := range l3GatewayConfig.IPAddresses {
		externalIPs[i] = ip.IP
	}

	var hostAddrs []string
	if config.Gateway.Mode == config.GatewayModeShared {
		hostAddrs, err = util.GetNodeHostAddrs(node)
		if err != nil && !util.IsAnnotationNotSetError(err) {
			return nil, fmt.Errorf("failed to get host CIDRs for node: %s: %v", node.Name, err)
		}
	}

	var clusterSubnets []*net.IPNet
	for _, clusterSubnet := range config.Default.ClusterSubnets {
		clusterSubnets = append(clusterSubnets, clusterSubnet.CIDR)
	}

	hostSubnets, err := util.ParseNodeHostSubnetAnnotation(node, oc.GetNetworkName())
	if err != nil {
		return nil, err
	}

	gwLRPIPs, err := util.ParseNodeGatewayRouterJoinAddrs(node, oc.GetNetworkName())
	if err != nil {
		if util.IsAnnotationNotSetError(err) {
			// FIXME(tssurya): This is present for backwards compatibility
			// Remove me a few months from now
			var err1 error
			gwLRPIPs, err1 = util.ParseNodeGatewayRouterLRPAddrs(node)
			if err1 != nil {
				return nil, fmt.Errorf("failed to get join switch port IP address for node %s: %v/%v", node.Name, err, err1)
			}
		}
	}

	return &GatewayConfig{
		annoConfig:                 l3GatewayConfig,
		hostSubnets:                hostSubnets,
		clusterSubnets:             clusterSubnets,
		gwLRPJoinIPs:               gwLRPIPs,
		hostAddrs:                  hostAddrs,
		externalIPs:                externalIPs,
		ovnClusterLRPToJoinIfAddrs: oc.ovnClusterLRPToJoinIfAddrs,
	}, nil
}

func (oc *DefaultNetworkController) addNode(node *corev1.Node) ([]*net.IPNet, error) {
	// Node subnet for the default network is allocated by cluster manager.
	// Make sure that the node is allocated with the subnet before proceeding
	// to create OVN Northbound resources.
	hostSubnets, err := util.ParseNodeHostSubnetAnnotation(node, oc.GetNetworkName())
	if err != nil {
		return nil, err
	}

	// We expect one subnet per configured ClusterNetwork IP family.
	var haveV4, haveV6 bool
	for _, net := range hostSubnets {
		if !haveV4 {
			haveV4 = net.IP.To4() != nil
		}
		if !haveV6 {
			haveV6 = net.IP.To4() == nil
		}
	}
	if haveV4 != config.IPv4Mode || haveV6 != config.IPv6Mode {
		return nil, fmt.Errorf("failed to get expected host subnets for node %s; expected v4 %v have %v, expected v6 %v have %v",
			node.Name, config.IPv4Mode, haveV4, config.IPv6Mode, haveV6)
	}

	// delete stale chassis in SBDB if any
	if err = oc.deleteStaleNodeChassis(node); err != nil {
		return nil, err
	}

	// Ensure that the node's logical network has been created. Note that if the
	// subsequent operation in addNode() fails, oc.lsManager.DeleteNode(node.Name)
	// needs to be done, otherwise, this node's IPAM will be overwritten and the
	// same IP could be allocated to multiple Pods scheduled on this node.
	err = oc.createNodeLogicalSwitch(node.Name, hostSubnets, oc.clusterLoadBalancerGroupUUID, oc.switchLoadBalancerGroupUUID)
	if err != nil {
		return nil, err
	}

	return hostSubnets, nil
}

// check if any existing chassis entries in the SBDB mismatches with node's chassisID annotation
func (oc *DefaultNetworkController) checkNodeChassisMismatch(node *corev1.Node) (string, error) {
	chassisID, err := util.ParseNodeChassisIDAnnotation(node)
	if err != nil {
		return "", nil
	}

	chassisList, err := libovsdbops.ListChassis(oc.sbClient)
	if err != nil {
		return "", fmt.Errorf("failed to get chassis list for node %s: error: %v", node.Name, err)
	}

	for _, chassis := range chassisList {
		if chassis.Hostname == node.Name && chassis.Name != chassisID {
			return chassis.Name, nil
		}
	}
	return "", nil
}

// delete stale chassis in SBDB if system-id of the specific node has changed.
func (oc *DefaultNetworkController) deleteStaleNodeChassis(node *corev1.Node) error {
	staleChassis, err := oc.checkNodeChassisMismatch(node)
	if err != nil {
		return fmt.Errorf("failed to check if there is any stale chassis for node %s in SBDB: %v", node.Name, err)
	} else if staleChassis != "" {
		klog.V(5).Infof("Node %s now has a new chassis ID, delete its stale chassis %s in SBDB", node.Name, staleChassis)
		p := func(item *sbdb.Chassis) bool {
			return item.Name == staleChassis
		}
		if err = libovsdbops.DeleteChassisTemplateVar(oc.nbClient, &nbdb.ChassisTemplateVar{Chassis: staleChassis}); err != nil {
			// Send an event and Log on failure
			oc.recorder.Eventf(node, corev1.EventTypeWarning, "ErrorMismatchChassis",
				"Node %s is now with a new chassis ID. Its stale chassis template vars are still in the NBDB",
				node.Name)
			return fmt.Errorf("node %s is now with a new chassis ID. Its stale chassis template vars are still in the NBDB", node.Name)
		}
		if err = libovsdbops.DeleteChassisWithPredicate(oc.sbClient, p); err != nil {
			if err == libovsdbclient.ErrNotFound {
				klog.Infof("deleteStaleNodeChassis: chassis %s not found", node.Name)
				return nil
			}
			// Send an event and Log on failure
			oc.recorder.Eventf(node, corev1.EventTypeWarning, "ErrorMismatchChassis",
				"Node %s is now with a new chassis ID. Its stale chassis entry is still in the SBDB",
				node.Name)
			return fmt.Errorf("node %s is now with a new chassis ID. Its stale chassis entry is still in the SBDB", node.Name)
		}
	}
	return nil
}

// cleanupNodeResources deletes the node resources from the OVN Northbound database
func (oc *DefaultNetworkController) cleanupNodeResources(nodeName string) error {
	if err := oc.deleteNodeLogicalNetwork(nodeName); err != nil {
		return fmt.Errorf("error deleting node %s logical network: %v", nodeName, err)
	}

	if err := oc.newGatewayManager(nodeName).Cleanup(); err != nil {
		return fmt.Errorf("failed to clean up node %s gateway: (%w)", nodeName, err)
	}

	chassisTemplateVars := make([]*nbdb.ChassisTemplateVar, 0)
	p := func(item *sbdb.Chassis) bool {
		if item.Hostname == nodeName {
			chassisTemplateVars = append(chassisTemplateVars, &nbdb.ChassisTemplateVar{Chassis: item.Name})
			return true
		}
		return false
	}
	if err := libovsdbops.DeleteChassisWithPredicate(oc.sbClient, p); err != nil {
		return fmt.Errorf("failed to remove the chassis associated with node %s in the OVN SB Chassis table: %v", nodeName, err)
	}
	if err := libovsdbops.DeleteChassisTemplateVar(oc.nbClient, chassisTemplateVars...); err != nil {
		return fmt.Errorf("failed deleting chassis template variables for %s: %v", nodeName, err)
	}
	return nil
}

// this is the worker function that does the periodic sync of nodes from kube API
// and sbdb and deletes chassis that are stale
func (oc *DefaultNetworkController) syncNodesPeriodic() {
	//node names is a slice of all node names
	kNodes, err := oc.watchFactory.GetNodes()
	if err != nil {
		klog.Errorf("Error getting existing nodes from kube API: %v", err)
		return
	}

	localZoneNodeNames := make([]string, 0, len(kNodes))
	remoteZoneNodeNames := make([]string, 0, len(kNodes))
	for i := range kNodes {
		if oc.isLocalZoneNode(kNodes[i]) {
			localZoneNodeNames = append(localZoneNodeNames, kNodes[i].Name)
		} else {
			remoteZoneNodeNames = append(remoteZoneNodeNames, kNodes[i].Name)
		}
	}

	if err := oc.syncChassis(localZoneNodeNames, remoteZoneNodeNames); err != nil {
		klog.Errorf("Failed to sync chassis: error: %v", err)
	}
}

// We only deal with cleaning up nodes that shouldn't exist here, since
// watchNodes() will be called for all existing nodes at startup anyway.
// Note that this list will include the 'join' cluster switch, which we
// do not want to delete.
func (oc *DefaultNetworkController) syncNodes(kNodes []interface{}) error {
	foundNodes := sets.New[string]()
	localZoneNodeNames := make([]string, 0, len(kNodes))
	remoteZoneKNodeNames := make([]string, 0, len(kNodes))
	for _, tmp := range kNodes {
		node, ok := tmp.(*corev1.Node)
		if !ok {
			return fmt.Errorf("spurious object in syncNodes: %v", tmp)
		}

		if config.HybridOverlay.Enabled && util.NoHostSubnet(node) {
			continue
		}

		// Add the node to the foundNodes only if it belongs to the local zone.
		if oc.isLocalZoneNode(node) {
			foundNodes.Insert(node.Name)
			oc.localZoneNodes.Store(node.Name, true)
			localZoneNodeNames = append(localZoneNodeNames, node.Name)
		} else {
			remoteZoneKNodeNames = append(remoteZoneKNodeNames, node.Name)
		}
	}

	defaultNetworkPredicate := func(item *nbdb.LogicalSwitch) bool {
		_, ok := item.ExternalIDs[types.NetworkExternalID]
		return len(item.OtherConfig) > 0 && !ok
	}
	nodeSwitches, err := libovsdbops.FindLogicalSwitchesWithPredicate(oc.nbClient, defaultNetworkPredicate)
	if err != nil {
		return fmt.Errorf("failed to get node logical switches which have other-config set: %v", err)
	}

	staleSwitches := sets.NewString()
	for _, nodeSwitch := range nodeSwitches {
		if nodeSwitch.Name != types.TransitSwitch && !foundNodes.Has(nodeSwitch.Name) {
			staleSwitches.Insert(nodeSwitch.Name)
		}
	}

	// Find stale external logical switches, based on well known prefix and node name
	lookupExtSwFunction := func(item *nbdb.LogicalSwitch) bool {
		_, ok := item.ExternalIDs[types.NetworkExternalID]
		if ok {
			return false
		}
		nodeName := strings.TrimPrefix(item.Name, types.ExternalSwitchPrefix)
		if nodeName != item.Name && len(nodeName) > 0 && !foundNodes.Has(nodeName) {
			staleSwitches.Insert(nodeName)
			return true
		}
		return false
	}
	_, err = libovsdbops.FindLogicalSwitchesWithPredicate(oc.nbClient, lookupExtSwFunction)
	if err != nil && !errors.Is(err, libovsdbclient.ErrNotFound) {
		klog.Warning("Failed trying to find stale external logical switches")
	}

	// Find stale gateway routers, based on well known prefix and node name
	lookupGwRouterFunction := func(item *nbdb.LogicalRouter) bool {
		_, ok := item.ExternalIDs[types.NetworkExternalID]
		if ok {
			return false
		}
		nodeName := strings.TrimPrefix(item.Name, types.GWRouterPrefix)
		if nodeName != item.Name && len(nodeName) > 0 && !foundNodes.Has(nodeName) {
			staleSwitches.Insert(nodeName)
			return true
		}
		return false
	}
	_, err = libovsdbops.FindLogicalRoutersWithPredicate(oc.nbClient, lookupGwRouterFunction)
	if err != nil && !errors.Is(err, libovsdbclient.ErrNotFound) {
		klog.Warning("Failed trying to find stale gateway routers")
	}

	// Cleanup stale nodes (including gateway routers and external logical switches)
	for _, staleSwitch := range staleSwitches.UnsortedList() {
		if err := oc.cleanupNodeResources(staleSwitch); err != nil {
			return fmt.Errorf("failed to cleanup node resources:%s, err:%w", staleSwitch, err)
		}
	}

	if err := oc.syncChassis(localZoneNodeNames, remoteZoneKNodeNames); err != nil {
		return fmt.Errorf("failed to sync chassis: error: %v", err)
	}

	if config.OVNKubernetesFeature.EnableInterconnect {
		if err := oc.zoneChassisHandler.SyncNodes(kNodes); err != nil {
			return fmt.Errorf("zoneChassisHandler failed to sync nodes: error: %w", err)
		}

		if err := oc.zoneICHandler.SyncNodes(kNodes); err != nil {
			return fmt.Errorf("zoneICHandler failed to sync nodes: error: %w", err)
		}
	}

	return nil
}

// Cleanup stale chassis and chassis template variables with no
// corresponding nodes.
func (oc *DefaultNetworkController) syncChassis(localZoneNodeNames, remoteZoneNodeNames []string) error {
	chassisList, err := libovsdbops.ListChassis(oc.sbClient)
	if err != nil {
		return fmt.Errorf("failed to get chassis list: error: %v", err)
	}

	// Cleanup stale chassis private with no corresponding chassis
	chassisPrivateList, err := libovsdbops.ListChassisPrivate(oc.sbClient)
	if err != nil {
		return fmt.Errorf("failed to get chassis private list: %v", err)
	}

	templateVarList := []*nbdb.ChassisTemplateVar{}

	if oc.svcTemplateSupport {
		templateVarList, err = libovsdbops.ListTemplateVar(oc.nbClient)
		if err != nil {
			return fmt.Errorf("failed to get template var list: error: %w", err)
		}
	}

	chassisHostNameMap := map[string]*sbdb.Chassis{}
	chassisNameMap := map[string]*sbdb.Chassis{}
	for _, chassis := range chassisList {
		chassisHostNameMap[chassis.Hostname] = chassis
		chassisNameMap[chassis.Name] = chassis
	}

	for _, chassisPrivate := range chassisPrivateList {
		// Skip chassis private that have a corresponding chassis
		if _, ok := chassisNameMap[chassisPrivate.Name]; ok {
			continue
		}
		// We add to the map what would be the corresponding Chassis. Even if
		// the Chassis does not exist in SBDB, DeleteChassis will remove the
		// ChassisPrivate.
		chassisNameMap[chassisPrivate.Name] = &sbdb.Chassis{Name: chassisPrivate.Name}
	}

	templateChassisMap := map[string]*nbdb.ChassisTemplateVar{}
	for _, templateVar := range templateVarList {
		templateChassisMap[templateVar.Chassis] = templateVar
	}

	// Delete existing nodes from the chassis map.
	// Also delete existing templateVars from the template map.
	for _, nodeName := range localZoneNodeNames {
		if chassis, ok := chassisHostNameMap[nodeName]; ok {
			delete(chassisNameMap, chassis.Name)
			delete(chassisHostNameMap, chassis.Hostname)
			delete(templateChassisMap, chassis.Name)
		}
	}

	// Delete existing remote zone nodes from the chassis map, but not from the templateVars
	// as we need to cleanup chassisTemplateVars for the remote zone nodes
	for _, nodeName := range remoteZoneNodeNames {
		if chassis, ok := chassisHostNameMap[nodeName]; ok {
			delete(chassisNameMap, chassis.Name)
			delete(chassisHostNameMap, chassis.Hostname)
		}
	}

	staleChassis := make([]*sbdb.Chassis, 0, len(chassisHostNameMap))
	for _, chassis := range chassisNameMap {
		staleChassis = append(staleChassis, chassis)
	}

	staleChassisTemplateVars := make([]*nbdb.ChassisTemplateVar, 0, len(templateChassisMap))
	for _, template := range templateChassisMap {
		staleChassisTemplateVars = append(staleChassisTemplateVars, template)
	}

	if err := libovsdbops.DeleteChassis(oc.sbClient, staleChassis...); err != nil {
		return fmt.Errorf("failed Deleting chassis %v error: %v", chassisHostNameMap, err)
	}

	if err := libovsdbops.DeleteChassisTemplateVar(oc.nbClient, staleChassisTemplateVars...); err != nil {
		return fmt.Errorf("failed Deleting chassis template vars %v error: %v", chassisHostNameMap, err)
	}

	return nil
}

// nodeSyncs structure contains flags for the different failures
// so the retry logic can control what need to retry based
type nodeSyncs struct {
	syncNode              bool
	syncClusterRouterPort bool
	syncMgmtPort          bool
	syncGw                bool
	syncHo                bool
	syncZoneIC            bool
	syncReroute           bool
}

func nodeNeedsSync(syncs *nodeSyncs) bool {
	return syncs.syncNode ||
		syncs.syncClusterRouterPort ||
		syncs.syncMgmtPort ||
		syncs.syncGw ||
		syncs.syncHo ||
		syncs.syncZoneIC ||
		syncs.syncReroute
}

func (oc *DefaultNetworkController) addUpdateLocalNodeEvent(node *corev1.Node, nSyncs *nodeSyncs) error {
	var hostSubnets []*net.IPNet
	var errs []error
	var err error

	_, _ = oc.localZoneNodes.LoadOrStore(node.Name, true)

	if noHostSubnet := util.NoHostSubnet(node); noHostSubnet {
		err := oc.lsManager.AddNoHostSubnetSwitch(node.Name)
		if err != nil {
			return fmt.Errorf("nodeAdd: error adding noHost subnet for switch %s: %w", node.Name, err)
		}
		if config.HybridOverlay.Enabled {
			// Parse the hybrid overlay host subnet for the node to
			// make sure that cluster manager has allocated the subnet.
			if _, err := houtil.ParseHybridOverlayHostSubnet(node); err != nil {
				return err
			}
		}
		return nil
	}

	if !nodeNeedsSync(nSyncs) {
		return nil
	}

	klog.Infof("Adding or Updating local node %q for network %q", node.Name, oc.GetNetworkName())
	if nSyncs.syncNode {
		if hostSubnets, err = oc.addNode(node); err != nil {
			oc.addNodeFailed.Store(node.Name, true)
			oc.nodeClusterRouterPortFailed.Store(node.Name, true)
			oc.mgmtPortFailed.Store(node.Name, true)
			oc.gatewaysFailed.Store(node.Name, true)
			oc.hybridOverlayFailed.Store(node.Name, config.HybridOverlay.Enabled)
			if nSyncs.syncZoneIC {
				oc.syncZoneICFailed.Store(node.Name, true)
			}
			return fmt.Errorf("nodeAdd: error adding node %q: %w", node.Name, err)
		}
		oc.addNodeFailed.Delete(node.Name)
	}

	// since the nodeSync objects are created knowing if hybridOverlay is enabled this should work
	if nSyncs.syncHo && config.HybridOverlay.Enabled {
		if err = oc.allocateHybridOverlayDRIP(node); err != nil {
			errs = append(errs, err)
			oc.hybridOverlayFailed.Store(node.Name, true)
		}
	}

	if nSyncs.syncClusterRouterPort {
		if err = oc.syncNodeClusterRouterPort(node, nil); err != nil {
			errs = append(errs, err)
			oc.nodeClusterRouterPortFailed.Store(node.Name, true)
		} else {
			oc.nodeClusterRouterPortFailed.Delete(node.Name)
		}
		// delete stale chassis in SBDB if any
		if err := oc.deleteStaleNodeChassis(node); err != nil {
			errs = append(errs, err)
		}
	}

	if nSyncs.syncMgmtPort {
		if hostSubnets == nil {
			hostSubnets, err = util.ParseNodeHostSubnetAnnotation(node, oc.GetNetworkName())
			if err != nil {
				errs = append(errs, err)
				oc.mgmtPortFailed.Store(node.Name, true)
			}
		}

		// If we successfully discovered the host subnets then add the management port.
		if hostSubnets != nil {
			if err = oc.syncNodeManagementPortDefault(node, oc.GetNetworkScopedSwitchName(node.Name), hostSubnets); err != nil {
				errs = append(errs, err)
				oc.mgmtPortFailed.Store(node.Name, true)
			} else {
				oc.mgmtPortFailed.Delete(node.Name)
			}
		}
	}

	if nSyncs.syncHo {
		annotator := kube.NewNodeAnnotator(oc.kube, node.Name)
		if config.HybridOverlay.Enabled {
			if err := oc.handleHybridOverlayPort(node, annotator); err != nil {
				errs = append(errs, fmt.Errorf("failed to set up hybrid overlay logical switch port for %s: %v", node.Name, err))
				oc.hybridOverlayFailed.Store(node.Name, true)
			} else {
				oc.hybridOverlayFailed.Delete(node.Name)
			}
		} else {
			// pedantic - node should never be stored in hybridOverlayFailed if HO is not enabled
			oc.hybridOverlayFailed.Delete(node.Name)

			// the node needs to cleanup Hybrid overlay annotations LogicalRouterPolicies and Hybrid overlay port
			// if it has them and hybrid overlay is not enabled
			if err := oc.deleteHybridOverlayPort(node); err != nil {
				errs = append(errs, err)
			} else {
				// only clear annotations if tear down was successful
				if _, exist := node.Annotations[hotypes.HybridOverlayDRMAC]; exist {
					annotator.Delete(hotypes.HybridOverlayDRMAC)
				}
				if _, exist := node.Annotations[hotypes.HybridOverlayDRIP]; exist {
					annotator.Delete(hotypes.HybridOverlayDRIP)
				}
			}
		}
		if err := annotator.Run(); err != nil {
			errs = append(errs, fmt.Errorf("failed to set hybrid overlay annotations for node %s: %v", node.Name, err))
		}
	}

	if nSyncs.syncGw {
		err := oc.syncNodeGateway(node)
		if err != nil {
			errs = append(errs, err)
			oc.gatewaysFailed.Store(node.Name, true)
		} else {
			oc.gatewaysFailed.Delete(node.Name)
		}
	}

	// ensure pods that already exist on this node have their logical ports created
	// if per pod SNAT is being used, then l3 gateway config is required to be able to add pods
	if _, gwFailed := oc.gatewaysFailed.Load(node.Name); !gwFailed || !config.Gateway.DisableSNATMultipleGWs {
		if nSyncs.syncNode || nSyncs.syncGw { // do this only if it is a new node add or a gateway sync happened
			errors := oc.addAllPodsOnNode(node.Name)
			errs = append(errs, errors...)
		}
	}

	if nSyncs.syncZoneIC && config.OVNKubernetesFeature.EnableInterconnect {
		// Call zone chassis handler's AddLocalZoneNode function to mark
		// this node's chassis record in Southbound db as a local zone chassis.
		// This is required when a node moves from a remote zone to local zone
		if err := oc.zoneChassisHandler.AddLocalZoneNode(node); err != nil {
			errs = append(errs, err)
			oc.syncZoneICFailed.Store(node.Name, true)
		} else {
			// Call zone IC handler's AddLocalZoneNode function to create
			// interconnect resources in the OVN Northbound db for this local zone node.
			if err := oc.zoneICHandler.AddLocalZoneNode(node); err != nil {
				errs = append(errs, err)
				oc.syncZoneICFailed.Store(node.Name, true)
			} else {
				oc.syncZoneICFailed.Delete(node.Name)
			}
		}
	}

	return utilerrors.Join(errs...)
}

func (oc *DefaultNetworkController) addUpdateRemoteNodeEvent(node *corev1.Node, syncZoneIC bool) error {
	// nothing to do for hybrid nodes
	if util.NoHostSubnet(node) {
		return nil
	}
	start := time.Now()
	// Check if the remote node is present in the local zone nodes.  If its present
	// it means it moved from this controller zone to other remote zone. Cleanup the node
	// from the local zone cache.
	_, present := oc.localZoneNodes.Load(node.Name)

	if present {
		klog.Infof("Node %q moved from the local zone %s to a remote zone %s. Cleaning the node resources", node.Name, oc.zone, util.GetNodeZone(node))
		if err := oc.cleanupNodeResources(node.Name); err != nil {
			return fmt.Errorf("error cleaning up the local resources for the remote node %s, err : %w", node.Name, err)
		}
		oc.localZoneNodes.Delete(node.Name)
	}

	var err error
	if syncZoneIC && config.OVNKubernetesFeature.EnableInterconnect {
		// Call zone chassis handler's AddRemoteZoneNode function to creates
		// the remote chassis for the remote zone node in the SB DB or mark
		// the entry as remote if it was local chassis earlier
		if err = oc.zoneChassisHandler.AddRemoteZoneNode(node); err != nil {
			err = fmt.Errorf("adding or updating remote node chassis %s failed, err - %w", node.Name, err)
			oc.syncZoneICFailed.Store(node.Name, true)
			return err
		}

		// Call zone IC handler's AddRemoteZoneNode function to create
		// interconnect resources in the OVN NBDB for this remote zone node.
		// Also, create the remote port binding in SBDB
		if err = oc.zoneICHandler.AddRemoteZoneNode(node); err != nil {
			err = fmt.Errorf("adding or updating remote node IC resources %s failed, err - %w", node.Name, err)
			oc.syncZoneICFailed.Store(node.Name, true)
		} else {
			oc.syncZoneICFailed.Delete(node.Name)
		}
		klog.V(5).Infof("Creating Interconnect resources for remote node %q on network %q took: %s", node.Name, oc.GetNetworkName(), time.Since(start))
	}
	return err
}

func (oc *DefaultNetworkController) deleteNodeEvent(node *corev1.Node) error {
	klog.V(5).Infof("Deleting Node %q. Removing the node from "+
		"various caches", node.Name)
	if config.HybridOverlay.Enabled && util.NoHostSubnet(node) {
		if err := oc.deleteHoNodeEvent(node); err != nil {
			return err
		}
	}
	return oc.deleteOVNNodeEvent(node)
}

func (oc *DefaultNetworkController) deleteOVNNodeEvent(node *corev1.Node) error {
	if config.HybridOverlay.Enabled {
		if err := oc.deleteHybridOverlayPort(node); err != nil {
			return fmt.Errorf("failed to delete hybrid overlay switch port for node %s: %w", node.Name, err)
		}
	}

	if err := oc.cleanupNodeResources(node.Name); err != nil {
		return err
	}

	if config.OVNKubernetesFeature.EnableInterconnect {
		if err := oc.zoneICHandler.DeleteNode(node); err != nil {
			return err
		}
		if !oc.isLocalZoneNode(node) {
			if err := oc.zoneChassisHandler.DeleteRemoteZoneNode(node); err != nil {
				return err
			}
		}
		oc.syncZoneICFailed.Delete(node.Name)
	}

	// Remove management port IP and node's gateway-router-lrp-ifaddr
	// from address_set specific to HostNetworkNamespace
	if err := oc.delIPFromHostNetworkNamespaceAddrSet(node); err != nil {
		return fmt.Errorf("failed to delete IPs from %s address_set: %v",
			config.Kubernetes.HostNetworkNamespace, err)
	}

	oc.lsManager.DeleteSwitch(node.Name)
	oc.addNodeFailed.Delete(node.Name)
	oc.mgmtPortFailed.Delete(node.Name)
	oc.gatewaysFailed.Delete(node.Name)
	oc.nodeClusterRouterPortFailed.Delete(node.Name)
	oc.localZoneNodes.Delete(node.Name)
	oc.syncHostNetAddrSetFailed.Delete(node.Name)

	return nil
}

// addUpdateHoNodeEvent reconsile ovn nodes when a hybrid overlay node is added.
func (oc *DefaultNetworkController) addUpdateHoNodeEvent(node *corev1.Node) error {
	if subnets, _ := util.ParseNodeHostSubnetAnnotation(node, oc.GetNetworkName()); len(subnets) > 0 {
		klog.Infof("Node %q is used to be a OVN-K managed node, deleting it from OVN topology", node.Name)
		if err := oc.deleteOVNNodeEvent(node); err != nil {
			return err
		}
	}

	err := oc.lsManager.AddNoHostSubnetSwitch(node.Name)
	if err != nil {
		return fmt.Errorf("nodeAdd: error adding no hostsubnet for switch %s: %w", node.Name, err)
	}

	// Parse the hybrid overlay host subnet annotation of the node to
	// make sure that the subnet is allocated.
	if _, err := houtil.ParseHybridOverlayHostSubnet(node); err != nil {
		return err
	}

	nodes, err := oc.watchFactory.GetNodes()
	if err != nil {
		return err
	}
	annotator := kube.NewNodeAnnotator(oc.kube, node.Name)

	for _, node := range nodes {
		node := *node
		// reconcile hybrid overlay subnets for local zone nodes.
		if !util.NoHostSubnet(&node) && oc.isLocalZoneNode(&node) {
			if err := oc.handleHybridOverlayPort(&node, annotator); err != nil {
				return err
			}
		}
	}
	return nil
}

func (oc *DefaultNetworkController) deleteHoNodeEvent(node *corev1.Node) error {
	if oc.lsManager.IsNonHostSubnetSwitch(node.Name) {
		klog.Infof("Delete hybrid overlay node switch %s", node.Name)
		oc.lsManager.DeleteSwitch(node.Name)
	}
	if subnet, ok := node.Annotations[hotypes.HybridOverlayNodeSubnet]; ok {
		// Delete the routes and policies for this HO node
		_, nodeSubnet, err := net.ParseCIDR(subnet)
		if err != nil {
			return fmt.Errorf("failed to parse hybridOverlay node subnet for node %s: %w", node.Name, err)
		}
		err = oc.removeRoutesToHONodeSubnet(node.Name, nodeSubnet)
		if err != nil {
			return fmt.Errorf("failed to remove hybrid overlay static routes and route policy: %w", err)
		}
	}
	return nil
}

// addIPToHostNetworkNamespaceAddrSet adds management port IP and node's
// gateway-router-lrp-ifaddr to address_set created for HostNetworkNamespace.
// This function gets called from both AddResource & UpdateResource to add IPs
// to address_set for both local and remote zone nodes.
func (oc *DefaultNetworkController) addIPToHostNetworkNamespaceAddrSet(node *corev1.Node) error {
	var hostNetworkPolicyIPs []net.IP

	if util.NoHostSubnet(node) {
		return nil
	}
	hostNetworkPolicyIPs, err := oc.getHostNamespaceAddressesForNode(node)
	if err != nil {
		parsedErr := err
		if !oc.isLocalZoneNode(node) {
			parsedErr = types.NewSuppressedError(err)
		}
		return fmt.Errorf("error parsing annotation for node %s: %w", node.Name, parsedErr)
	}

	// add the host network IPs for this node to host network namespace's address set
	if err = func() error {
		hostNetworkNamespace := config.Kubernetes.HostNetworkNamespace
		if hostNetworkNamespace != "" {
			nsInfo, nsUnlock, err := oc.ensureNamespaceLocked(hostNetworkNamespace, true, nil)
			if err != nil {
				return fmt.Errorf("failed to ensure namespace locked: %v", err)
			}
			defer nsUnlock()
			if err = nsInfo.addressSet.AddAddresses(util.StringSlice(hostNetworkPolicyIPs)); err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		return err
	}
	return nil
}

// delIPFromHostNetworkNamespaceAddrSet removes management port IP and node's
// gateway-router-lrp-ifaddr from address_set created for HostNetworkNamespace.
// This function gets called from deleteOVNNodeEvent to remove IPs from address_set
// for both local and remote zone nodes
func (oc *DefaultNetworkController) delIPFromHostNetworkNamespaceAddrSet(node *corev1.Node) error {
	var hostNetworkPolicyIPs []net.IP

	hostNetworkPolicyIPs, err := oc.getHostNamespaceAddressesForNode(node)
	if err != nil {
		if util.IsAnnotationNotSetError(err) {
			// if annotation is not set for node subnet or node GW router LRP IP address, we can assume nothing was added to the
			// host network namespace address set. We depend on both annotations to be set before configuring the address set.
			return nil
		}
		return fmt.Errorf("error parsing annotation for node %s: %v", node.Name, err)
	}

	// delete host network IPs for this node from host network namespace's address set
	if err = func() error {
		hostNetworkNamespace := config.Kubernetes.HostNetworkNamespace
		if hostNetworkNamespace != "" {
			nsInfo, nsUnlock, err := oc.ensureNamespaceLocked(hostNetworkNamespace, true, nil)
			if err != nil {
				return fmt.Errorf("failed to ensure namespace locked: %v", err)
			}
			defer nsUnlock()
			if err = nsInfo.addressSet.DeleteAddresses(util.StringSlice(hostNetworkPolicyIPs)); err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		return err
	}
	return nil
}

func (oc *DefaultNetworkController) newGatewayManager(nodeName string) *GatewayManager {
	gatewayManager := NewGatewayManager(
		nodeName,
		oc.defaultCOPPUUID,
		oc.kube,
		oc.nbClient,
		oc.GetNetInfo(),
		oc.watchFactory,
		oc.gatewayOptions()...,
	)
	return gatewayManager
}

func (oc *DefaultNetworkController) gatewayOptions() []GatewayOption {
	var opts []GatewayOption
	if oc.clusterLoadBalancerGroupUUID != "" {
		opts = append(opts, WithLoadBalancerGroups(
			oc.routerLoadBalancerGroupUUID,
			oc.clusterLoadBalancerGroupUUID,
			oc.switchLoadBalancerGroupUUID,
		))
	}
	return opts
}
