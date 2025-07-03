package node

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/generator/udn"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/bridgeconfig"
	nodeutil "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/util"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

type BridgeManager struct {
	DefaultBridge         *bridgeconfig.BridgeConfiguration
	ExternalGatewayBridge *bridgeconfig.BridgeConfiguration
	defaultOFM            *openflowManager
	externalOFM           *openflowManager
	// channel to indicate we need to update flows immediately
	flowChan chan struct{}
}

func NewBridgeManager(gwBridge, exGWBridge *bridgeconfig.BridgeConfiguration) *BridgeManager {
	m := &BridgeManager{
		DefaultBridge: gwBridge,
		defaultOFM:    newOpenFlowManager(gwBridge.GetBridgeName()),
		flowChan:      make(chan struct{}, 1),
	}
	if exGWBridge != nil {
		m.ExternalGatewayBridge = exGWBridge
		m.externalOFM = newOpenFlowManager(exGWBridge.GetBridgeName())
	}
	return m
}

func (c *BridgeManager) getDefaultBridgePortConfigurations() ([]*bridgeconfig.BridgeUDNConfiguration, string, string) {
	return c.DefaultBridge.GetPortConfigurations()
}

func (c *BridgeManager) getExGwBridgePortConfigurations() ([]*bridgeconfig.BridgeUDNConfiguration, string, string) {
	return c.ExternalGatewayBridge.GetPortConfigurations()
}

func (c *BridgeManager) addNetwork(nInfo util.NetInfo, nodeSubnets []*net.IPNet, masqCTMark, pktMark uint, v6MasqIPs, v4MasqIPs *udn.MasqueradeIPs) error {
	if err := c.DefaultBridge.AddNetworkConfig(nInfo, nodeSubnets, masqCTMark, pktMark, v6MasqIPs, v4MasqIPs); err != nil {
		return err
	}
	if c.ExternalGatewayBridge != nil {
		if err := c.ExternalGatewayBridge.AddNetworkConfig(nInfo, nodeSubnets, masqCTMark, pktMark, v6MasqIPs, v4MasqIPs); err != nil {
			return err
		}
	}
	return nil
}

func (c *BridgeManager) delNetwork(nInfo util.NetInfo) {
	c.DefaultBridge.DelNetworkConfig(nInfo)
	if c.ExternalGatewayBridge != nil {
		c.ExternalGatewayBridge.DelNetworkConfig(nInfo)
	}
}

func (c *BridgeManager) getActiveNetwork(nInfo util.NetInfo) *bridgeconfig.BridgeUDNConfiguration {
	return c.DefaultBridge.GetActiveNetworkBridgeConfigCopy(nInfo.GetNetworkName())
}

func (c *BridgeManager) getDefaultBridgeName() string {
	return c.DefaultBridge.GetBridgeName()
}

func (c *BridgeManager) getDefaultBridgeMAC() net.HardwareAddr {
	return c.DefaultBridge.GetMAC()
}

func (c *BridgeManager) setDefaultBridgeMAC(macAddr net.HardwareAddr) {
	c.DefaultBridge.SetMAC(macAddr)
}

func (c *BridgeManager) Run(stopChan <-chan struct{}, doneWg *sync.WaitGroup) {
	doneWg.Add(1)
	go func() {
		defer doneWg.Done()
		syncPeriod := 15 * time.Second
		timer := time.NewTicker(syncPeriod)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if err := checkPorts(c.getDefaultBridgePortConfigurations()); err != nil {
					klog.Errorf("Checkports failed %v", err)
					continue
				}

				if c.ExternalGatewayBridge != nil {
					if err := checkPorts(c.getExGwBridgePortConfigurations()); err != nil {
						klog.Errorf("Checkports failed %v", err)
						continue
					}
				}
				c.syncFlows()
			case <-c.flowChan:
				c.syncFlows()
				timer.Reset(syncPeriod)
			case <-stopChan:
				return
			}
		}
	}()
}

func (c *BridgeManager) syncFlows() {
	c.defaultOFM.syncFlows()
	if c.externalOFM != nil {
		c.externalOFM.syncFlows()
	}
}

func (c *BridgeManager) requestFlowSync() {
	select {
	case c.flowChan <- struct{}{}:
		klog.V(5).Infof("Gateway OpenFlow sync requested")
	default:
		klog.V(5).Infof("Gateway OpenFlow sync already requested")
	}
}

func (c *BridgeManager) UpdateBridgePMTUDFlowCache(key string, ipAddrs []string) {
	dftFlows := c.DefaultBridge.PmtudDropFlows(ipAddrs)
	c.defaultOFM.updateFlowCacheEntry(key, dftFlows)
	if c.ExternalGatewayBridge != nil {
		exGWBridgeDftFlows := c.ExternalGatewayBridge.PmtudDropFlows(ipAddrs)
		c.externalOFM.updateFlowCacheEntry(key, exGWBridgeDftFlows)
	}
}

// UpdateBridgeFlowCache generates the "static" per-bridge flows
// note: this is shared between shared and local gateway modes
func (c *BridgeManager) UpdateBridgeFlowCache(hostIPs []net.IP, hostSubnets []*net.IPNet) error {
	// CAUTION: when adding new flows where the in_port is ofPortPatch and the out_port is ofPortPhys, ensure
	// that dl_src is included in match criteria!

	dftFlows, err := c.DefaultBridge.DefaultBridgeFlows(hostSubnets, hostIPs)
	if err != nil {
		return err
	}

	c.defaultOFM.updateFlowCacheEntry("NORMAL", []string{fmt.Sprintf("table=0,priority=0,actions=%s\n", util.NormalAction)})
	c.defaultOFM.updateFlowCacheEntry("DEFAULT", dftFlows)

	// we consume ex gw bridge flows only if that is enabled
	if c.ExternalGatewayBridge != nil {
		exGWBridgeDftFlows, err := c.ExternalGatewayBridge.ExternalBridgeFlows(hostSubnets)
		if err != nil {
			return err
		}

		c.externalOFM.updateFlowCacheEntry("NORMAL", []string{fmt.Sprintf("table=0,priority=0,actions=%s\n", util.NormalAction)})
		c.externalOFM.updateFlowCacheEntry("DEFAULT", exGWBridgeDftFlows)
	}
	return nil
}

func (c *BridgeManager) UpdateDefaultBridgeFlows(key string, flows []string) {
	c.defaultOFM.updateFlowCacheEntry(key, flows)
}

func (c *BridgeManager) DeleteDefaultBridgeFlows(key string) {
	c.defaultOFM.deleteFlowsByKey(key)
}

func checkPorts(netConfigs []*bridgeconfig.BridgeUDNConfiguration, physIntf, ofPortPhys string) error {
	// it could be that the ovn-controller recreated the patch between the host OVS bridge and
	// the integration bridge, as a result the ofport number changed for that patch interface
	for _, netConfig := range netConfigs {
		if netConfig.OfPortPatch == "" {
			continue
		}
		curOfportPatch, stderr, err := util.GetOVSOfPort("--if-exists", "get", "Interface", netConfig.PatchPort, "ofport")
		if err != nil {
			return fmt.Errorf("failed to get ofport of %s, stderr: %q: %w", netConfig.PatchPort, stderr, err)

		}
		if netConfig.OfPortPatch != curOfportPatch {
			if netConfig.IsDefaultNetwork() || curOfportPatch != "" {
				klog.Errorf("Fatal error: patch port %s ofport changed from %s to %s",
					netConfig.PatchPort, netConfig.OfPortPatch, curOfportPatch)
				os.Exit(1)
			} else {
				klog.Warningf("Patch port %s removed for existing network", netConfig.PatchPort)
			}
		}
	}

	// it could be that someone removed the physical interface and added it back on the OVS host
	// bridge, as a result the ofport number changed for that physical interface
	curOfportPhys, stderr, err := util.GetOVSOfPort("--if-exists", "get", "interface", physIntf, "ofport")
	if err != nil {
		return fmt.Errorf("failed to get ofport of %s, stderr: %q: %w", physIntf, stderr, err)
	}
	if ofPortPhys != curOfportPhys {
		klog.Errorf("Fatal error: phys port %s ofport changed from %s to %s",
			physIntf, ofPortPhys, curOfportPhys)
		os.Exit(1)
	}
	return nil
}

type openflowManager struct {
	bridgeName string
	// flow cache, use map instead of array for readability when debugging
	flowCache map[string][]string
	flowMutex sync.Mutex
}

func (c *openflowManager) updateFlowCacheEntry(key string, flows []string) {
	c.flowMutex.Lock()
	defer c.flowMutex.Unlock()
	c.flowCache[key] = flows
}

func (c *openflowManager) deleteFlowsByKey(key string) {
	c.flowMutex.Lock()
	defer c.flowMutex.Unlock()
	delete(c.flowCache, key)
}

func (c *openflowManager) getFlowsByKey(key string) []string {
	c.flowMutex.Lock()
	defer c.flowMutex.Unlock()
	return c.flowCache[key]
}

func (c *openflowManager) syncFlows() {
	c.flowMutex.Lock()
	defer c.flowMutex.Unlock()

	flows := []string{}
	for _, entry := range c.flowCache {
		flows = append(flows, entry...)
	}

	_, stderr, err := util.ReplaceOFFlows(c.bridgeName, flows)
	if err != nil {
		klog.Errorf("Failed to add flows, error: %v, stderr, %s, flows: %s", err, stderr, c.flowCache)
	}
}

// since we share the host's k8s node IP, add OpenFlow flows
// -- to steer the NodePort traffic arriving on the host to the OVN logical topology and
// -- to also connection track the outbound north-south traffic through l3 gateway so that
//
//	the return traffic can be steered back to OVN logical topology
//
// -- to handle host -> service access, via masquerading from the host to OVN GR
// -- to handle external -> service(ExternalTrafficPolicy: Local) -> host access without SNAT
func newOpenFlowManager(bridgeName string) *openflowManager {
	// add health check function to check default OpenFlow flows are on the shared gateway bridge
	ofm := &openflowManager{
		bridgeName: bridgeName,
		flowCache:  make(map[string][]string),
		flowMutex:  sync.Mutex{},
	}

	// defer flowSync until syncService() to prevent the existing service OpenFlows being deleted
	return ofm
}

// bootstrapOVSFlows handles ensuring basic, required flows are in place. This is done before OpenFlow manager has
// been created/started, and only done when there is just a NORMAL flow programmed and OVN/OVS is already setup
func bootstrapOVSFlows(nodeName string) error {
	// see if patch port exists already
	var portsOutput string
	var stderr string
	var err error
	if portsOutput, stderr, err = util.RunOVSVsctl("--no-heading", "--data=bare", "--format=csv", "--columns",
		"name", "list", "interface"); err != nil {
		// bridge exists, but could not list ports
		return fmt.Errorf("failed to list ports on existing bridge br-int: %s, %w", stderr, err)
	}

	bridge, patchPort := localnetPortInfo(nodeName, portsOutput)

	if len(bridge) == 0 {
		// bridge exists but no patch port was found
		return nil
	}

	// get the current flows and if there is more than just default flow, we dont need to bootstrap as we already
	// have flows
	flows, err := util.GetOFFlows(bridge)
	if err != nil {
		return err
	}
	if len(flows) > 1 {
		// more than 1 flow, assume the OVS has retained previous flows from previous running OVNK instance
		return nil
	}

	// only have 1 flow, need to install required flows
	klog.Infof("Default NORMAL flow installed on OVS bridge: %s, will bootstrap with required port security flows", bridge)

	// Get ofport of patchPort
	ofportPatch, stderr, err := util.GetOVSOfPort("get", "Interface", patchPort, "ofport")
	if err != nil {
		return fmt.Errorf("failed while waiting on patch port %q to be created by ovn-controller and "+
			"while getting ofport. stderr: %q, error: %v", patchPort, stderr, err)
	}

	var bridgeMACAddress net.HardwareAddr
	if config.OvnKubeNode.Mode == types.NodeModeDPU {
		hostRep, err := util.GetDPUHostInterface(bridge)
		if err != nil {
			return err
		}
		bridgeMACAddress, err = util.GetSriovnetOps().GetRepresentorPeerMacAddress(hostRep)
		if err != nil {
			return err
		}
	} else {
		bridgeMACAddress, err = util.GetOVSPortMACAddress(bridge)
		if err != nil {
			return fmt.Errorf("failed to get MAC address for ovs port %s: %w", bridge, err)
		}
	}

	var dftFlows []string
	// table 0, check packets coming from OVN have the correct mac address. Low priority flows that are a catch all
	// for non-IP packets that would normally be forwarded with NORMAL action (table 0, priority 0 flow).
	dftFlows = append(dftFlows,
		fmt.Sprintf("cookie=%s, priority=10, table=0, in_port=%s, dl_src=%s, actions=output:NORMAL",
			nodeutil.DefaultOpenFlowCookie, ofportPatch, bridgeMACAddress))
	dftFlows = append(dftFlows,
		fmt.Sprintf("cookie=%s, priority=9, table=0, in_port=%s, actions=drop",
			nodeutil.DefaultOpenFlowCookie, ofportPatch))
	dftFlows = append(dftFlows, "priority=0, table=0, actions=output:NORMAL")

	_, stderr, err = util.ReplaceOFFlows(bridge, dftFlows)
	if err != nil {
		return fmt.Errorf("failed to add flows, error: %v, stderr, %s, flows: %s", err, stderr, dftFlows)
	}

	return nil
}

// localnetPortInfo returns the name of the bridge and the patch port name for the default cluster network
func localnetPortInfo(nodeName string, portsOutput string) (string, string) {
	// This needs to work with:
	// - default network: patch-<bridge name>_<node>-to-br-int
	// but not with:
	// - user defined primary network: patch-<bridge name>_<network-name>_<node>-to-br-int
	// - user defined secondary localnet network: patch-<bridge name>_<network-name>_ovn_localnet_port-to-br-int
	// TODO: going forward, maybe it would preferable to just read the bridge name from the config.
	r := regexp.MustCompile(fmt.Sprintf("^patch-([^_]*)_%s-to-br-int$", nodeName))
	for _, line := range strings.Split(portsOutput, "\n") {
		matches := r.FindStringSubmatch(line)
		if len(matches) == 2 {
			return matches[1], matches[0]
		}
	}
	return "", ""
}
