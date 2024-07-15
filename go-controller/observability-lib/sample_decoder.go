package observability_lib

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os/exec"
	"strings"
	"time"
	"unsafe"

	"github.com/ovn-org/libovsdb/client"
	libovsdbops "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdb/ops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/observability"
)

type SampleDecoder struct {
	nbClient client.Client
}

type nbConfig struct {
	address string
	scheme  string
}

type Cookie struct {
	ObsDomainID uint32
	ObsPointID  uint32
}

const cookieSize = 8
const bridgeName = "br-int"

var nativeEndian binary.ByteOrder

func setEndian() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		nativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		nativeEndian = binary.BigEndian
	default:
		panic("Could not determine native endianness.")
	}
	fmt.Printf("Endian is %v\n", nativeEndian)
}

func getLocalNBClient(ctx context.Context) (client.Client, error) {
	config := nbConfig{
		address: "unix:/var/run/ovn/ovnnb_db.sock",
		scheme:  "unix",
	}
	libovsdbOvnNBClient, err1 := NewNBClientWithConfig(ctx, config)
	if err1 == nil {
		return libovsdbOvnNBClient, nil
	}
	config = nbConfig{
		address: "unix:/var/run/ovn-ic/ovnnb_db.sock",
		scheme:  "unix",
	}
	var err2 error
	libovsdbOvnNBClient, err2 = NewNBClientWithConfig(ctx, config)
	if err2 != nil {
		return nil, fmt.Errorf("error creating libovsdb client: [ with /var/run/ovn/ovnnb_db.sock: %w, with /var/run/ovn-ic/ovnnb_db.sock: %w ]", err1, err2)
	}
	return libovsdbOvnNBClient, nil
}

func NewSampleDecoder(ctx context.Context) (*SampleDecoder, error) {
	setEndian()
	nbClient, err := getLocalNBClient(ctx)
	if err != nil {
		return nil, err
	}
	return &SampleDecoder{
		nbClient: nbClient,
	}, nil
}

func getObservAppID(obsDomainID uint32) uint8 {
	return uint8(obsDomainID >> 24)
}

// TODO Check this with secondary indexes
func (d *SampleDecoder) getACL(acl *nbdb.ACL) (*nbdb.ACL, error) {
	results := []*nbdb.ACL{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := d.nbClient.Where(acl).List(ctx, &results)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		if len(results) > 1 {
			for _, res := range results {
				fmt.Printf("ACL: %+v", res)
			}
		}
		return nil, fmt.Errorf("expected 1 ACL, got %d", len(results))
	}
	return results[0], nil
}

func (d *SampleDecoder) DecodeCookieIDs(obsDomainID, obsPointID uint32) (string, error) {
	// Find sample using obsPointID
	sample, err := libovsdbops.FindSample(d.nbClient, int(obsPointID))
	if err != nil || sample == nil {
		return "", fmt.Errorf("find sample failed: %w", err)
	}
	// find db object using observ application ID
	var dbObj interface{}
	switch getObservAppID(obsDomainID) {
	case observability.ACLNewTrafficSamplingID:
		acls, err := libovsdbops.FindACLsWithPredicate(d.nbClient, func(acl *nbdb.ACL) bool {
			return acl.SampleNew != nil && *acl.SampleNew == sample.UUID
		})
		if err != nil {
			return "", fmt.Errorf("find acl for sample failed: %w", err)
		}
		if len(acls) != 1 {
			return "", fmt.Errorf("expected 1 ACL, got %d", len(acls))
		}
		dbObj = acls[0]
	case observability.ACLEstTrafficSamplingID:
		acls, err := libovsdbops.FindACLsWithPredicate(d.nbClient, func(acl *nbdb.ACL) bool {
			return acl.SampleEst != nil && *acl.SampleEst == sample.UUID
		})
		if err != nil {
			return "", fmt.Errorf("find acl for sample failed: %w", err)
		}
		if len(acls) != 1 {
			return "", fmt.Errorf("expected 1 ACL, got %d", len(acls))
		}
		dbObj = acls[0]
	default:
		return "", fmt.Errorf("unknown app ID: %d", getObservAppID(obsDomainID))
	}
	msg := getMessage(dbObj)
	if msg == "" {
		return "", fmt.Errorf("failed to get message for db object %v", dbObj)
	}
	return msg, nil
}

func getMessage(dbObj interface{}) string {
	switch o := dbObj.(type) {
	case *nbdb.ACL:
		action := "Allowed"
		if o.Action == "drop" {
			action = "Dropped"
		}
		actor := o.ExternalIDs[libovsdbops.OwnerTypeKey.String()]
		var msg string
		switch actor {
		case string(libovsdbops.AdminNetworkPolicyOwnerType):
			msg = fmt.Sprintf("admin network policy %s, direction %s", o.ExternalIDs[libovsdbops.ObjectNameKey.String()], o.ExternalIDs[libovsdbops.PolicyDirectionKey.String()])
		case string(libovsdbops.BaselineAdminNetworkPolicyOwnerType):
			msg = fmt.Sprintf("baseline admin network policy %s, direction %s", o.ExternalIDs[libovsdbops.ObjectNameKey.String()], o.ExternalIDs[libovsdbops.PolicyDirectionKey.String()])
		case string(libovsdbops.MulticastNamespaceOwnerType):
			msg = fmt.Sprintf("multicast in namespace %s, direction %s", o.ExternalIDs[libovsdbops.ObjectNameKey.String()], o.ExternalIDs[libovsdbops.PolicyDirectionKey.String()])
		case string(libovsdbops.MulticastClusterOwnerType):
			msg = fmt.Sprintf("cluster multicast policy, direction %s", o.ExternalIDs[libovsdbops.PolicyDirectionKey.String()])
		case string(libovsdbops.NetpolNodeOwnerType):
			msg = fmt.Sprintf("default allow from local node policy, direction ingress")
		case string(libovsdbops.NetworkPolicyOwnerType):
			msg = fmt.Sprintf("network policy %s, direction %s", o.ExternalIDs[libovsdbops.ObjectNameKey.String()], o.ExternalIDs[libovsdbops.PolicyDirectionKey.String()])
		case string(libovsdbops.NetpolNamespaceOwnerType):
			msg = fmt.Sprintf("network policies isolation in namespace %s, direction %s", o.ExternalIDs[libovsdbops.ObjectNameKey.String()], o.ExternalIDs[libovsdbops.PolicyDirectionKey.String()])
		case string(libovsdbops.EgressFirewallOwnerType):
			msg = fmt.Sprintf("egress firewall in namespace %s", o.ExternalIDs[libovsdbops.ObjectNameKey.String()])
		}
		return fmt.Sprintf("%s by %s", action, msg)
	default:
		return ""
	}
}

func (d *SampleDecoder) DecodeCookieBytes(cookie []byte) (string, error) {
	if uint64(len(cookie)) != cookieSize {
		return "", fmt.Errorf("invalid cookie size: %d", len(cookie))
	}
	c := Cookie{}
	err := binary.Read(bytes.NewReader(cookie), nativeEndian, &c)
	if err != nil {
		return "", err
	}
	return d.DecodeCookieIDs(c.ObsDomainID, c.ObsPointID)
}

func (d *SampleDecoder) DecodeCookie8Bytes(cookie [8]byte) (string, error) {
	c := Cookie{}
	err := binary.Read(bytes.NewReader(cookie[:]), nativeEndian, &c)
	if err != nil {
		return "", err
	}
	return d.DecodeCookieIDs(c.ObsDomainID, c.ObsPointID)
}

// TODO psample group?
func AddDefaultCollector() error {
	out, _ := exec.Command("ovs-vsctl", "find", "Flow_Sample_Collector_Set",
		fmt.Sprintf("id=%v", observability.DefaultObservabilityCollectorSetID)).Output()
	if string(out) != "" {
		return nil
	}
	return exec.Command("ovs-vsctl", "--id=@br", "get", "Bridge", bridgeName, "--",
		"create", "Flow_Sample_Collector_Set", "bridge=@br",
		fmt.Sprintf("id=%v", observability.DefaultObservabilityCollectorSetID), "psample_group=10").Run()
}

func DeleteDefaultCollector() error {
	out, err := exec.Command("ovs-vsctl", "--no-headings", "--columns=_uuid", "find", "Flow_Sample_Collector_Set",
		fmt.Sprintf("id=%v", observability.DefaultObservabilityCollectorSetID)).Output()
	fmt.Println("find default collector", string(out), err)
	if err != nil {
		return err
	}
	uuid := strings.TrimSpace(string(out))
	if string(out) == "" {
		return nil
	}
	_, err = exec.Command("ovs-vsctl", "destroy", "Flow_Sample_Collector_Set", uuid).Output()
	return err
}
