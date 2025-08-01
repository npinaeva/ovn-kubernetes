package util

//
//
//import (
//	"fmt"
//	libovsdb "github.com/ovn-org/libovsdb/ovsdb"
//	"time"
//
//	libovsdbclient "github.com/ovn-org/libovsdb/client"
//	libovsdbops "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdb/ops"
//	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
//)
//
//const RtrPGNAme = "a4743249366342378346"
//
//func TestASWithACLs(nbClient libovsdbclient.Client, addrsets, aclPerAddrset int) error {
//	ops := []libovsdb.Operation{}
//	var err error
//	for i := 1; i <= addrsets; i++ {
//		ids :=libovsdbops.NewDbObjectIDs(libovsdbops.AddressSetNetworkPolicy, "controlkler", map[libovsdbops.ExternalIDKey]string{
//			libovsdbops.ObjectNameKey: "namespace_name",
//			libovsdbops.PolicyDirectionKey: "egress",
//			libovsdbops.GressIdxKey:        fmt.Sprintf("%d", i),
//			libovsdbops.IPFamilyKey: 	  "ip4",
//		})
//		as := &nbdb.AddressSet{
//			Name: fmt.Sprintf("addressset%d", i),
//			Addresses: []string{"1.1.1.1"},
//			ExternalIDs: ids.GetExternalIDs(),
//		}
//		ops, err = libovsdbops.CreateOrUpdateAddressSetsOps(nbClient, ops, as)
//		if err != nil {
//			return err
//		}
//		for j := 1; j <= aclPerAddrset; j++ {
//			aclIds := libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetworkPolicyPortIndex, "controlkler",
//				map[libovsdbops.ExternalIDKey]string{
//					// policy namespace+name
//					libovsdbops.ObjectNameKey: "namespace_name",
//					// egress or ingress
//					libovsdbops.PolicyDirectionKey:  "egress",
//					// gress rule index
//					libovsdbops.GressIdxKey: fmt.Sprintf("%d_%d", i, j),
//					libovsdbops.PortPolicyIndexKey: "-1",
//					libovsdbops.IpBlockIndexKey:    "-1",
//				})
//			acl := &nbdb.ACL{
//				Action:      nbdb.ACLActionAllow,
//				Direction:   nbdb.ACLDirectionToLport,
//				ExternalIDs: aclIds.GetExternalIDs(),
//				Log:         true,
//				Match:       "ip4.src ==@" + as.Name,
//				Meter:       nil,
//				Name:        nil,
//				Options:     nil,
//				Priority:    1,
//				Severity:    nil,
//			}
//			ops, err = libovsdbops.CreateOrUpdateACLsOps(nbClient, ops, acl)
//			if err != nil {
//				return err
//			}
//			ops, err = libovsdbops.AddACLsToPortGroupOps(nbClient, ops, RtrPGNAme, acl)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	start := time.Now()
//	fmt.Println("tx begin", time.Now())
//	_, err = libovsdbops.TransactAndCheck(nbClient, ops)
//	fmt.Println("tx end", time.Now(), time.Since(start))
//	return err
//}
//
//func UpdateAsWithACLs(nbClient libovsdbclient.Client, addrsets, aclPerAddrset int) error {
//	ops := []libovsdb.Operation{}
//	var err error
//	for i := 1; i <= addrsets; i++ {
//		as := &nbdb.AddressSet{
//			Name: fmt.Sprintf("addressset%d", i),
//		}
//		as, err := libovsdbops.GetAddressSet(nbClient, as)
//		as.Name = fmt.Sprintf("newaddressset%d", i)
//		ops, err = libovsdbops.CreateOrUpdateAddressSetsOps(nbClient, ops, as)
//		if err != nil {
//			return err
//		}
//		for j := 1; j <= aclPerAddrset; j++ {
//			aclIds := libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetworkPolicyPortIndex, "controlkler",
//				map[libovsdbops.ExternalIDKey]string{
//					// policy namespace+name
//					libovsdbops.ObjectNameKey: "namespace_name",
//					// egress or ingress
//					libovsdbops.PolicyDirectionKey:  "egress",
//					// gress rule index
//					libovsdbops.GressIdxKey: fmt.Sprintf("%d_%d", i, j),
//					libovsdbops.PortPolicyIndexKey: "-1",
//					libovsdbops.IpBlockIndexKey:    "-1",
//				})
//			acl := &nbdb.ACL{
//				Action:      nbdb.ACLActionAllow,
//				Direction:   nbdb.ACLDirectionToLport,
//				ExternalIDs: aclIds.GetExternalIDs(),
//				Log:         true,
//				Match:       "ip4.src ==@" + as.Name,
//				Meter:       nil,
//				Name:        nil,
//				Options:     nil,
//				Priority:    1,
//				Severity:    nil,
//			}
//			ops, err = libovsdbops.CreateOrUpdateACLsOps(nbClient, ops, acl)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	start := time.Now()
//	fmt.Println("tx begin", time.Now())
//	_, err = libovsdbops.TransactAndCheck(nbClient, ops)
//	fmt.Println("tx end", time.Now(), time.Since(start))
//	return err
//}
//
//func Test(nbClient libovsdbclient.Client) {
//	//clusterPG := &nbdb.PortGroup{
//	//	Name: RtrPGNAme,
//	//}
//	//clusterPG, err := libovsdbops.GetPortGroup(nbClient, clusterPG)
//	//if err != nil {
//	//	fmt.Println(err)
//	//	return
//	//}
//	//clusterPG.ACLs = []string{}
//	//
//	//fmt.Println("cleanup port group", time.Now())
//	//err = libovsdbops.CreateOrUpdatePortGroups(nbClient, clusterPG)
//	//fmt.Println("cleanup port group done", time.Now())
//	//if err != nil {
//	//	fmt.Println(err)
//	//	return
//	//}
//	//err = libovsdbops.DeleteAddressSetsWithPredicate(nbClient, func(as *nbdb.AddressSet) bool {
//	//	return as.ExternalIDs[libovsdbops.ObjectNameKey.String()] == "namespace_name"
//	//})
//	//if err != nil {
//	//	fmt.Println(err)
//	//	return
//	//}
//	err := UpdateAsWithACLs(nbClient, 50, 10000)
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//}
