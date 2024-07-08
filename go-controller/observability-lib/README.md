An example program that uses the observability library to decode and print samples can be found in
[ovnkubeobserv.go](../cmd/ovnkube-observ/ovnkubeobserv.go).
To get the binary, run 
```shell
hack/build-go.sh cmd/ovnkube-observ
```
The binary will be created under `./_output/go/bin`.

## OCP test setup
```shell
# pick the node to use for testing, NODE_NAME
oc debug node/NODE_NAME
chroot /host
mkdir /root/rpms
(on a different terminal)
oc get pods (get the name of the debug-pod)
# copy the kernel rpms to the debug pod
oc cp ./rpms <debug pod>:/host/root/rpms
(back to debug container)
rpm-ostree status
rpm-ostree override replace *.rpm
rpm-ostree status
(should show the new upgrade)

systemctl reboot
# wait for the node to become Ready

# now replace ovn-k with the new image
oc scale deployment cluster-version-operator -n openshift-cluster-version --replicas=0
oc scale deployment network-operator -n openshift-network-operator --replicas=0

# edit required containers, wait for them to restart
oc -n openshift-ovn-kubernetes set image ds/ovnkube-node ovnkube-controller=quay.io/npinaeva/ovn-k:observ nbdb=quay.io/npinaeva/ovn-k:observ sbdb=quay.io/npinaeva/ovn-k:observ ovn-controller=quay.io/npinaeva/ovn-k:observ northd=quay.io/npinaeva/ovn-k:observ

# Sampling is ready now, you can either go to the observ agent now, or continue with the local debug mode as follows.
# copy the ovnkube-observ binary to the debug pod
oc cp ~/ocp/ovn-kubernetes/dist/images/ovnkube-observ <debug pod>:/host/root/rpms
# on debug pod
/root/rpms/ovnkube-observ -enable-enrichment
# should print messages like
OVN-K message: Allowed by default allow from local node policy, direction ingress
group_id 10, obs_domain=50331651, obs_point=3468260520, PACKET: 66 bytes
- Layer 1 (14 bytes) = Ethernet	{Contents=[..14..] Payload=[..52..] SrcMAC=7a:7b:ea:df:85:64 DstMAC=0a:58:0a:81:02:06 EthernetType=IPv4 Length=0}
- Layer 2 (20 bytes) = IPv4	{Contents=[..20..] Payload=[..32..] Version=4 IHL=5 TOS=0 Length=52 Id=43684 Flags=DF FragOffset=0 TTL=64 Protocol=TCP Checksum=30486 SrcIP=10.129.2.2 DstIP=10.129.2.6 Options=[] Padding=[]}
- Layer 3 (32 bytes) = TCP	{Contents=[..32..] Payload=[] SrcPort=40118 DstPort=8181(intermapper) Seq=2008547592 Ack=2164584236 DataOffset=8 FIN=false SYN=false RST=false PSH=false ACK=true URG=false ECE=false CWR=false NS=false Window=257 Checksum=6448 Urgent=0 Options=[TCPOption(NOP:), TCPOption(NOP:), TCPOption(Timestamps:4148251443/673084145 0xf7414b33281e72f1)] Padding=[]}

# now you can add network policies and see the logs like
OVN-K message: Allowed by network policy default:egress-ns-selector, direciton Egress
OR
OVN-K message: Dropped by network policies isolation in namespace default, direction Egress
```
