package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/ovn-org/ovn-kubernetes/test/e2e/deploymentconfig"
	"github.com/ovn-org/ovn-kubernetes/test/e2e/feature"
	"github.com/ovn-org/ovn-kubernetes/test/e2e/images"
	"github.com/ovn-org/ovn-kubernetes/test/e2e/infraprovider"
	infraapi "github.com/ovn-org/ovn-kubernetes/test/e2e/infraprovider/api"
	"github.com/ovn-org/ovn-kubernetes/test/e2e/ipalloc"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2erc "k8s.io/kubernetes/test/e2e/framework/rc"
	e2eservice "k8s.io/kubernetes/test/e2e/framework/service"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	utilnet "k8s.io/utils/net"
	"k8s.io/utils/pointer"
)

const (
	metallbNamespace      = "metallb-system"
	speakerLabelgSelector = "component=speaker"
)

var (
	reportPath string
)

var _ = ginkgo.Describe("Services", feature.Service, func() {
	const (
		serviceName               = "testservice"
		echoServerPodNameTemplate = "echo-server-pod-%d"
		echoClientPodName         = "echo-client-pod"
		echoServiceName           = "echo-service"
	)

	f := wrappedTestFramework("services")

	var cs clientset.Interface

	ginkgo.BeforeEach(func() {
		cs = f.ClientSet
	})
	cleanupFn := func() {}

	ginkgo.AfterEach(func() {
		cleanupFn()
	})

	ginkgo.It("Allow connection to an external IP using a source port that is equal to a node port", func() {
		const (
			nodePort    = 31990
			connTimeout = "2"
			dstIPv4     = "1.1.1.1"
			dstPort     = "80"
		)
		if IsIPv6Cluster(f.ClientSet) {
			e2eskipper.Skipf("Test requires IPv4 or IPv4 primary dualstack cluster")
		}
		ginkgo.By("create node port service")
		jig := e2eservice.NewTestJig(cs, f.Namespace.Name, serviceName)
		_, err := jig.CreateTCPService(context.TODO(), func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.Ports[0].NodePort = nodePort
		})
		framework.ExpectNoError(err, "failed to create TCP node port service")
		ginkgo.By("create pod selected by node port service")
		serverPod := e2epod.NewAgnhostPod(f.Namespace.Name, "svc-backend", nil, nil, nil)
		serverPod.Labels = jig.Labels
		e2epod.NewPodClient(f).CreateSync(context.TODO(), serverPod)
		ginkgo.By("create pod which will connect externally")
		clientPod := e2epod.NewAgnhostPod(f.Namespace.Name, "client-for-external", nil, nil, nil)
		e2epod.NewPodClient(f).CreateSync(context.TODO(), clientPod)
		ginkgo.By("connect externally pinning the source port to equal the node port")
		_, err = e2ekubectl.RunKubectl(clientPod.Namespace, "exec", clientPod.Name, "--", "nc",
			"-p", strconv.Itoa(nodePort), "-z", "-w", connTimeout, dstIPv4, dstPort)
		framework.ExpectNoError(err, "expected connection to succeed using source port identical to node port")
	})

	ginkgo.It("Creates a host-network service, and ensures that host-network pods can connect to it", func() {
		namespace := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, namespace, serviceName)

		ginkgo.By("Creating a ClusterIP service")
		targetPort := infraprovider.Get().GetK8HostPort()
		service, err := jig.CreateUDPService(context.TODO(), func(s *v1.Service) {
			s.Spec.Ports = []v1.ServicePort{
				{
					Name:       "udp",
					Protocol:   v1.ProtocolUDP,
					Port:       80,
					TargetPort: intstr.FromInt(int(targetPort)),
				},
			}
		})
		framework.ExpectNoError(err)

		ginkgo.By("creating a host-network backend pod")

		serverPod := e2epod.NewAgnhostPod(namespace, "backend", nil, nil, []v1.ContainerPort{{ContainerPort: (int32(targetPort))}, {ContainerPort: (int32(targetPort)), Protocol: "UDP"}},
			"netexec", fmt.Sprintf("--udp-port=%d", targetPort))
		serverPod.Labels = jig.Labels
		serverPod.Spec.HostNetwork = true

		serverPod = e2epod.NewPodClient(f).CreateSync(context.TODO(), serverPod)
		nodeName := serverPod.Spec.NodeName

		ginkgo.By("Connecting to the service from another host-network pod on node " + nodeName)
		// find the ovn-kube node pod on this node
		pods, err := cs.CoreV1().Pods(deploymentconfig.Get().OVNKubernetesNamespace()).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "app=ovnkube-node",
			FieldSelector: "spec.nodeName=" + nodeName,
		})
		framework.ExpectNoError(err)
		gomega.Expect(pods.Items).To(gomega.HaveLen(1))
		clientPod := pods.Items[0]

		cmd := fmt.Sprintf(`/bin/sh -c 'echo hostname | /usr/bin/socat -t 5 - "udp:%s"'`,
			net.JoinHostPort(service.Spec.ClusterIP, "80"))

		err = wait.PollImmediate(framework.Poll, 30*time.Second, func() (bool, error) {
			stdout, err := e2epodoutput.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
			if err != nil {
				return false, err
			}
			return stdout == nodeName, nil
		})
		framework.ExpectNoError(err)
	})

	ginkgo.It("Creates a service with session-affinity, and ensures it works after backend deletion", func() {
		namespace := f.Namespace.Name
		servicePort := 80
		jig := e2eservice.NewTestJig(cs, namespace, serviceName)

		ginkgo.By("Creating a session-affinity service")
		var createdPods []*v1.Pod
		maxContainerFailures := 0
		replicas := 3
		targetPort := infraprovider.Get().GetK8HostPort()
		config := testutils.RCConfig{
			Client:               cs,
			Image:                imageutils.GetE2EImage(imageutils.Agnhost),
			Command:              []string{"/agnhost", "serve-hostname", fmt.Sprintf("--port=%d", targetPort)},
			Name:                 "backend",
			Labels:               jig.Labels,
			Namespace:            namespace,
			PollInterval:         3 * time.Second,
			Timeout:              framework.PodReadyBeforeTimeout,
			Replicas:             replicas,
			CreatedPods:          &createdPods,
			MaxContainerFailures: &maxContainerFailures,
		}
		err := e2erc.RunRC(context.TODO(), config)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(len(createdPods)).To(gomega.Equal(replicas), fmt.Sprintf("incorrect number of running pods: %v", len(createdPods)))

		svc, err := jig.CreateTCPService(context.TODO(), func(s *v1.Service) {
			s.Spec.SessionAffinity = "ClientIP"
			s.Spec.Type = v1.ServiceTypeClusterIP
			s.Spec.Ports = []v1.ServicePort{{
				Port: int32(servicePort),
				// agnhost serve-hostname port
				TargetPort: intstr.FromInt(int(targetPort)),
				Protocol:   v1.ProtocolTCP,
			}}
		})
		framework.ExpectNoError(err)

		execPod := e2epod.CreateExecPodOrFail(context.TODO(), cs, namespace, "execpod-affinity", nil)
		err = jig.CheckServiceReachability(context.TODO(), svc, execPod)
		framework.ExpectNoError(err)

		ensureStickySession := func() string {
			hosts := getServiceBackendsFromPod(execPod, svc.Spec.ClusterIP, int(svc.Spec.Ports[0].Port))
			uniqHosts := sets.New[string](hosts...)
			gomega.Expect(uniqHosts.Len()).To(gomega.Equal(1), fmt.Sprintf("expected the same backend for every connection with session-affinity set, got %v", uniqHosts))
			backendPod, _ := uniqHosts.PopAny()
			return backendPod
		}

		ginkgo.By("check sessions affinity from a client pod")
		backendPod := ensureStickySession()

		ginkgo.By(fmt.Sprintf("delete chosen backend pod %v", backendPod))
		err = e2epod.NewPodClient(f).Delete(context.TODO(), backendPod, metav1.DeleteOptions{})
		framework.ExpectNoError(err)
		err = e2epod.WaitForPodNotFoundInNamespace(context.TODO(), cs, backendPod, namespace, 60*time.Second)
		framework.ExpectNoError(err)

		ginkgo.By("check sessions affinity from a client pod again")
		ensureStickySession()
	})

	// The below series of tests queries nodePort services with hostNetwork:true and hostNetwork:false pods as endpoints,
	// for both HTTP and UDP and different ingress and egress payload sizes.
	// Steps:
	// * Set up a hostNetwork:true|false pod (agnhost echo server) on node z
	// * Set up a nodePort service on node y
	// * Set up a hostNetwork:true client pod on node x
	// * Query from node x to the service on node y that targets the pod on node z
	for _, hostNetwork := range []bool{true, false} {
		hostNetwork := hostNetwork
		ginkgo.When(fmt.Sprintf("a nodePort service targeting a pod with hostNetwork:%t is created", hostNetwork), func() {
			var serverPod *v1.Pod
			var serverPodNodeName string
			var serverPodName string

			var svc *v1.Service
			var serviceNode v1.Node
			var serviceNodeInternalIPs []string

			var clientPod *v1.Pod
			var clientPodNodeName string

			var echoPayloads = map[string]string{
				"small": fmt.Sprintf("%010d", 1),
				"large": fmt.Sprintf("%01420d", 1),
			}
			var echoMtuRegex = regexp.MustCompile(`cache expires.*mtu.*`)
			tcpPortName := "tcp-port"
			udpPortName := "udp-port"

			ginkgo.BeforeEach(func() {
				ginkgo.By("Selecting 3 schedulable nodes")
				nodes, err := e2enode.GetBoundedReadySchedulableNodes(context.TODO(), f.ClientSet, 3)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(len(nodes.Items)).To(gomega.BeNumerically(">", 2))

				ginkgo.By("Selecting nodes for pods and service")
				serverPodNodeName = nodes.Items[0].Name
				serviceNode = nodes.Items[1]
				clientPodNodeName = nodes.Items[2].Name

				ginkgo.By("Getting all InternalIP addresses of the service node")
				serviceNodeInternalIPs = e2enode.GetAddresses(&serviceNode, v1.NodeInternalIP)
				gomega.Expect(len(serviceNodeInternalIPs)).To(gomega.BeNumerically(">", 0))

				ginkgo.By("Creating hostNetwork:true client pod")
				clientPod = e2epod.NewAgnhostPod(f.Namespace.Name, echoClientPodName, nil, nil, nil)
				clientPod.Spec.NodeName = clientPodNodeName
				clientPod.Spec.HostNetwork = true
				for k := range clientPod.Spec.Containers {
					if clientPod.Spec.Containers[k].Name == "agnhost-container" {
						clientPod.Spec.Containers[k].Command = []string{
							"sleep",
							"infinity",
						}
						clientPod.Spec.Containers[k].SecurityContext.Privileged = pointer.Bool(true)
					}
				}
				e2epod.NewPodClient(f).CreateSync(context.TODO(), clientPod)

				ginkgo.By(fmt.Sprintf("Creating the server pod with hostNetwork:%t", hostNetwork))
				// Create the server pod.
				// Wait for 1 minute and if the pod does not come up, select a different port and try again.
				// Wait for a max of 5 minutes.
				serverPodPortTCP := infraprovider.Get().GetK8HostPort() // maybe a host net or cluster net pod but select host port anyway
				serverPodPortUDP := infraprovider.Get().GetK8HostPort() // maybe a host net or cluster net pod but select host port anyway
				gomega.Eventually(func() error {
					serverPodName = fmt.Sprintf(echoServerPodNameTemplate, serverPodPortTCP)
					framework.Logf("Creating server pod listening on TCP and UDP port %d", serverPodPortTCP)
					serverPod = e2epod.NewAgnhostPod(f.Namespace.Name, serverPodName, nil, nil, nil, "netexec",
						"--http-port",
						fmt.Sprintf("%d", serverPodPortTCP),
						"--udp-port",
						fmt.Sprintf("%d", serverPodPortUDP))
					serverPod.ObjectMeta.Labels = map[string]string{
						"app": serverPodName,
					}
					serverPod.Spec.HostNetwork = hostNetwork
					serverPod.Spec.NodeName = serverPodNodeName
					e2epod.NewPodClient(f).Create(context.TODO(), serverPod)

					err := e2epod.WaitTimeoutForPodReadyInNamespace(context.TODO(), f.ClientSet, serverPod.Name, f.Namespace.Name, 1*time.Minute)
					if err != nil {
						e2epod.NewPodClient(f).Delete(context.TODO(), serverPod.Name, metav1.DeleteOptions{})
						return err
					}
					serverPod, err = e2epod.NewPodClient(f).Get(context.TODO(), serverPod.Name, metav1.GetOptions{})
					return err
				}, 5*time.Minute, 1*time.Second).Should(gomega.Succeed())

				ginkgo.By("Creating the nodePort service")
				// Create the service.
				// If the servicePorts are already in use, creating the service should fail and we should choose another
				// random port.
				gomega.Eventually(func() error {
					servicePortTCP := rand.Intn(32767-30000) + 30000
					servicePortUDP := rand.Intn(32767-30000) + 30000

					framework.Logf("Creating the nodePort service")
					svc = &v1.Service{
						ObjectMeta: metav1.ObjectMeta{Name: echoServiceName},
						Spec: v1.ServiceSpec{
							Ports: []v1.ServicePort{
								{
									Name:       tcpPortName,
									NodePort:   int32(servicePortTCP),
									Port:       int32(serverPodPortTCP),
									TargetPort: intstr.FromInt(int(serverPodPortTCP)),
									Protocol:   v1.ProtocolTCP,
								},
								{
									Name:       udpPortName,
									NodePort:   int32(servicePortUDP),
									Port:       int32(serverPodPortUDP),
									TargetPort: intstr.FromInt(int(serverPodPortUDP)),
									Protocol:   v1.ProtocolUDP,
								},
							},
							Selector: map[string]string{"app": serverPodName},
							Type:     v1.ServiceTypeNodePort},
					}
					svc, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(context.TODO(), svc, metav1.CreateOptions{})
					if err != nil {
						framework.Logf("creating service failed, err: %v", err)
					}
					return err
				}, 60*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			// Run queries against the service both with a small (10 bytes + overhead for echo service) and
			// a large (1420 bytes + overhead for echo service) payload.
			// The payload is transmitted to and echoed from the echo service for both HTTP and UDP tests.
			ginkgo.When("tests are run towards the agnhost echo service", func() {

				ginkgo.It("queries to the nodePort service shall work for TCP", func() {
					tcpNodePort, _ := nodePortsFromService(svc)
					packetSizes := []string{"small", "large"}
					if isLocalGWModeEnabled() && hostNetwork {
						// if local gateway mode the intermediary node will attempt to fragment the packet, if the DF
						// bit is not set. However, the decision on setting DF bit is left up to the kernel, and
						// is unpredictable. If the DF bit is set, the iptables rule that DNATs nodeport -> cluster IP
						// will then attempt to route the packet, and hit our 1400 byte MTU route. This will cause:
						// 172.18.0.2:37755->10.96.141.254:9881(udp) sk_skb_reason_drop(SKB_DROP_REASON_PKT_TOO_BIG)
						packetSizes = []string{"small"}
					}
					for _, size := range packetSizes {
						for _, serviceNodeIP := range serviceNodeInternalIPs {
							serviceNodeIP := serviceNodeIP
							if IsIPv6Cluster(f.ClientSet) {
								serviceNodeIP = fmt.Sprintf("[%s]", serviceNodeIP)
							}
							ginkgo.By(fmt.Sprintf("Sending TCP %s payload to service IP %s "+
								"and expecting to receive the same payload", size, serviceNodeIP))
							cmd := fmt.Sprintf("curl --max-time 10 -g -q -s http://%s:%d/echo?msg=%s",
								serviceNodeIP,
								tcpNodePort,
								echoPayloads[size],
							)
							framework.Logf("Testing TCP %s with command %q", size, cmd)
							stdout, err := e2epodoutput.RunHostCmdWithRetries(
								clientPod.Namespace,
								clientPod.Name,
								cmd,
								framework.Poll,
								60*time.Second)
							framework.ExpectNoError(err, fmt.Sprintf("Testing TCP with %s payload failed", size))
							gomega.Expect(stdout).To(gomega.Equal(echoPayloads[size]), fmt.Sprintf("Testing TCP with %s payload failed", size))
						}
					}
				})

				// We have 2 possible scenarios - hostNetwork endpoints and non-hostNetwork endpoints.
				// We will only see fragmentation for large (> 1400 Bytes) packets and non-hostNetwork endpoints.
				//
				// hostNetwork endpoints:
				// Packet from ovn-worker2 to GR_ovn-worker where it will hit LB:
				// # ovn-nbctl lr-lb-list GR_ovn-worker | grep udp | grep 31206
				// 083a13aa-13e0-45bb-8a1a-175ce7e9fe81    Service_services    udp        172.18.0.3:31206      10.244.2.3:9894
				// With routes:
				// # ovn-nbctl lr-route-list GR_ovn-worker  | grep dst-ip
				//            10.244.0.0/16                100.64.0.1 dst-ip
				//                0.0.0.0/0                172.18.0.1 dst-ip rtoe-GR_ovn-worker
				// This means that the packet will enter port rtoe-GR_ovn-worker, be load-balanced and
				// exit port rtoe-GR_ovn-worker right away.
				// In OVN, we must apply the gateway_mtu setting to the rtoj port:
				// # ovn-nbctl find Logical_Router_Port name=rtoj-GR_ovn-control-plane | grep options
				// options             : {gateway_mtu="1400"}
				// As a consequence, we shall never see fragmentation for hostNetwork:true pods.
				//
				// non-hostNetwork endpoints:
				// # ovn-nbctl lr-lb-list GR_ovn-worker | grep udp | grep 31206
				// 083a13aa-13e0-45bb-8a1a-175ce7e9fe81    Service_services    udp        172.18.0.3:31206      10.244.2.3:9894
				// # ovn-nbctl lr-route-list GR_ovn-worker  | grep dst-ip
				//            10.244.0.0/16                100.64.0.1 dst-ip
				//                0.0.0.0/0                172.18.0.1 dst-ip rtoe-GR_ovn-worker
				// This time, the packet will leave the rtoj port and it will be fragmented.
				ginkgo.It("queries to the nodePort service shall work for UDP", func() {
					packetSizes := []string{"small", "large"}
					// If gateway mode is shared, and endpoint is OVN networked, host networked originated packets
					// exceeding pod MTU will not be delivered. This is because ICMP needs frag will be sent back to the original
					// Kubernetes node by OVN (even if DF bit is not set) and the node will refuse to install an MTU cache route.
					// To fix this later we can install ip rules that match on nodeport and lower the MTU from the originator
					// but for now we consider nodeport access from a k8s node as not a practical use case. See
					// https://issues.redhat.com/browse/OCPBUGS-7609
					// Furthermore, in local gateway mode, if the DF bit was not set the packet will go into the host of
					// intermediary node, where nodeport will be DNAT'ed to cluster IP service, and then hit the MTU 1400 route.
					// Netcat will not set Don't Fragment (DF) bit, so packet will be fragmented at intermediary
					// node and sent to server. However, it is up to the kernel to decide whether to set the DF bit,
					// and it is not predictable. Therefore, we have to skip large packet size for local gateway mode
					// as well. This is true when the endpoint is host or ovn networked, because the route for the cluster
					// cidr service is set to 1400, which causes:
					// 172.18.0.2:37755->10.96.141.254:9881(udp) sk_skb_reason_drop(SKB_DROP_REASON_PKT_TOO_BIG)
					if !hostNetwork || isLocalGWModeEnabled() {
						packetSizes = []string{"small"}
					}

					for _, size := range packetSizes {
						for _, serviceNodeIP := range serviceNodeInternalIPs {
							flushCmd := "ip route flush cache"
							if utilnet.IsIPv6String(serviceNodeIP) {
								flushCmd = "ip -6 route flush cache"
							}
							if size == "large" {
								// Flushing the IP route cache will remove any routes in the cache
								// that are a result of receiving a "need to frag" packet.
								ginkgo.By("Flushing the ip route cache")
								_, err := e2epodoutput.RunHostCmdWithRetries(
									clientPod.Namespace,
									clientPod.Name,
									flushCmd,
									framework.Poll,
									60*time.Second)
								framework.ExpectNoError(err, "Flushing the ip route cache failed")

								// List the current IP route cache for informative purposes.
								cmd := fmt.Sprintf("ip route get %s", serviceNodeIP)
								stdout, err := e2epodoutput.RunHostCmd(
									clientPod.Namespace,
									clientPod.Name,
									cmd)
								framework.ExpectNoError(err, "Listing IP route cache")
								framework.Logf("%s: %s", cmd, stdout)
							}
							// We expect the following to fail at least once for large payloads and non-hostNetwork
							// endpoints: the first request will fail as we have to receive a "need to frag" ICMP
							// message, subsequent requests then should succeed.
							_, udpNodePort := nodePortsFromService(svc)
							gomega.Eventually(func() error {
								ginkgo.By(fmt.Sprintf("Sending UDP %s payload to service IP %s "+
									"and expecting to receive the same payload", size, serviceNodeIP))
								// Send payload via UDP.
								cmd := fmt.Sprintf("echo 'echo %s' | nc -w2 -u %s %d",
									echoPayloads[size],
									serviceNodeIP,
									udpNodePort,
								)
								framework.Logf("Testing UDP %s with command %q", size, cmd)
								stdout, err := e2epodoutput.RunHostCmd(
									clientPod.Namespace,
									clientPod.Name,
									cmd)
								if err != nil {
									return err
								}
								// Compare received payload vs sent payload.
								if stdout != echoPayloads[size] {
									return fmt.Errorf("stdout does not match payloads[%s], %s != %s", size, stdout, echoPayloads[size])
								}
								// fc00:f853:ccd:e793::3 from :: dev breth0 src fc00:f853:ccd:e793::4 metric 256 expires 537sec mtu 1400 pref medium
								// for IPV6 the regex changes a bit
								if IsIPv6Cluster(f.ClientSet) {
									echoMtuRegex = regexp.MustCompile(`expires.*mtu.*`)
								}

								if size == "large" {
									cmd = fmt.Sprintf("ip route get %s", serviceNodeIP)
									stdout, err = e2epodoutput.RunHostCmd(
										clientPod.Namespace,
										clientPod.Name,
										cmd)
									if err != nil {
										return fmt.Errorf("could not list IP route cache, err: %q", err)
									}
									ginkgo.By("Making sure that the ip route cache does NOT contain an MTU route")
									if echoMtuRegex.Match([]byte(stdout)) {
										framework.Failf("found unexpected MTU cache route: %s", stdout)
									}
								}
								return nil
							}, 60*time.Second, 1*time.Second).Should(gomega.Succeed())
							// Flushing the IP route cache will remove any routes in the cache
							// that are a result of receiving a "need to frag" packet. Let's
							// flush this on all 3 nodes else we will run into the
							// bug: https://issues.redhat.com/browse/OCPBUGS-7609.
							// TODO: Revisit this once https://bugzilla.redhat.com/show_bug.cgi?id=2169839 is fixed.
							ovnKubernetesNamespace := deploymentconfig.Get().OVNKubernetesNamespace()
							ovnKubeNodePods, err := f.ClientSet.CoreV1().Pods(ovnKubernetesNamespace).List(context.TODO(), metav1.ListOptions{
								LabelSelector: "name=ovnkube-node",
							})
							if err != nil {
								framework.Failf("could not get ovnkube-node pods: %v", err)
							}
							for _, ovnKubeNodePod := range ovnKubeNodePods.Items {
								framework.Logf("Flushing the ip route cache on %s", ovnKubeNodePod.Name)
								containerName := "ovnkube-node"
								if isInterconnectEnabled() {
									containerName = "ovnkube-controller"
								}
								_, err := e2ekubectl.RunKubectl(ovnKubernetesNamespace, "exec", ovnKubeNodePod.Name, "--container", containerName, "--",
									"ip", "route", "flush", "cache")
								framework.ExpectNoError(err, "Flushing the ip route cache failed")
							}
						}
					}
				})
			})
		})

	}

	ginkgo.It("does not use host masquerade address as source IP address when communicating externally", func() {
		const (
			v6ExternAddr = "2001:db8:3333:4444:CCCC:DDDD:EEEE:FFFF"
			v4ExternAddr = "8.8.8.8"
			hostMasqIPv4 = "169.254.0.2"
			hostMasqIPv6 = "fd69::2"
		)
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(context.TODO(), cs, e2eservice.MaxNodesForEndpointsTests)
		framework.ExpectNoError(err)
		gomega.Expect(len(nodes.Items)).To(gomega.BeNumerically(">", 0))
		v4NodeAddrs := e2enode.FirstAddressByTypeAndFamily(nodes, v1.NodeInternalIP, v1.IPv4Protocol)
		v6NodeAddrs := e2enode.FirstAddressByTypeAndFamily(nodes, v1.NodeInternalIP, v1.IPv6Protocol)
		if v4NodeAddrs == "" && v6NodeAddrs == "" {
			framework.Failf("unable to detect if cluster supports IPv4 or IPv6")
		}
		nodeName := nodes.Items[0].Name
		getIPRouteGetOutput := func(dst string) string {
			cmd := []string{"ip"}
			if utilnet.IsIPv6String(dst) {
				cmd = append(cmd, "-6")
			}
			cmd = append(cmd, "route", "get", dst)
			output, err := infraprovider.Get().ExecK8NodeCommand(nodeName, cmd)
			framework.ExpectNoError(err, fmt.Sprintf("failed to exec '%v': %v", cmd, err))
			return output
		}
		isHostMasqSrcIP := func(dst, masqIP string) bool {
			output := getIPRouteGetOutput(dst)
			// if its not included in the output of ip route get, its sufficient to say, its not being used as src ip
			if strings.Contains(output, masqIP) {
				return true
			}
			return false
		}
		explain := "host masquerade IP incorrectly used as source IP for external communication"
		if v4NodeAddrs != "" { // v4 enabled
			gomega.Expect(isHostMasqSrcIP(v4ExternAddr, hostMasqIPv4)).Should(gomega.BeFalse(), explain)
		}
		if v6NodeAddrs != "" { // v6 enabled
			gomega.Expect(isHostMasqSrcIP(v6ExternAddr, hostMasqIPv6)).Should(gomega.BeFalse(), explain)
		}
	})

	// This test checks a special case: we add another IP address on the node *and* manually set that
	// IP address in to endpoints. It is used for some special apiserver hacks by remote cluster people.
	// So, ensure that it works for pod -> service and host -> service traffic
	ginkgo.It("All service features work when manually listening on a non-default address", func() {
		namespace := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, namespace, serviceName)
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(context.TODO(), cs, e2eservice.MaxNodesForEndpointsTests)
		framework.ExpectNoError(err)
		node := nodes.Items[0]
		nodeName := node.Name
		pods, err := cs.CoreV1().Pods(deploymentconfig.Get().OVNKubernetesNamespace()).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "app=ovnkube-node",
			FieldSelector: "spec.nodeName=" + nodeName,
		})
		framework.ExpectNoError(err)
		gomega.Expect(pods.Items).To(gomega.HaveLen(1))
		clientPod := &pods.Items[0]

		ginkgo.By("Using node" + nodeName + " and pod " + clientPod.Name)

		ginkgo.By("Creating an empty ClusterIP service")
		udpHostNsPort := infraprovider.Get().GetK8HostPort()
		service, err := jig.CreateUDPService(context.TODO(), func(s *v1.Service) {
			s.Spec.Ports = []v1.ServicePort{
				{
					Name:       "udp",
					Protocol:   v1.ProtocolUDP,
					Port:       80,
					TargetPort: intstr.FromInt(int(udpHostNsPort)),
				},
			}

			s.Spec.Selector = nil // because we will manage the endpoints ourselves
		})
		framework.ExpectNoError(err)

		ginkgo.By("Adding an extra IP address to the node's loopback interface")
		isV6 := strings.Contains(service.Spec.ClusterIP, ":")
		octet := rand.Intn(255)
		extraIP := fmt.Sprintf("192.0.2.%d", octet)
		extraCIDR := extraIP + "/32"
		if isV6 {
			extraIP = fmt.Sprintf("fc00::%d", octet)
			extraCIDR = extraIP + "/128"
		}

		cmd := fmt.Sprintf(`ip -br addr; ip addr del %s dev lo; ip addr add %s dev lo; ip -br addr`, extraCIDR, extraCIDR)
		_, err = e2epodoutput.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
		framework.ExpectNoError(err)
		cleanupFn = func() {
			// initial pod used for host command may be deleted at this point, refetch
			pods, err := cs.CoreV1().Pods(deploymentconfig.Get().OVNKubernetesNamespace()).List(context.TODO(), metav1.ListOptions{
				LabelSelector: "app=ovnkube-node",
				FieldSelector: "spec.nodeName=" + nodeName,
			})
			framework.ExpectNoError(err)
			gomega.Expect(pods.Items).To(gomega.HaveLen(1))
			clientPod := &pods.Items[0]
			cmd := fmt.Sprintf(`ip addr del %s dev lo || true`, extraCIDR)
			_, _ = e2epodoutput.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
		}

		ginkgo.By("Starting a UDP server listening on the additional IP")
		// now that 2.2.2.2 exists on the node's lo interface, let's start a server listening on it
		// we use UDP here since agnhost lets us pick the listen address only for UDP
		serverPod := e2epod.NewAgnhostPod(namespace, "backend", nil, nil, []v1.ContainerPort{{ContainerPort: int32(udpHostNsPort)}, {ContainerPort: int32(udpHostNsPort), Protocol: "UDP"}},
			"netexec", "--udp-port="+fmt.Sprintf("%d", udpHostNsPort), "--udp-listen-addresses="+extraIP)
		serverPod.Labels = jig.Labels
		serverPod.Spec.NodeName = nodeName
		serverPod.Spec.HostNetwork = true
		serverPod.Spec.Containers[0].TerminationMessagePolicy = v1.TerminationMessageFallbackToLogsOnError
		e2epod.NewPodClient(f).CreateSync(context.TODO(), serverPod)

		ginkgo.By("Ensuring the server is listening on the additional IP")
		// Connect from host -> additional IP. This shouldn't touch OVN at all, just acting as a basic
		// sanity check that we're actually listening on this IP
		err = wait.PollImmediate(framework.Poll, 30*time.Second, func() (bool, error) {
			cmd = fmt.Sprintf(`echo hostname | /usr/bin/socat -t 5 - "udp:%s"`,
				net.JoinHostPort(extraIP, fmt.Sprintf("%d", udpHostNsPort)))
			stdout, err := e2epodoutput.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
			if err != nil {
				return false, err
			}
			return (stdout == nodeName), nil
		})
		framework.ExpectNoError(err)

		ginkgo.By("Adding this IP as a manual endpoint")
		_, err = f.ClientSet.CoreV1().Endpoints(namespace).Create(context.TODO(),
			&v1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{Name: service.Name},
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP: extraIP,
							},
						},
						Ports: []v1.EndpointPort{
							{
								Name:     "udp",
								Port:     int32(udpHostNsPort),
								Protocol: "UDP",
							},
						},
					},
				},
			},
			metav1.CreateOptions{},
		)
		framework.ExpectNoError(err)

		ginkgo.By("Confirming that the service is accesible via the service IP from a host-network pod")
		err = wait.PollImmediate(framework.Poll, 30*time.Second, func() (bool, error) {
			cmd = fmt.Sprintf(`/bin/sh -c 'echo hostname | /usr/bin/socat -t 5 - "udp:%s"'`,
				net.JoinHostPort(service.Spec.ClusterIP, "80"))
			stdout, err := e2epodoutput.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
			if err != nil {
				return false, err
			}
			return stdout == nodeName, nil
		})
		framework.ExpectNoError(err)

		ginkgo.By("Confirming that the service is accessible from the node's pod network")
		// Now, spin up a pod-network pod on the same node, and ensure we can talk to the "local address" service
		clientServerPod := e2epod.NewAgnhostPod(namespace, "client", nil, nil, []v1.ContainerPort{{ContainerPort: 12345}, {ContainerPort: 12345, Protocol: "UDP"}},
			"netexec")
		clientServerPod.Spec.NodeName = nodeName
		e2epod.NewPodClient(f).CreateSync(context.TODO(), clientServerPod)
		clientServerPod, err = e2epod.NewPodClient(f).Get(context.TODO(), clientServerPod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)

		// annoying: need to issue a curl to the test pod to tell it to connect to the service
		err = wait.PollImmediate(framework.Poll, 30*time.Second, func() (bool, error) {
			cmd = fmt.Sprintf("curl -g -q -s 'http://%s/dial?request=%s&protocol=%s&host=%s&port=%d&tries=1'",
				net.JoinHostPort(clientServerPod.Status.PodIP, "8080"),
				"hostname",
				"udp",
				service.Spec.ClusterIP,
				80)
			stdout, err := e2epodoutput.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
			if err != nil {
				return false, err
			}
			return stdout == fmt.Sprintf(`{"responses":["%s"]}`, nodeName), nil
		})
		framework.ExpectNoError(err)
	})

	ginkgo.Context("of type NodePort", func() {
		var nodes *v1.NodeList
		var err error
		nodeIPs := make(map[string]map[int]string)
		var egressNode string
		var providerCtx infraapi.Context

		const (
			endpointHTTPPort             = 80
			endpointUDPPort              = 90
			clusterHTTPPort              = 81
			clusterUDPPort               = 91
			clientContainerName          = "npclient"
			targetSecondaryContainerName = "egressSecondaryTargetNode-allowed"
		)

		ginkgo.BeforeEach(func() {
			providerCtx = infraprovider.Get().NewTestContext()
			nodeIPs = make(map[string]map[int]string)
			egressNode = ""
		})

		ginkgo.AfterEach(func() {
			ginkgo.By("Deleting additional IP addresses from nodes")
			for nodeName, ipFamilies := range nodeIPs {
				for _, ip := range ipFamilies {
					subnetMask := "/32"
					if utilnet.IsIPv6String(ip) {
						subnetMask = "/128"
					}
					_, err := infraprovider.Get().ExecK8NodeCommand(nodeName, []string{"ip", "addr", "delete",
						fmt.Sprintf("%s/%s", ip, subnetMask), "dev", deploymentconfig.Get().ExternalBridgeName()})
					if err != nil && !(strings.Contains(err.Error(),
						"RTNETLINK answers: Cannot assign requested address") || !strings.Contains(err.Error(), "Address not found")) {
						framework.Failf("failed to remove ip address %s from node %s, err: %q", ip, nodeName, err)
					}
				}
			}
			ginkgo.By("Deleting EgressIP Setup if any")
			e2ekubectl.RunKubectlOrDie("default", "delete", "eip", "egressip", "--ignore-not-found=true")
			if egressNode != "" {
				e2ekubectl.RunKubectlOrDie("default", "label", "node", egressNode, "k8s.ovn.org/egress-assignable-")
			}
			// network is removed by provider Context API
		})

		ginkgo.It("should listen on each host addresses", func() {
			endPoints := make([]*v1.Pod, 0)
			endpointsSelector := map[string]string{"servicebackend": "true"}
			nodesHostnames := sets.NewString()
			nodes, err = e2enode.GetBoundedReadySchedulableNodes(context.TODO(), f.ClientSet, 3)
			framework.ExpectNoError(err)

			if len(nodes.Items) < 3 {
				framework.Failf(
					"Test requires >= 3 Ready nodes, but there are only %v nodes",
					len(nodes.Items))
			}

			ginkgo.By("Creating the endpoints pod, one for each worker")
			for _, node := range nodes.Items {
				args := []string{
					"netexec",
					fmt.Sprintf("--http-port=%d", endpointHTTPPort),
					fmt.Sprintf("--udp-port=%d", endpointUDPPort),
				}
				pod, err := createPod(f, node.Name+"-ep", node.Name, f.Namespace.Name, []string{},
					endpointsSelector, func(p *v1.Pod) {
						p.Spec.Containers[0].Args = args
					})
				framework.ExpectNoError(err)

				endPoints = append(endPoints, pod)
				nodesHostnames.Insert(pod.Name)
			}

			ginkgo.By("Creating an external container to send the traffic from")
			primaryProviderNetwork, err := infraprovider.Get().PrimaryNetwork()
			framework.ExpectNoError(err, "failed to get primary network")
			externalContainerPort := infraprovider.Get().GetExternalContainerPort()
			externalContainer := infraapi.ExternalContainer{Name: clientContainerName, Image: images.AgnHost(), Network: primaryProviderNetwork,
				CmdArgs: getAgnHostHTTPPortBindCMDArgs(externalContainerPort), ExtPort: externalContainerPort}
			externalContainer, err = providerCtx.CreateExternalContainer(externalContainer)
			framework.ExpectNoError(err, "external container %s must be created", externalContainer.Name)

			// If `xgw` exists, connect client container to it
			exGwNetwork, err := infraprovider.Get().GetNetwork("xgw")
			if err == nil {
				_, _ = providerCtx.AttachNetwork(exGwNetwork, externalContainer.Name)
			}

			ginkgo.By("Selecting additional IP addresses for each node")
			// add new secondary IP from node subnet to all nodes, if the cluster is v6 add an ipv6 address
			toCurlAddresses := sets.NewString()
			primaryIPv4Subnet, ipv6, err := primaryProviderNetwork.IPv4IPv6Subnets()
			framework.ExpectNoError(err, "must get primary provider network subnets")
			primaryNetworkSubnet := primaryIPv4Subnet
			if IsIPv6Cluster(f.ClientSet) {
				primaryNetworkSubnet = ipv6
			}
			_, primaryNetworkNet, err := net.ParseCIDR(primaryNetworkSubnet)
			framework.ExpectNoError(err, "failed to parse primary subnet %q", primaryNetworkSubnet)
			for _, node := range nodes.Items {
				addrAnnotation, ok := node.Annotations["k8s.ovn.org/host-cidrs"]
				gomega.Expect(ok).To(gomega.BeTrue())

				var addrs []string
				var addrsFiltered []string
				err := json.Unmarshal([]byte(addrAnnotation), &addrs)
				framework.ExpectNoError(err, "failed to parse node[%s] host-address annotation[%s]", node.Name,
					addrAnnotation)
				for _, addr := range addrs {
					addrSplit := strings.Split(addr, "/")
					gomega.Expect(addrSplit).Should(gomega.HaveLen(2))
					ip := net.ParseIP(addrSplit[0])
					if !primaryNetworkNet.Contains(ip) {
						framework.Logf("skipping IP %s because its outside primary provider network %s", ip, primaryNetworkSubnet)
						continue
					}
					addrsFiltered = append(addrsFiltered, ip.String())
				}
				toCurlAddresses.Insert(addrsFiltered...)

				// Calculate and store for AfterEach new target IP addresses.
				var newIP string
				if nodeIPs[node.Name] == nil {
					nodeIPs[node.Name] = make(map[int]string)
				}
				if IsIPv6Cluster(f.ClientSet) {
					newIPIP, err := ipalloc.NewPrimaryIPv6()
					framework.ExpectNoError(err, "must get new primary provider IPv4")
					newIP = newIPIP.String()
					nodeIPs[node.Name][6] = newIP
				} else {
					newIPIP, err := ipalloc.NewPrimaryIPv4()
					framework.ExpectNoError(err, "must get new primary provider IPv4")
					nodeIPs[node.Name][4] = newIPIP.String()
				}
			}
			iproute2Proto := "-4"
			if IsIPv6Cluster(f.ClientSet) {
				iproute2Proto = "-6"
			}
			ginkgo.By("Adding additional IP addresses to each node")
			for nodeName, ipFamilies := range nodeIPs {
				for _, ip := range ipFamilies {
					// manually add the a secondary IP to each node
					framework.Logf("adding IP %q to Node %s", ip, nodeName)
					_, err = infraprovider.Get().ExecK8NodeCommand(nodeName, []string{"ip", iproute2Proto, "addr", "add", ip, "dev", deploymentconfig.Get().ExternalBridgeName()})
					if err != nil {
						framework.Failf("failed to add new IP address %s to node %s: %v", ip, nodeName, err)
					}
					providerCtx.AddCleanUpFn(func() error {
						// manually add the a secondary IP to each node
						_, err = infraprovider.Get().ExecK8NodeCommand(nodeName, []string{"ip", iproute2Proto, "addr", "del", ip, "dev", deploymentconfig.Get().ExternalBridgeName()})
						if err != nil {
							return fmt.Errorf("failed to add new IP address %s to node %s: %v", ip, nodeName, err)
						}
						return nil
					})
					toCurlAddresses.Insert(ip)
				}
			}

			isIPv6Cluster := IsIPv6Cluster(f.ClientSet)

			ginkgo.By("Creating NodePort services")

			etpLocalServiceName := "etp-local-svc"
			etpLocalSvc := nodePortServiceSpecFrom(etpLocalServiceName, v1.IPFamilyPolicyPreferDualStack,
				endpointHTTPPort, endpointUDPPort, clusterHTTPPort, clusterUDPPort, endpointsSelector,
				v1.ServiceExternalTrafficPolicyTypeLocal)
			etpLocalSvc, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(context.Background(), etpLocalSvc,
				metav1.CreateOptions{})
			framework.ExpectNoError(err)

			etpClusterServiceName := "etp-cluster-svc"
			etpClusterSvc := nodePortServiceSpecFrom(etpClusterServiceName, v1.IPFamilyPolicyPreferDualStack,
				endpointHTTPPort, endpointUDPPort, clusterHTTPPort, clusterUDPPort, endpointsSelector,
				v1.ServiceExternalTrafficPolicyTypeCluster)
			etpClusterSvc, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(context.Background(),
				etpClusterSvc, metav1.CreateOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("Waiting for the endpoints to pop up")

			err = framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, f.Namespace.Name,
				etpLocalServiceName, len(endPoints), time.Second, wait.ForeverTestTimeout)
			framework.ExpectNoError(err, "failed to validate endpoints for service %s in namespace: %s",
				etpLocalServiceName, f.Namespace.Name)

			err = framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, f.Namespace.Name,
				etpClusterServiceName, len(endPoints), time.Second, wait.ForeverTestTimeout)
			framework.ExpectNoError(err, "failed to validate endpoints for service %s in namespace: %s",
				etpClusterServiceName, f.Namespace.Name)

			for _, serviceSpec := range []*v1.Service{etpLocalSvc, etpClusterSvc} {
				tcpNodePort, udpNodePort := nodePortsFromService(serviceSpec)

				for _, protocol := range []string{"http", "udp"} {
					toCurlPort := int32(tcpNodePort)
					if protocol == "udp" {
						toCurlPort = int32(udpNodePort)
					}

					for _, address := range toCurlAddresses.List() {
						if !isIPv6Cluster && utilnet.IsIPv6String(address) {
							continue
						}

						ginkgo.By("Hitting service " + serviceSpec.Name + " on " + address + " via " + protocol)
						gomega.Eventually(func() bool {
							epHostname := pokeEndpointViaExternalContainer(externalContainer, protocol, address, toCurlPort,
								"hostname")
							// Expect to receive a valid hostname
							return nodesHostnames.Has(epHostname)
						}, "40s", "1s").Should(gomega.BeTrue())
					}
				}
			}
		})

		ginkgo.It("should work on secondary node interfaces for ETP=local and ETP=cluster when backend pods are also served by EgressIP", func() {
			endPoints := make([]*v1.Pod, 0)
			endpointsSelector := map[string]string{"servicebackend": "true"}
			nodesHostnames := sets.NewString()
			isIPv6Cluster := IsIPv6Cluster(f.ClientSet)

			nodes, err = e2enode.GetBoundedReadySchedulableNodes(context.TODO(), f.ClientSet, 3)
			framework.ExpectNoError(err)

			if len(nodes.Items) < 3 {
				framework.Failf(
					"Test requires >= 3 Ready nodes, but there are only %v nodes",
					len(nodes.Items))
			}

			ginkgo.By("Creating the endpoints pod, one for each worker")
			for _, node := range nodes.Items {
				args := []string{
					"netexec",
					fmt.Sprintf("--http-port=%d", endpointHTTPPort),
					fmt.Sprintf("--udp-port=%d", endpointUDPPort),
				}
				pod, err := createPod(f, node.Name+"-ep", node.Name, f.Namespace.Name, []string{},
					endpointsSelector, func(p *v1.Pod) {
						p.Spec.Containers[0].Args = args
					})
				framework.ExpectNoError(err)

				endPoints = append(endPoints, pod)
				nodesHostnames.Insert(pod.Name)
			}

			ginkgo.By("Choosing egressIP pod")
			egressPod := endPoints[0]
			framework.Logf("EgressIP pod is %s/%s", endPoints[0].Namespace, endPoints[0].Name)

			ginkgo.By("Label egress node" + egressNode + " create external container to send egress traffic to via secondary MultiNIC EIP")
			egressNode = egressPod.Spec.NodeName
			e2enode.AddOrUpdateLabelOnNode(f.ClientSet, egressNode, "k8s.ovn.org/egress-assignable", "dummy")
			ginkgo.By("creating secondary network and attaching to all Nodes and external container")
			// configure and add additional network to worker containers for EIP multi NIC feature
			subnet := secondaryIPV4Subnet
			if isIPv6Cluster {
				subnet = secondaryIPV6Subnet
			}
			secondaryProviderNetwork, err := providerCtx.CreateNetwork(secondaryNetworkName, subnet)
			framework.ExpectNoError(err, "creation of network %s must be available to attach containers", secondaryNetworkName)
			nodes, err = f.ClientSet.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
			framework.ExpectNoError(err, "must list all Nodes")
			for _, node := range nodes.Items {
				_, err = providerCtx.AttachNetwork(secondaryProviderNetwork, node.Name)
				framework.ExpectNoError(err, "network %s must attach to node %s", secondaryProviderNetwork.Name, node.Name)
			}
			serverExternalContainerPort := infraprovider.Get().GetExternalContainerPort()
			serverExternalContainerSpec := infraapi.ExternalContainer{
				Name:    targetSecondaryContainerName,
				Image:   images.AgnHost(),
				Network: secondaryProviderNetwork,
				CmdArgs: getAgnHostHTTPPortBindCMDArgs(serverExternalContainerPort),
				ExtPort: serverExternalContainerPort,
			}
			serverExternalContainer, err := providerCtx.CreateExternalContainer(serverExternalContainerSpec)
			framework.ExpectNoError(err, "creation of external container %s attached to network must succeed",
				serverExternalContainerSpec.Name, serverExternalContainer.Network.Name())
			var serverExternalContainerIP string
			if isIPv6Cluster {
				serverExternalContainerIP = serverExternalContainer.GetIPv6()
			} else {
				serverExternalContainerIP = serverExternalContainer.GetIPv4()
			}
			ginkgo.By("Create an EgressIP object with one secondary multi NIC egress IP defined")
			egressIP := "10.10.10.105" // secondary subnet as defined in EIP test suite
			if isIPv6Cluster {
				egressIP = "2001:db8:abcd:1234:c001::" // secondary subnet as defined in EIP test suite
			}

			var egressIPConfig = fmt.Sprintf(`apiVersion: k8s.ovn.org/v1
kind: EgressIP
metadata:
    name: ` + "egressip" + `
spec:
    egressIPs:
    - "` + egressIP + `"
    namespaceSelector:
        matchLabels:
            kubernetes.io/metadata.name: ` + f.Namespace.Name + `
`)
			if err := os.WriteFile("egressip.yaml", []byte(egressIPConfig), 0644); err != nil {
				framework.Failf("Unable to write CRD config to disk: %v", err)
			}
			ginkgo.DeferCleanup(func() error {
				e2ekubectl.RunKubectlOrDie("default", "delete", "-f", "egressip.yaml", "--ignore-not-found=true")
				if err := os.Remove("egressip.yaml"); err != nil {
					return fmt.Errorf("unable to remove the CRD config from disk: %v", err)
				}
				return nil
			})

			framework.Logf("Create the EgressIP configuration")
			e2ekubectl.RunKubectlOrDie("default", "create", "-f", "egressip.yaml")

			ginkgo.By("Check that the status is of length one and that it is assigned to " + egressNode)
			err = wait.PollImmediate(retryInterval, retryTimeout, func() (bool, error) {
				egressIP := egressIPs{}
				egressIPStdout, err := e2ekubectl.RunKubectl("default", "get", "eip", "-o", "json")
				if err != nil {
					framework.Logf("Error: failed to get the EgressIP object, err: %v", err)
					return false, nil
				}
				json.Unmarshal([]byte(egressIPStdout), &egressIP)
				if len(egressIP.Items) > 1 {
					framework.Failf("Didn't expect to retrieve more than one egress IP during the execution of this test, saw: %v", len(egressIP.Items))
				}
				return egressIP.Items[0].Status.Items[0].Node == egressNode, nil
			})
			if err != nil {
				framework.Failf("Error: expected to have 1 egress IP assignment")
			}

			// FIXME: implement feature to connect networks
			// FIXME: tests shouldnt depend on external resources precreated and instead manage those resources within
			// the lifecycle of the test.
			// If `xgw` exists, connect client container to it
			exGWNetwork, err := infraprovider.Get().GetNetwork("xgw")
			if err == nil {
				_, _ = providerCtx.AttachNetwork(exGWNetwork, serverExternalContainer.GetName())
			}

			ginkgo.By("Selecting additional IP addresses for each node")
			// add new secondary IP from node subnet to all nodes, if the cluster is v6 add an ipv6 address
			secondaryNetworkV4Subnet, secondaryNetworkV6Subnet, err := secondaryProviderNetwork.IPv4IPv6Subnets()
			framework.ExpectNoError(err, "must get secondary provider network subnets")
			secondarySubnetStr := secondaryNetworkV4Subnet
			if isIPv6Cluster {
				secondarySubnetStr = secondaryNetworkV6Subnet
			}
			_, secondarySubnet, err := net.ParseCIDR(secondarySubnetStr)
			framework.ExpectNoError(err, "must parse secondary subnet %q", secondarySubnetStr)
			toCurlAddressesSecondary := sets.NewString()
			for i, node := range nodes.Items {
				addrAnnotation, ok := node.Annotations["k8s.ovn.org/host-cidrs"]
				gomega.Expect(ok).To(gomega.BeTrue())
				var allAddrs []string
				var filteredAddrs []string
				err := json.Unmarshal([]byte(addrAnnotation), &allAddrs)
				framework.ExpectNoError(err, "failed to parse node[%s] host-address annotation[%s]", node.Name,
					addrAnnotation)
				for _, addr := range allAddrs {
					addrSplit := strings.Split(addr, "/")
					gomega.Expect(addrSplit).Should(gomega.HaveLen(2))
					// ensure only IPs for the secondary subnet are added
					ip := net.ParseIP(addrSplit[0])
					gomega.Expect(ip).NotTo(gomega.BeNil())
					if !secondarySubnet.Contains(ip) {
						framework.Logf("skipping IP %s because it is not within networks subnet %s", ip, subnet)
						continue // skip if not within the networks subnet
					}
					filteredAddrs = append(filteredAddrs, ip.String())
				}
				toCurlAddressesSecondary.Insert(filteredAddrs...)

				// Calculate and store for AfterEach new target IP addresses.
				if nodeIPs[node.Name] == nil {
					nodeIPs[node.Name] = make(map[int]string)
				}

				if isIPv6Cluster {
					newIPIP := fmt.Sprintf("2001:db8:abcd:1234::%d", i+140)
					nodeIPs[node.Name][6] = newIPIP
				} else {
					newIPIP := fmt.Sprintf("10.10.10.%d", i+140)
					nodeIPs[node.Name][4] = newIPIP
				}
			}

			ginkgo.By("Adding additional IP addresses to each node")
			for nodeName, ipFamilies := range nodeIPs {
				secondaryNetworkInterface, err := infraprovider.Get().GetK8NodeNetworkInterface(nodeName, secondaryProviderNetwork)
				framework.ExpectNoError(err, "must get Node %s network interface %s", nodeName, secondaryProviderNetwork)
				gomega.Expect(secondaryNetworkInterface.InfName).NotTo(gomega.BeEmpty(), "failed to fetch interface name from a k8 node attached to a secondary network")
				for _, ip := range ipFamilies {
					// manually add the a secondary IP to each node
					_, err = infraprovider.Get().ExecK8NodeCommand(nodeName, []string{"ip", "addr", "add", ip, "dev", secondaryNetworkInterface.InfName})
					if err != nil {
						framework.Failf("failed to add new IP address %s to node %s: %v", ip, nodeName, err)
					}
					providerCtx.AddCleanUpFn(func() error {
						_, err = infraprovider.Get().ExecK8NodeCommand(nodeName, []string{"ip", "addr", "del", ip, "dev", secondaryNetworkInterface.InfName})
						if err != nil {
							return fmt.Errorf("failed to del newly assigned node IP address %s to node %s: %v", ip, nodeName, err)
						}
						return nil
					})
					toCurlAddressesSecondary.Insert(ip)
				}
			}

			ginkgo.By("Creating NodePort services")

			etpLocalServiceName := "etp-local-svc"
			etpLocalSvc := nodePortServiceSpecFrom(etpLocalServiceName, v1.IPFamilyPolicyPreferDualStack,
				endpointHTTPPort, endpointUDPPort, clusterHTTPPort, clusterUDPPort, endpointsSelector,
				v1.ServiceExternalTrafficPolicyTypeLocal)
			etpLocalSvc, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(context.Background(), etpLocalSvc,
				metav1.CreateOptions{})
			framework.ExpectNoError(err)

			etpClusterServiceName := "etp-cluster-svc"
			etpClusterSvc := nodePortServiceSpecFrom(etpClusterServiceName, v1.IPFamilyPolicyPreferDualStack,
				endpointHTTPPort, endpointUDPPort, clusterHTTPPort, clusterUDPPort, endpointsSelector,
				v1.ServiceExternalTrafficPolicyTypeCluster)
			etpClusterSvc, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(context.Background(),
				etpClusterSvc, metav1.CreateOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("Waiting for the endpoints to pop up")

			err = framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, f.Namespace.Name,
				etpLocalServiceName, len(endPoints), time.Second, wait.ForeverTestTimeout)
			framework.ExpectNoError(err, "failed to validate endpoints for service %s in namespace: %s",
				etpLocalServiceName, f.Namespace.Name)

			err = framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, f.Namespace.Name,
				etpClusterServiceName, len(endPoints), time.Second, wait.ForeverTestTimeout)
			framework.ExpectNoError(err, "failed to validate endpoints for service %s in namespace: %s",
				etpClusterServiceName, f.Namespace.Name)

			ginkgo.By("Checking connectivity to the external container from egressIP pod " + egressPod.Name + " and verify that the source IP is the secondary NIC egress IP")
			framework.Logf("Destination IPs for external container are ip=%v", serverExternalContainerIP)
			err = wait.PollImmediate(retryInterval, retryTimeout, targetExternalContainerAndTest(serverExternalContainer,
				egressPod.Namespace, egressPod.Name, true, []string{egressIP}))
			framework.ExpectNoError(err, "Check connectivity from pod (%s/%s) to an external container attached to "+
				"a network that is a secondary host network and verify that the src IP is the expected egressIP %s, failed: %v",
				egressPod.Namespace, egressPod.Name, egressIP, err)

			externalSvcClientIPs := sets.NewString(serverExternalContainerIP)
			for _, serviceSpec := range []*v1.Service{etpLocalSvc, etpClusterSvc} {
				tcpNodePort, udpNodePort := nodePortsFromService(serviceSpec)

				for _, protocol := range []string{"http", "udp"} {
					toCurlPort := int32(tcpNodePort)
					if protocol == "udp" {
						toCurlPort = int32(udpNodePort)
					}
					for _, address := range toCurlAddressesSecondary.List() {
						if !isIPv6Cluster && utilnet.IsIPv6String(address) {
							continue
						}
						ginkgo.By("Hitting service " + serviceSpec.Name + " on " + address + " via " + protocol)
						gomega.Eventually(func() bool {
							epHostname := pokeEndpointViaExternalContainer(serverExternalContainer, protocol, address, toCurlPort,
								"hostname")
							// Expect to receive a valid hostname
							return nodesHostnames.Has(epHostname)
						}, "40s", "1s").Should(gomega.BeTrue())
					}
					secondaryNetworkInterface, err := infraprovider.Get().GetK8NodeNetworkInterface(egressNode, secondaryProviderNetwork)
					framework.ExpectNoError(err, "must get secondary network interface information")
					egressNodeIP := secondaryNetworkInterface.IPv4
					if isIPv6Cluster {
						egressNodeIP = secondaryNetworkInterface.IPv6
					}
					gomega.Expect(egressNodeIP).NotTo(gomega.BeEmpty(), "secondary external IP must be available")
					framework.Logf("NodeIP of node %s is %s", egressNode, egressNodeIP)
					ginkgo.By("Hitting service nodeport " + serviceSpec.Name + " on " + egressNodeIP + " via " + protocol)
					// send ingress traffic from external container to egressNode where the pod lives
					// On secondary bridges CI lane we will also created eth1 interface on each node
					// in the cluster. In that case:
					// (1) SGW: npclient's eth1 -> node's eth1-> node's breth1 -> iptables -> DNAT to CIP ->
					//          route to breth0 -> send to OVN -> hit GR; ETP=local will not be respected
					//          in this case and its broken at the moment. (FIXME)
					// (2) LGW: npclient's eth1 -> node's eth1-> node's breth1 -> iptables -> DNAT to .3 masquerade ->
					//          route to mp0 -> send to OVN -> hit switch; ETP=local will be respected
					//          in this case and its delivered to the pod. (test works for this case)
					if !isLocalGWModeEnabled() || serviceSpec.Name != etpLocalServiceName {
						framework.Logf("Mode is shared gateway OR service is ETP=cluster, so skipping srcIP verification")
						continue // cannot verify sourceIP for ETP=local with SGW on secondary interfaces
					}

					// verify srcIP of traffic is that of the external container npclient when for nodeport service type ETP=local
					// we try to hit the backend pod which is on the egressNode
					framework.Logf("%+v", externalSvcClientIPs)
					gomega.Eventually(func() bool {
						epClientIP := pokeEndpointViaExternalContainer(serverExternalContainer, protocol, egressNodeIP, toCurlPort, "clientip") // Returns the request's IP address.
						framework.Logf("Received srcIP: %v", epClientIP)
						IP, _, err := net.SplitHostPort(epClientIP)
						if err != nil {
							return false
						}
						// Expect to receive a valid hostname
						return externalSvcClientIPs.Has(IP)
					}, "40s", "1s").Should(gomega.BeTrue())
				}
			}
		})

		// This tests specific flows required to handle IP fragments towards
		// node port services to avoid forwarding via host in SGW mode. On one
		// side, it is undesireable due to performance considerations. On the
		// other side, it could be problematic if some fragmented packets within
		// a stream are forwarded via host while other non fragmented packets of
		// that same stream are forwarded directly to OVN, as the NATing in both
		// scenarios is different such that OVN could interpret them as
		// different streams and replace what it thinks to be a conflicting port
		// with a different one, breaking the stream for the involved peers.
		ginkgo.It("should handle IP fragments", func() {
			ginkgo.By("Selecting a schedulable node")
			nodes, err = e2enode.GetBoundedReadySchedulableNodes(context.TODO(), f.ClientSet, 1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(nodes.Items)).To(gomega.BeNumerically(">", 0))
			nodeName := nodes.Items[0].Name
			primaryProviderNetwork, err := infraprovider.Get().PrimaryNetwork()
			framework.ExpectNoError(err, "failed to get primary network")
			primaryInf, err := infraprovider.Get().GetK8NodeNetworkInterface(nodeName, primaryProviderNetwork)
			framework.ExpectNoError(err, "failed to get primary interface for node %s", nodeName)
			ginkgo.By("Creating the backend pod")
			args := []string{
				"netexec",
				fmt.Sprintf("--http-port=%d", endpointHTTPPort),
				fmt.Sprintf("--udp-port=%d", endpointUDPPort),
			}
			endpointsSelector := map[string]string{"servicebackend": "true"}

			serverPodName := nodeName + "-ep"
			var serverContainerName string
			_, err = createPod(f, serverPodName, nodeName, f.Namespace.Name, []string{}, endpointsSelector,
				func(p *v1.Pod) {
					p.Spec.Containers[0].Args = args
					serverContainerName = p.Spec.Containers[0].Name
				},
			)
			framework.ExpectNoError(err)

			ginkgo.By("Creating NodePort service")
			serviceName := "service"
			service := nodePortServiceSpecFrom(
				serviceName,
				v1.IPFamilyPolicyPreferDualStack,
				endpointHTTPPort,
				endpointUDPPort,
				clusterHTTPPort,
				clusterUDPPort,
				endpointsSelector,
				v1.ServiceExternalTrafficPolicyTypeCluster,
			)
			service, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(context.Background(), service, metav1.CreateOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("Waiting for the endpoints to pop up")
			err = framework.WaitForServiceEndpointsNum(
				context.TODO(),
				f.ClientSet,
				f.Namespace.Name,
				serviceName,
				1,
				time.Second,
				wait.ForeverTestTimeout,
			)
			framework.ExpectNoError(err)

			ginkgo.By("Creating an external client")
			externalContainer := infraapi.ExternalContainer{Name: clientContainerName, Image: images.AgnHost(), Network: primaryProviderNetwork,
				CmdArgs: []string{"pause"}, ExtPort: infraprovider.Get().GetExternalContainerPort()}
			externalContainer, err = providerCtx.CreateExternalContainer(externalContainer)
			framework.ExpectNoError(err, "failed to create external container", externalContainer)

			clientIP := externalContainer.GetIPv4()
			serverNodeIP := primaryInf.IPv4
			ipContainerCmdPrefix := []string{"ip"}
			if IsIPv6Cluster(f.ClientSet) {
				clientIP = externalContainer.GetIPv6()
				serverNodeIP = primaryInf.IPv6
				ipContainerCmdPrefix = append(ipContainerCmdPrefix, "-6")
			}
			gomega.Expect(clientIP).ShouldNot(gomega.BeEmpty())

			const pmtu = "1300"
			payloads := map[string]string{
				"non-fragmented": "1220",
				"fragmented":     "1320",
			}

			// We set a route MTU towards the server node emulating that PMTUD
			// has already happened resulting in a plausible low PMTU.
			// For UDP the system wide default IP_PMTUDISC_WANT will
			// result in fragmentation for packets bigger than the PMTU.
			// Note that fragmentation could also happen if a client chooses to
			// not use PMTUD with IP_PMTUDISC_DONT both for TCP or UDP but the
			// test setup required to achieve fragmentation without emulating
			// PMTUD is more complex so we stick to UDP.
			ginkgo.By("Lowering PMTU towards the server")
			ipContainerCmd := append(ipContainerCmdPrefix, "route", "add", serverNodeIP, "dev", infraprovider.Get().ExternalContainerPrimaryInterfaceName(), "src", clientIP, "mtu", pmtu)
			_, err = infraprovider.Get().ExecExternalContainerCommand(externalContainer, ipContainerCmd)
			framework.ExpectNoError(err, "lowering MTU in the external kind container failed: %v", err)
			providerCtx.AddCleanUpFn(func() error {
				ipContainerCmd := append(ipContainerCmdPrefix, "route", "del", serverNodeIP, "dev", infraprovider.Get().ExternalContainerPrimaryInterfaceName(), "src", clientIP, "mtu", pmtu)
				_, err = infraprovider.Get().ExecExternalContainerCommand(externalContainer, ipContainerCmd)
				return err
			})
			var udpPort int32
			for _, port := range service.Spec.Ports {
				if port.Protocol == v1.ProtocolUDP {
					udpPort = port.NodePort
				}
			}
			gomega.Expect(udpPort).NotTo(gomega.Equal(0))

			// To check that forwarding did not happen via host, we send a
			// non-fragmented packet first and then a fragmented one on the same
			// source port. If the server sees the same source port it means
			// that both packets were forwarded the same. This is because when
			// forwarding via OVN, GR SNATs from the node IP to the join subnet,
			// whereas forwarding via host, GR SNATs from the masquerade IP to
			// the join subnet. Thus, OVN sees both as different streams and
			// ends up replacing the source port to avoid the collision.
			sourcePortRegex := `to UDP client .*:(?P<Port>\d{1,5})`
			var sourcePort string
			for _, payload := range []string{"non-fragmented", "fragmented"} {
				ginkgo.By(fmt.Sprintf("Sending a %s UDP payload to the service node port", payload))
				payload := fmt.Sprintf("%0"+payloads[payload]+"d", 1)
				containerCmd := fmt.Sprintf("echo 'echo %s' | nc -w2 -u %s %d", payload, serverNodeIP, udpPort)
				if sourcePort != "" {
					containerCmd = fmt.Sprintf("echo 'echo %s' | nc -w2 -u -p %s %s %d", payload, sourcePort, serverNodeIP, udpPort)
				}
				framework.Logf("Running %v", containerCmd)
				stdout, err := infraprovider.Get().ExecExternalContainerCommand(externalContainer, []string{"/bin/sh", "-c", containerCmd})
				framework.ExpectNoError(err, "sending echo request failed: %v", err)

				ginkgo.By("Checking that the service received the request and replied")
				framework.Logf("Server replied with %s", stdout)
				gomega.Expect(stdout).To(gomega.Equal(payload), "server did not reply with the requested payload")

				ginkgo.By("Checking that the request was done on the intended source port")
				matches, err := CaptureContainerOutput(context.TODO(), f.ClientSet, f.Namespace.Name, serverPodName, serverContainerName, sourcePortRegex)
				framework.ExpectNoError(err)
				gomega.Expect(matches).To(gomega.HaveKey("Port"))
				gomega.Expect(matches["Port"]).ToNot(gomega.BeEmpty())
				if sourcePort != "" {
					gomega.Expect(matches).To(gomega.HaveKeyWithValue("Port", sourcePort), "request did not use the intended source port")
				}
				sourcePort = matches["Port"]
			}
		})
	})
})

func getServiceBackendsFromPod(execPod *v1.Pod, serviceIP string, servicePort int) []string {
	connectionAttempts := 15
	serviceIPPort := net.JoinHostPort(serviceIP, strconv.Itoa(servicePort))
	curl := fmt.Sprintf(`curl -q -s --connect-timeout 2 http://%s/`, serviceIPPort)
	cmd := fmt.Sprintf("for i in $(seq 1 %d); do echo; %s ; done", connectionAttempts, curl)

	stdout, err := e2eoutput.RunHostCmd(execPod.Namespace, execPod.Name, cmd)
	if err != nil {
		framework.Logf("Failed to get response from %s. Retry until timeout", serviceIPPort)
		return nil
	}
	hosts := strings.Split(stdout, "\n")
	nonEmptyHosts := []string{}
	for _, host := range hosts {
		if len(host) > 0 {
			nonEmptyHosts = append(nonEmptyHosts, strings.TrimSpace(host))
		}
	}
	gomega.Expect(len(nonEmptyHosts)).To(gomega.Equal(connectionAttempts), fmt.Sprintf("Expected %v replies, got %v", connectionAttempts, nonEmptyHosts))
	return nonEmptyHosts
}

// This test ensures that - when a pod that's a backend for a service curls the
// service ip; if the traffic was DNAT-ed to the same src pod (hairpin/loopback case) -
// the srcIP of reply traffic is SNATed to the special masqurade IP 169.254.0.5
// or "fd69::5"
var _ = ginkgo.Describe("Service Hairpin SNAT", feature.Service, func() {
	const (
		svcName                        = "service-hairpin-test"
		backendName                    = "hairpin-backend-pod"
		endpointHTTPPort        uint16 = 80
		serviceHTTPPort         uint16 = 6666
		V4LBHairpinMasqueradeIP        = "169.254.0.5"
		V6LBHairpinMasqueradeIP        = "fd69::5"
	)

	var (
		svcIP           string
		isIpv6          bool
		namespaceName   string
		backendNodeName string
		nodeIP          string
	)

	f := wrappedTestFramework(svcName)
	hairpinPodSel := map[string]string{"hairpinbackend": "true"}

	ginkgo.BeforeEach(func() {
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(context.TODO(), f.ClientSet, 2)
		framework.ExpectNoError(err)
		if len(nodes.Items) < 2 {
			framework.Failf("Test requires >= 2 Ready nodes, but there are only %v nodes", len(nodes.Items))
		}
		ips := e2enode.CollectAddresses(nodes, v1.NodeInternalIP)
		namespaceName = f.Namespace.Name
		backendNodeName = nodes.Items[0].Name
		nodeIP = ips[1]
	})

	ginkgo.It("Should ensure service hairpin traffic is SNATed to hairpin masquerade IP; Switch LB", func() {

		ginkgo.By("creating an ovn-network backend pod")
		_, err := createGenericPodWithLabel(f, backendName, backendNodeName, namespaceName, getAgnHostHTTPPortBindFullCMD(endpointHTTPPort), hairpinPodSel)
		framework.ExpectNoError(err, fmt.Sprintf("unable to create backend pod: %s, err: %v", backendName, err))

		ginkgo.By("creating a TCP service service-for-pods with type=ClusterIP in namespace " + namespaceName)

		svcIP, err = createServiceForPodsWithLabel(f, namespaceName, serviceHTTPPort, endpointHTTPPort, "ClusterIP", hairpinPodSel)
		framework.ExpectNoError(err, fmt.Sprintf("unable to create service: service-for-pods, err: %v", err))

		err = framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, namespaceName, "service-for-pods", 1, time.Second, wait.ForeverTestTimeout)
		framework.ExpectNoError(err, fmt.Sprintf("service: service-for-pods never had an endpoint, err: %v", err))

		ginkgo.By("by sending a TCP packet to service service-for-pods with type=ClusterIP in namespace " + namespaceName + " from backend pod " + backendName)

		if utilnet.IsIPv6String(svcIP) {
			framework.Logf("service: service-for-pods is ipv6")
			isIpv6 = true
		}

		clientIP := pokeEndpointViaPod(f, namespaceName, backendName, svcIP, serviceHTTPPort, "clientip")
		clientIP, _, err = net.SplitHostPort(clientIP)
		framework.ExpectNoError(err, "failed to parse client ip:port")

		if isIpv6 {
			gomega.Expect(clientIP).To(gomega.Equal(V6LBHairpinMasqueradeIP), fmt.Sprintf("returned client ipv6: %v was not correct", clientIP))
		} else {
			gomega.Expect(clientIP).To(gomega.Equal(V4LBHairpinMasqueradeIP), fmt.Sprintf("returned client ipv4: %v was not correct", clientIP))
		}
	})

	ginkgo.It("Should ensure service hairpin traffic is NOT SNATed to hairpin masquerade IP; GR LB", func() {

		ginkgo.By("creating an host-network backend pod on " + backendNodeName)
		// create hostNeworkedPods
		hostNetPort := infraprovider.Get().GetK8HostPort()
		_, err := createPod(f, backendName, backendNodeName, namespaceName, []string{}, hairpinPodSel, func(p *v1.Pod) {
			p.Spec.Containers[0].Command = getAgnHostHTTPPortBindFullCMD(hostNetPort)
			p.Spec.HostNetwork = true
		})
		framework.ExpectNoError(err, fmt.Sprintf("unable to create backend pod: %s, err: %v", backendName, err))

		ginkgo.By("creating a TCP service service-for-pods with type=NodePort in namespace " + namespaceName)

		svcIP, err = createServiceForPodsWithLabel(f, namespaceName, serviceHTTPPort, hostNetPort, "NodePort", hairpinPodSel)
		framework.ExpectNoError(err, fmt.Sprintf("unable to create service: service-for-pods, err: %v", err))

		err = framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, namespaceName, "service-for-pods", 1, time.Second, wait.ForeverTestTimeout)
		framework.ExpectNoError(err, fmt.Sprintf("service: service-for-pods never had an endpoint, err: %v", err))

		svc, err := f.ClientSet.CoreV1().Services(namespaceName).Get(context.TODO(), "service-for-pods", metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to fetch service: service-for-pods")

		ginkgo.By("by sending a TCP packet to service service-for-pods with type=NodePort(" + nodeIP + ":" + fmt.Sprint(svc.Spec.Ports[0].NodePort) + ") in namespace " + namespaceName + " from node " + backendNodeName)

		clientIP := pokeEndpointViaNode(backendNodeName, "http", nodeIP, hostNetPort, uint16(svc.Spec.Ports[0].NodePort), "clientip")
		clientIP, _, err = net.SplitHostPort(clientIP)
		framework.ExpectNoError(err, "failed to parse client ip:port")

		gomega.Expect(clientIP).To(gomega.Equal(nodeIP), fmt.Sprintf("returned client: %v was not correct", clientIP))
	})

})

var _ = ginkgo.Describe("Load Balancer Service Tests with MetalLB", feature.Service, func() {

	const (
		svcName                     = "lbservice-test"
		backendName                 = "lb-backend-pod"
		endpointHTTPPort            = 80
		endpointUDPPort             = 10001
		loadBalancerYaml            = "loadbalancer.yaml"
		bgpAddYaml                  = "bgpAdd.yaml"
		bgpEmptyYaml                = "bgpEmptyAdd.yaml"
		externalClientContainerName = "lbclient"
		externalRouterContainerName = "frr"
	)

	var (
		backendNodeName    string
		nonBackendNodeName string
		namespaceName      = "default"
	)
	f := wrappedTestFramework(svcName)
	ginkgo.BeforeEach(func() {
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(context.TODO(), f.ClientSet, 2)
		framework.ExpectNoError(err)
		if len(nodes.Items) < 2 {
			framework.Failf("Test requires >= 2 Ready nodes, but there are only %v nodes", len(nodes.Items))
		}
		backendNodeName = nodes.Items[0].Name
		nonBackendNodeName = nodes.Items[1].Name
		var loadBalancerServiceConfig = fmt.Sprintf(`
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: dynamic-claim
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: standard
  resources:
    requests:
      storage: 1000Mi

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ` + backendName + `
spec:
  replicas: 4
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      volumes:
        - name: data
          persistentVolumeClaim:
           claimName: dynamic-claim
      initContainers:
      - name: get-big-file
        image: quay.io/itssurya/dev-images:metallb-lbservice
        command: ['sh', '-c', "dd if=/dev/zero of=/usr/share/nginx/html/big.iso  bs=1024 count=0 seek=102400"]
        volumeMounts:
        - name: data
          mountPath: "/usr/share/nginx/html"
      containers:
      - name: nginx
        image: nginx:1
        volumeMounts:
        - name: data
          mountPath: "/usr/share/nginx/html"
        ports:
        - name: http
          containerPort: 80
      - name: udp-server
        image: quay.io/itssurya/dev-images:udp-server-srcip-printer
        imagePullPolicy: Always
        ports:
        - containerPort: 10001
          protocol: UDP
          name: udp
      nodeSelector:
        kubernetes.io/hostname: ` + backendNodeName + `

---
apiVersion: v1
kind: Service
metadata:
  name: ` + svcName + `
spec:
  ports:
  - name: http
    port: 80
    protocol: TCP
    targetPort: 80
  - name: udp
    port: 10001
    protocol: UDP
    targetPort: 10001
  selector:
    app: nginx
  type: LoadBalancer
`)
		if err := os.WriteFile(loadBalancerYaml, []byte(loadBalancerServiceConfig), 0644); err != nil {
			framework.Failf("Unable to write CRD config to disk: %v", err)
		}
		framework.Logf("Create the Load Balancer configuration")
		e2ekubectl.RunKubectlOrDie("default", "create", "-f", loadBalancerYaml)

	})

	ginkgo.JustAfterEach(func() {
		if ginkgo.CurrentSpecReport().Failed() {
			DumpBGPInfo(reportPath, ginkgo.CurrentSpecReport().LeafNodeText, f)
			k8sReporter := InitReporter(framework.TestContext.KubeConfig, reportPath,
				[]string{metallbNamespace, namespaceName})
			DumpInfo(k8sReporter)
		}
	})

	ginkgo.AfterEach(func() {
		framework.Logf("Delete the Load Balancer configuration")
		e2ekubectl.RunKubectlOrDie("default", "delete", "-f", loadBalancerYaml, "--ignore-not-found=true")
		defer func() {
			if err := os.Remove(loadBalancerYaml); err != nil {
				framework.Logf("Unable to remove the CRD config from disk: %v", err)
			}
			framework.Logf("Reset MTU on intermediary router to allow large packets")
			primaryProviderNetwork, err := infraprovider.Get().PrimaryNetwork()
			framework.ExpectNoError(err, "must get primary provider network")
			externalContainer := infraapi.ExternalContainer{Name: externalRouterContainerName, Network: primaryProviderNetwork}
			_, err = infraprovider.Get().ExecExternalContainerCommand(externalContainer, []string{"ip", "link", "set", "mtu", "1500", "dev", "eth1"})
			framework.ExpectNoError(err, "failed to reset MTU on intermediary router")
			framework.Logf("Delete the custom BGP Advertisement configuration")
			e2ekubectl.RunKubectlOrDie("metallb-system", "delete", "bgpadvertisement", "example", "--ignore-not-found=true")
			var bgpEmptyConfig = fmt.Sprintf(`
---
apiVersion: metallb.io/v1beta1
kind: BGPAdvertisement
metadata:
  name: empty
  namespace: metallb-system
`)
			if err := os.WriteFile(bgpEmptyYaml, []byte(bgpEmptyConfig), 0644); err != nil {
				framework.Failf("Unable to write CRD config to disk: %v", err)
			}
			framework.Logf("Re-create the default BGP Advertisement configuration for other tests")
			e2ekubectl.RunKubectlOrDie("metallb-system", "apply", "-f", bgpEmptyYaml)
		}()
		e2ekubectl.RunKubectlOrDie("default", "delete", "eip", "egressip", "--ignore-not-found=true")
		e2ekubectl.RunKubectlOrDie("default", "label", "node", nonBackendNodeName, "k8s.ovn.org/egress-assignable-")
	})

	ginkgo.It("Should ensure connectivity works on an external service when mtu changes in intermediate node", func() {
		err := framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, namespaceName, svcName, 4, time.Second, time.Second*180)
		framework.ExpectNoError(err, fmt.Sprintf("service: %s never had an endpoint, err: %v", svcName, err))

		time.Sleep(time.Second * 5) // buffer to ensure all rules are created correctly

		svcLoadBalancerIP, err := getServiceLoadBalancerIP(f.ClientSet, namespaceName, svcName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get service lb ip: %s, err: %v", svcName, err))

		endpoints, err := getEndpointsForService(f.ClientSet, namespaceName, svcName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get endpoints for service %s", svcName))
		gomega.Expect(endpoints).NotTo(gomega.BeNil())
		gomega.Expect(len(endpoints.Subsets)).To(gomega.Equal(1))
		gomega.Expect(len(endpoints.Subsets[0].Addresses)).To(gomega.Equal(4))
		endPointIP := endpoints.Subsets[0].Addresses[0].IP
		nodeName := endpoints.Subsets[0].Addresses[0].NodeName
		nodeIP, err := getNodeIP(f.ClientSet, *nodeName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get endpoint's %s node ip address", endPointIP))

		svcIPforCurl := svcLoadBalancerIP
		if !utilnet.IsIPv6String(svcLoadBalancerIP) {
			ginkgo.By("Setting up external IPv4 client with an intermediate node")
			defer func() {
				cleanupIPv4NetworkForExternalClient(svcLoadBalancerIP, endpointHTTPPort)
			}()
			setupIPv4NetworkForExternalClient(svcLoadBalancerIP, endpointHTTPPort, nodeIP)
		} else {
			ginkgo.By("Setting up external IPv6 client with an intermediate node")
			defer func() {
				cleanupIPv6NetworkForExternalClient(svcLoadBalancerIP, endpointHTTPPort)
			}()
			setupIPv6NetworkForExternalClient(svcLoadBalancerIP, endpointHTTPPort, nodeIP)
			svcIPforCurl = fmt.Sprintf("[%s]", svcLoadBalancerIP)
		}

		ginkgo.By("Test sevice connectivity before changing MTU on the intermediate node")
		// Ensure service connectivity works from external client with default settings.
		// Use Eventually because IPv6 takes a while to finish its network configuration
		// with network namespaces.
		gomega.Eventually(func() error {
			return buildAndRunCommand(fmt.Sprintf("sudo ip netns exec client curl %s:%d/big.iso -o big.iso", svcIPforCurl, endpointHTTPPort))
		}, 5*time.Second).Should(gomega.BeNil(), "failed to connect with external load balancer service")

		// Change MTU size of vmtobridge veth pair and verify service connectivity still works.
		// Set the value >=1280 so that it works for IPv6 as well.
		err = buildAndRunCommand("sudo ip link set vmtobridge mtu 1280")
		framework.ExpectNoError(err, "failed to change mtu size on vmtobridge gateway interface")
		err = buildAndRunCommand("sudo ip netns exec bridge ip link set bridgetovm up mtu 1280")
		framework.ExpectNoError(err, "failed to change mtu size on bridgetovm interface")
		ginkgo.By("Test sevice connectivity after changing MTU on the intermediate node")
		gomega.Eventually(func() error {
			return buildAndRunCommand(fmt.Sprintf("sudo ip netns exec client curl %s:%d/big.iso -o big.iso", svcIPforCurl, endpointHTTPPort))
		}, 5*time.Second).Should(gomega.BeNil(), "failed to connect with external load balancer service after changing mtu size")
	})

	ginkgo.It("Should ensure load balancer service works with pmtud", func() {

		// TEST LOGIC: This test uses metaLB BGP for advertising routes towards the 3 KIND ovnk cluster nodes
		// (control-plane, 2 workers) as potential candidates to reach the load balancer service (192.168.10.0 service VIP).
		// There is also a FRR router that sits in front of the KIND cluster through which traffic flows to the service VIP.
		// External client (name: lbclient container {installation details in kind.sh script}) tries to reach a
		// load balancer service with VIP: 192.168.10.0 that has 4 backends running on one of the nodes in the cluster through
		// the FRR router.
		// -----------------       ------------------      VIP: 192.168.10.0  ---------------------
		// |               | 1500  |                | 1500                    | ovn-control-plane |
		// |   lbclient    |------>|   FRR router   |------> KIND cluster --> ---------------------
		// |               | change|                |                         |    ovn-worker     |   (4 backend CNI pods running
		// -----------------  to   ------------------                         ---------------------    on one of the nodes serving
		//                   1200                                             |    ovn-worker2    |    lb service)
		//              generates ICMP                                        ---------------------
		//                 needs FRAG
		//
		// NOTE: There is no guarantee which node gets picked for serving the request since metalLB uses ECMP.
		// Hence we could either have:
		// A) lbclient->FRR router->ovn-worker->br-ex->GR_ovn-worker->join->cluster-router->ovn-worker-switch->pod OR
		// B) lbclient->FRR router->ovn-worker2->br-ex->GR_ovn-worker2->join->cluster-router-ovn-worker->transit-switch->GENEVE->
		//    transit-switch->cluster-router-ovn-worker->ovn-worker-switch->pod
		// depending on which node is hit for the service traffic and which node the backendpod lives on.
		err := framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, namespaceName, svcName, 4, time.Second, time.Second*120)
		framework.ExpectNoError(err, fmt.Sprintf("service: %s never had an endpoint, err: %v", svcName, err))

		time.Sleep(time.Second * 5) // buffer to ensure all rules are created correctly

		svcLoadBalancerIP, err := getServiceLoadBalancerIP(f.ClientSet, namespaceName, svcName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get service lb ip: %s, err: %v", svcName, err))

		numberOfETPRules := pokeNodeIPTableRules(backendNodeName, "OVN-KUBE-EXTERNALIP")
		gomega.Expect(numberOfETPRules).To(gomega.Equal(5))

		// curl the LB service from the client container to trigger BGP route advertisement
		ginkgo.By("by sending a TCP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName)
		primaryProviderNetwork, err := infraprovider.Get().PrimaryNetwork()
		framework.ExpectNoError(err, "must fetch primary provider network")
		externalContainer := infraapi.ExternalContainer{Name: "lbclient", Network: primaryProviderNetwork} // pre-created
		_, err = wgetInExternalContainer(externalContainer, svcLoadBalancerIP, endpointHTTPPort, "big.iso", 120)
		framework.ExpectNoError(err, "failed to curl load balancer service")

		ginkgo.By("all 3 nodeIP routes are advertised correctly by metalb BGP routes")
		// sample
		// 192.168.10.0 nhid 84 proto bgp metric 20
		//	nexthop via 172.19.0.3 dev eth0 weight 1
		//	nexthop via 172.19.0.4 dev eth0 weight 1
		//	nexthop via 172.19.0.2 dev eth0 weight 1

		cmd := []string{}
		ipVer := ""
		if utilnet.IsIPv6String(svcLoadBalancerIP) {
			ipVer = " -6"
		}
		bgpRouteCommand := strings.Split(fmt.Sprintf("ip%s route show %s", ipVer, svcLoadBalancerIP), " ")
		cmd = append(cmd, bgpRouteCommand...)

		backendNodeIP, err := getNodeIP(f.ClientSet, backendNodeName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get node's %s node ip address", backendNodeName))
		nonBackendNodeIP, err := getNodeIP(f.ClientSet, backendNodeName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get node's %s node ip address", backendNodeName))
		frrExternalContainer := infraapi.ExternalContainer{Name: "frr", Network: primaryProviderNetwork}
		gomega.Eventually(func() bool {
			routes, err := infraprovider.Get().ExecExternalContainerCommand(frrExternalContainer, cmd)
			framework.ExpectNoError(err, "failed to get BGP routes from intermediary router")
			framework.Logf("Routes in FRR %s", routes)
			return strings.Contains(routes, backendNodeIP)
		}, 30*time.Second).Should(gomega.BeTrue())
		gomega.Eventually(func() bool {
			routes, err := infraprovider.Get().ExecExternalContainerCommand(frrExternalContainer, cmd)
			framework.ExpectNoError(err, "failed to get BGP routes from intermediary router")
			framework.Logf("Routes in FRR %s", routes)
			return strings.Contains(routes, nonBackendNodeIP)
		}, 30*time.Second).Should(gomega.BeTrue())

		framework.Logf("Delete the default BGP Advertisement configuration")
		e2ekubectl.RunKubectlOrDie("metallb-system", "delete", "bgpadvertisement", "empty", "--ignore-not-found=true")

		// test CASE A: traffic lands on the same node where backend lives
		// test CASE B: traffic lands on different node than where the backend lives
		for _, node := range []string{backendNodeName, nonBackendNodeName} {
			var bgpConfig = fmt.Sprintf(`
---
apiVersion: metallb.io/v1beta1
kind: BGPAdvertisement
metadata:
  name: example
  namespace: metallb-system
spec:
  ipAddressPools:
  - dev-env-bgp
  nodeSelectors:
  - matchLabels:
      kubernetes.io/hostname: ` + node + `
`)
			if err := os.WriteFile(bgpAddYaml, []byte(bgpConfig), 0644); err != nil {
				framework.Failf("Unable to write CRD config to disk: %v", err)
			}
			framework.Logf("Create the BGP Advertisement configuration")
			e2ekubectl.RunKubectlOrDie("metallb-system", "apply", "-f", bgpAddYaml)

			ginkgo.By("only 1 nodeIP route is advertised correctly by metalb BGP routes")
			// ensure only this node's IP route is advertised correctly by metalb BGP routes
			// sample:
			// 192.168.10.0 nhid 31 via 172.19.0.4 dev eth0 proto bgp metric 20
			nodeIP, err := getNodeIP(f.ClientSet, node)
			framework.ExpectNoError(err, fmt.Sprintf("failed to get nodes's %s node ip address", node))
			framework.Logf("NodeIP of node %s is %s", node, nodeIP)
			externalContainer := infraapi.ExternalContainer{Name: externalRouterContainerName, Network: primaryProviderNetwork}
			cmd := []string{}
			cmd = append(cmd, bgpRouteCommand...)
			gomega.Eventually(func() bool {
				routes, err := infraprovider.Get().ExecExternalContainerCommand(externalContainer, cmd)
				framework.ExpectNoError(err, "failed to get BGP routes from intermediary router")
				framework.Logf("Routes in FRR %s", routes)
				routeCount := 0
				matchedRoute := ""
				for _, route := range strings.Split(routes, "\n") {
					match := strings.Contains(route, nodeIP)
					if match {
						framework.Logf("DEBUG: Matched route %s for pattern %s", route, nodeIP)
						matchedRoute = route
					}
					if strings.Contains(route, "via") {
						routeCount++
					}
				}
				return routeCount == 1 && strings.Contains(matchedRoute, nodeIP)
			}, 60*time.Second).Should(gomega.BeTrue())

			ginkgo.By("by sending a TCP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName + " via node " + node)

			_, err = wgetInExternalContainer(externalContainer, svcLoadBalancerIP, endpointHTTPPort, "big.iso", 120)
			framework.ExpectNoError(err, "failed to curl load balancer service")

			ginkgo.By("change MTU on intermediary router to force icmp related packets")
			cmd = []string{}
			mtuCommand := strings.Split("ip link set mtu 1280 dev eth1", " ")
			cmd = append(cmd, mtuCommand...)
			_, err = infraprovider.Get().ExecExternalContainerCommand(externalContainer, cmd)
			framework.ExpectNoError(err, "failed to change MTU on intermediary router")

			time.Sleep(time.Second * 5) // buffer to ensure MTU change took effect

			ginkgo.By("by sending a TCP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName + " via node " + node)

			_, err = wgetInExternalContainer(externalContainer, svcLoadBalancerIP, endpointHTTPPort, "big.iso", 120)
			framework.ExpectNoError(err, "failed to curl load balancer service")

			ginkgo.By("reset MTU on intermediary router to allow large packets")
			cmd = []string{}
			mtuCommand = strings.Split("ip link set mtu 1500 dev eth1", " ")
			cmd = append(cmd, mtuCommand...)
			_, err = infraprovider.Get().ExecExternalContainerCommand(externalContainer, cmd)
			framework.ExpectNoError(err, "failed to reset MTU on intermediary router")
		}
	})

	ginkgo.It("Should ensure load balancer service works with 0 node ports when ETP=local", func() {

		err := framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, namespaceName, svcName, 4, time.Second, time.Second*120)
		framework.ExpectNoError(err, fmt.Sprintf("service: %s never had an enpoint, err: %v", svcName, err))

		svcLoadBalancerIP, err := getServiceLoadBalancerIP(f.ClientSet, namespaceName, svcName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get service lb ip: %s, err: %v", svcName, err))

		time.Sleep(time.Second * 5) // buffer to ensure all rules are created correctly

		checkNumberOfETPRules := func(value int, pattern string) wait.ConditionFunc {
			return func() (bool, error) {
				numberOfETPRules := pokeNodeIPTableRules(backendNodeName, pattern)
				return (numberOfETPRules == value), nil
			}
		}
		checkNumberOfNFTElements := func(value int, name string) wait.ConditionFunc {
			return func() (bool, error) {
				numberOfNFTElements := countNFTablesElements(backendNodeName, name)
				return (numberOfNFTElements == value), nil
			}
		}
		noSNATServicesSet := "mgmtport-no-snat-services-v4"
		if utilnet.IsIPv6String(svcLoadBalancerIP) {
			noSNATServicesSet = "mgmtport-no-snat-services-v6"
		}

		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfETPRules(2, "OVN-KUBE-ETP"))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of iptable rules, err: %v", err)
		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfETPRules(5, "OVN-KUBE-EXTERNALIP"))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of iptable rules, err: %v", err)
		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfNFTElements(0, noSNATServicesSet))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of nftables elements, err: %v", err)
		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfNFTElements(0, "mgmtport-no-snat-nodeports"))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of nftables elements, err: %v", err)

		ginkgo.By("by sending a TCP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName)
		externalContainer := infraapi.ExternalContainer{Name: externalClientContainerName}
		_, err = wgetInExternalContainer(externalContainer, svcLoadBalancerIP, endpointHTTPPort, "big.iso", 120)
		framework.ExpectNoError(err, "failed to curl load balancer service")

		ginkgo.By("patching service " + svcName + " to allocateLoadBalancerNodePorts=false and externalTrafficPolicy=local")

		err = patchServiceBoolValue(f.ClientSet, svcName, "default", "/spec/allocateLoadBalancerNodePorts", false)
		framework.ExpectNoError(err)

		output := e2ekubectl.RunKubectlOrDie("default", "get", "svc", svcName, "-o=jsonpath='{.spec.allocateLoadBalancerNodePorts}'")
		gomega.Expect(output).To(gomega.Equal("'false'"))

		err = patchServiceStringValue(f.ClientSet, svcName, "default", "/spec/externalTrafficPolicy", "Local")
		framework.ExpectNoError(err)

		output = e2ekubectl.RunKubectlOrDie("default", "get", "svc", svcName, "-o=jsonpath='{.spec.externalTrafficPolicy}'")
		gomega.Expect(output).To(gomega.Equal("'Local'"))

		time.Sleep(time.Second * 5) // buffer to ensure all rules are created correctly

		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfETPRules(10, "OVN-KUBE-ETP"))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of iptable rules, err: %v", err)
		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfNFTElements(8, noSNATServicesSet))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of nftables elements, err: %v", err)
		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfNFTElements(0, "mgmtport-no-snat-nodeports"))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of nftables elements, err: %v", err)

		ginkgo.By("by sending a TCP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName)

		_, err = wgetInExternalContainer(externalContainer, svcLoadBalancerIP, endpointHTTPPort, "big.iso", 120)
		framework.ExpectNoError(err, "failed to curl load balancer service")

		pktSize := 60
		if utilnet.IsIPv6String(svcLoadBalancerIP) {
			pktSize = 80
		}
		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfETPRules(1, fmt.Sprintf("[1:%d] -A OVN-KUBE-ETP", pktSize)))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of iptable rules, err: %v", err)
		// FIXME: This used to check that the no-snat rule had been hit, but nftables
		// doesn't attach counters to rules unless you explicitly request them, which
		// we don't... Is this check really needed?

		ginkgo.By("Scale down endpoints of service: " + svcName + " to ensure iptable rules are also getting recreated correctly")
		e2ekubectl.RunKubectlOrDie("default", "scale", "deployment", backendName, "--replicas=3")
		err = framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, namespaceName, svcName, 3, time.Second, time.Second*120)
		framework.ExpectNoError(err, fmt.Sprintf("service: %s never had an endpoint, err: %v", svcName, err))
		time.Sleep(time.Second * 5) // buffer to ensure all rules are created correctly

		// number of rules/elements should have decreased by 2 (one for the TCP port,
		// one for UDP)
		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfETPRules(8, "OVN-KUBE-ETP"))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of iptable rules, err: %v", err)
		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfNFTElements(6, noSNATServicesSet))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of nftables elements, err: %v", err)
		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfNFTElements(0, "mgmtport-no-snat-nodeports"))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of nftables elements, err: %v", err)

		ginkgo.By("by sending a TCP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName)

		_, err = wgetInExternalContainer(externalContainer, svcLoadBalancerIP, endpointHTTPPort, "big.iso", 120)
		framework.ExpectNoError(err, "failed to curl load balancer service")

		err = wait.PollImmediate(retryInterval, retryTimeout, checkNumberOfETPRules(1, fmt.Sprintf("[1:%d] -A OVN-KUBE-ETP", pktSize)))
		framework.ExpectNoError(err, "Couldn't fetch the correct number of iptable rules, err: %v", err)
		// FIXME: This used to check that the no-snat rule had been hit, but nftables
		// doesn't attach counters to rules unless you explicitly request them, which
		// we don't... Is this check really needed?
	})

	ginkgo.It("Should ensure load balancer service works when ETP=local and session affinity is set", func() {

		err := framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, namespaceName, svcName, 4, time.Second, time.Second*120)
		framework.ExpectNoError(err, fmt.Sprintf("service: %s never had an enpoint, err: %v", svcName, err))

		svcLoadBalancerIP, err := getServiceLoadBalancerIP(f.ClientSet, namespaceName, svcName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get service lb ip: %s, err: %v", svcName, err))

		ginkgo.By("patching service " + svcName + " to externalTrafficPolicy=local")
		err = patchServiceStringValue(f.ClientSet, svcName, "default", "/spec/externalTrafficPolicy", "Local")
		framework.ExpectNoError(err)
		output := e2ekubectl.RunKubectlOrDie("default", "get", "svc", svcName, "-o=jsonpath='{.spec.externalTrafficPolicy}'")
		gomega.Expect(output).To(gomega.Equal("'Local'"))
		time.Sleep(time.Second * 5) // buffer to ensure all rules are created correctly

		ginkgo.By("1 nodeIP route is advertised correctly by metalb BGP routes")
		// since ETP=local; ensure only this node's IP route is advertised correctly by metalb BGP routes
		// sample:
		// 192.168.10.0 nhid 31 via 172.19.0.4 dev eth0 proto bgp metric 20
		nodeIP, err := getNodeIP(f.ClientSet, backendNodeName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get nodes's %s node ip address", backendNodeName))
		framework.Logf("NodeIP of node %s is %s", backendNodeName, nodeIP)
		primaryProviderNetwork, err := infraprovider.Get().PrimaryNetwork()
		framework.ExpectNoError(err, "must get primary provider network")
		frrExternalContainer := infraapi.ExternalContainer{Name: externalRouterContainerName, Network: primaryProviderNetwork}
		cmd := []string{}
		ipVer := ""
		if utilnet.IsIPv6String(svcLoadBalancerIP) {
			ipVer = " -6"
		}
		bgpRouteCommand := strings.Split(fmt.Sprintf("ip%s route show %s", ipVer, svcLoadBalancerIP), " ")
		cmd = append(cmd, bgpRouteCommand...)

		gomega.Eventually(func() bool {
			routes, err := infraprovider.Get().ExecExternalContainerCommand(frrExternalContainer, cmd)
			framework.ExpectNoError(err, "failed to get BGP routes from intermediary router")
			framework.Logf("Routes in FRR %s", routes)
			routeCount := 0
			matchedRoute := ""
			for _, route := range strings.Split(routes, "\n") {
				match := strings.Contains(route, nodeIP)
				if match {
					framework.Logf("DEBUG: Matched route %s for pattern %s", route, nodeIP)
					matchedRoute = route
				}
				if strings.Contains(route, "via") {
					routeCount++
				}
			}
			return routeCount == 1 && strings.Contains(matchedRoute, nodeIP)
		}, 60*time.Second).Should(gomega.BeTrue())

		ginkgo.By("by sending a UDP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName)
		netcatCmd := fmt.Sprintf("echo hostname | nc -uv -w2 %s %d",
			svcLoadBalancerIP,
			endpointUDPPort,
		)
		cmd = []string{"bash", "-x", "-c", netcatCmd}
		framework.Logf("netcat command %s", cmd)
		clientNetNetwork, err := infraprovider.Get().GetNetwork("clientnet")
		if errors.Is(err, infraapi.NotFound) {
			ginkgo.Skip("clientnet network is not available. Skipping")
		}
		framework.ExpectNoError(err, "clientnet network must be available")
		lbClientExternalContainer := infraapi.ExternalContainer{Name: "lbclient", Network: clientNetNetwork}
		output, err = infraprovider.Get().ExecExternalContainerCommand(lbClientExternalContainer, cmd)
		framework.ExpectNoError(err, "failed to connect to load balancer service")
		framework.Logf("netcat command output %s", output)

		ginkgo.By("ensure the sourceIP of the external container is preserved!")
		// Check that sourceIP of the LBService is preserved
		targetPodLogs, err := e2ekubectl.RunKubectl("default", "logs", "-l", "app=nginx", "--container", "udp-server")
		framework.ExpectNoError(err, "failed to inspect logs in backend pods")
		framework.Logf("Target pod logs (nginx): %q", targetPodLogs)
		lbClientInf, err := infraprovider.Get().GetExternalContainerNetworkInterface(lbClientExternalContainer, clientNetNetwork)
		framework.ExpectNoError(err, "failed to get network interface info for network %s within lbclient external container %s", clientNetNetwork.Name(), frrExternalContainer.GetName())
		framework.Logf("found external container IPv4: %q, IPv6: %q", lbClientInf.IPv4, lbClientInf.IPv6)
		if strings.Contains(targetPodLogs, lbClientInf.IPv4) {
			framework.Logf("found the expected srcIP %s!", lbClientInf.IPv4)
		} else if strings.Contains(targetPodLogs, lbClientInf.IPv6) {
			framework.Logf("found the expected srcIP %s!", lbClientInf.IPv6)
		} else {
			framework.Failf("could not get expected srcIP!, target pod logs:\n%q", targetPodLogs)
		}

		ginkgo.By("patching service " + svcName + " to sessionAffinity=ClientIP at default timeout of 10800")
		err = patchServiceStringValue(f.ClientSet, svcName, "default", "/spec/sessionAffinity", "ClientIP")
		framework.ExpectNoError(err)
		output = e2ekubectl.RunKubectlOrDie("default", "get", "svc", svcName, "-o=jsonpath='{.spec.sessionAffinity}'")
		gomega.Expect(output).To(gomega.Equal("'ClientIP'"))
		output = e2ekubectl.RunKubectlOrDie("default", "get", "svc", svcName, "-o=jsonpath='{.spec.sessionAffinityConfig.clientIP.timeoutSeconds}'")
		gomega.Expect(output).To(gomega.Equal("'10800'"))
		time.Sleep(time.Second * 5) // buffer to ensure all rules are created correctly

		ginkgo.By("by sending a UDP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName)
		// OVN drops the 1st packet so this one does nothing basically.
		// See https://issues.redhat.com/browse/FDP-223 for details
		output, err = infraprovider.Get().ExecExternalContainerCommand(lbClientExternalContainer, cmd)
		framework.ExpectNoError(err, "failed to connect to load balancer service")
		framework.Logf("netcat command output %s", output)
		time.Sleep(time.Second * 10) // buffer to ensure all learn flows are created correctly after the previous drop

		// OVN drops the 1st packet so let's be sure to another set of netcat connections at least to check the srcIP
		output, err = infraprovider.Get().ExecExternalContainerCommand(lbClientExternalContainer, cmd)
		framework.ExpectNoError(err, "failed to connect to load balancer service")
		framework.Logf("netcat command output %s", output)

		// Check that sourceIP of the LBService is preserved
		ginkgo.By("ensure the sourceIP of the external container is preserved!")
		targetPodLogs, err = e2ekubectl.RunKubectl("default", "logs", "-l", "app=nginx", "--container", "udp-server")
		framework.ExpectNoError(err, "failed to inspect logs in backend pods")
		framework.Logf("%v", targetPodLogs)
		if strings.Count(targetPodLogs, lbClientInf.IPv4) >= 2 {
			framework.Logf("found the expected srcIP %s!", lbClientInf.IPv4)
		} else if strings.Count(targetPodLogs, lbClientInf.IPv6) >= 2 {
			framework.Logf("found the expected srcIP %s!", lbClientInf.IPv6)
		} else {
			framework.Failf("could not get expected srcIP!, target pod logs:\n%q", targetPodLogs)
		}
	})
	ginkgo.It("Should ensure load balancer service works when ETP=local and backend pods are also egressIP served pods", func() {
		// TEST LOGIC: This test uses metaLB BGP for advertising routes towards 1 KIND ovnk cluster node
		// (node where the service backend pods live) as potential candidate to reach the load balancer service (192.168.10.0 service VIP).
		// There is also a FRR router that sits in front of the KIND cluster through which traffic flows to the service VIP.
		// External client (name: lbclient container {installation details in kind.sh script}) tries to reach a
		// load balancer service with VIP: 192.168.10.0 that has 4 backends running on one of the nodes in the cluster through
		// the FRR router.
		// -----------------       ------------------      VIP: 192.168.10.0  ---------------------
		// |               |       |                |                         | ovn-control-plane |
		// |   lbclient    |------>|   FRR router   |------> KIND cluster --> ---------------------
		// |               |       |                |                         |    ovn-worker     |   (4 backend CNI pods running
		// -----------------       ------------------                         ---------------------    on one of the nodes - say on ovn-worker -
		//                                                                    |    ovn-worker2    |    serving lb service; they are also
		//                                                                    ---------------------    served by EIP on primary network)
		//
		// So as an example here ovn-worker is where pods live and pods are served by EIP which is assigned on ovn-worker2
		// Now we test ETP=local works as expected without EIP re-routes messing with the reply traffic:
		// lbclient->FRR router->ovn-worker->br-ex->GR_ovn-worker->join->cluster-router->ovn-worker-switch->pod and response goes back
		// same way without it getting re-routed to egressNode ovn-worker2
		err := framework.WaitForServiceEndpointsNum(context.TODO(), f.ClientSet, namespaceName, svcName, 4, time.Second, time.Second*120)
		framework.ExpectNoError(err, fmt.Sprintf("service: %s never had an enpoint, err: %v", svcName, err))

		svcLoadBalancerIP, err := getServiceLoadBalancerIP(f.ClientSet, namespaceName, svcName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get service lb ip: %s, err: %v", svcName, err))

		ginkgo.By("patching service " + svcName + " to externalTrafficPolicy=local")
		err = patchServiceStringValue(f.ClientSet, svcName, "default", "/spec/externalTrafficPolicy", "Local")
		framework.ExpectNoError(err)
		output := e2ekubectl.RunKubectlOrDie("default", "get", "svc", svcName, "-o=jsonpath='{.spec.externalTrafficPolicy}'")
		gomega.Expect(output).To(gomega.Equal("'Local'"))
		time.Sleep(time.Second * 5) // buffer to ensure all rules are created correctly

		ginkgo.By("1 nodeIP route is advertised correctly by metalb BGP routes")
		// since ETP=local; ensure only this node's IP route is advertised correctly by metalb BGP routes
		// sample:
		// 192.168.10.0 nhid 31 via 172.19.0.4 dev eth0 proto bgp metric 20
		nodeIP, err := getNodeIP(f.ClientSet, backendNodeName)
		framework.ExpectNoError(err, fmt.Sprintf("failed to get nodes's %s node ip address", backendNodeName))
		framework.Logf("NodeIP of node %s is %s", backendNodeName, nodeIP)
		ipVer := ""
		if utilnet.IsIPv6String(svcLoadBalancerIP) {
			ipVer = " -6"
		}
		bgpRouteCommand := strings.Split(fmt.Sprintf("ip%s route show %s", ipVer, svcLoadBalancerIP), " ")
		primaryProviderNetwork, err := infraprovider.Get().PrimaryNetwork()
		framework.ExpectNoError(err, "must fetch primary provider network")
		frrExternalContainer := infraapi.ExternalContainer{Name: "frr", Network: primaryProviderNetwork}
		gomega.Eventually(func() bool {
			routes, err := infraprovider.Get().ExecExternalContainerCommand(frrExternalContainer, bgpRouteCommand)
			framework.ExpectNoError(err, "failed to get BGP routes from intermediary router")
			framework.Logf("Routes in FRR %s", routes)
			routeCount := 0
			matchedRoute := ""
			for _, route := range strings.Split(routes, "\n") {
				match := strings.Contains(route, nodeIP)
				if match {
					framework.Logf("DEBUG: Matched route %s for pattern %s", route, nodeIP)
					matchedRoute = route
				}
				if strings.Contains(route, "via") {
					routeCount++
				}
			}
			return routeCount == 1 && strings.Contains(matchedRoute, nodeIP)
		}, 60*time.Second).Should(gomega.BeTrue())

		ginkgo.By("by sending a UDP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName)
		netcatCmd := fmt.Sprintf("echo hostname | nc -uv -w2 %s %d",
			svcLoadBalancerIP,
			endpointUDPPort,
		)
		cmd := []string{"bash", "-x", "-c", netcatCmd}
		framework.Logf("netcat command %s", cmd)
		clientNetNetwork, err := infraprovider.Get().GetNetwork("clientnet")
		if errors.Is(err, infraapi.NotFound) {
			ginkgo.Skip("clientnet network is not available. Skipping")
		}
		framework.ExpectNoError(err, "clientnet network must be available")
		lbClientExternalContainer := infraapi.ExternalContainer{Name: "lbclient", Network: clientNetNetwork}
		output, err = infraprovider.Get().ExecExternalContainerCommand(lbClientExternalContainer, cmd)
		framework.ExpectNoError(err, "failed to connect to load balancer service")
		framework.Logf("netcat command output %s", output)

		ginkgo.By("ensure the sourceIP of the external container is preserved!")
		// Check that sourceIP of the LBService is preserved
		targetPodLogs, err := e2ekubectl.RunKubectl("default", "logs", "-l", "app=nginx", "--container", "udp-server")
		framework.ExpectNoError(err, "failed to inspect logs in backend pods")
		framework.Logf("Target pod logs:\n%q", targetPodLogs)
		lbClientNetworkInterface, err := infraprovider.Get().GetExternalContainerNetworkInterface(lbClientExternalContainer, clientNetNetwork)
		framework.ExpectNoError(err, "failed to get network interface info for external container %s connected to network %s", frrExternalContainer.Name, clientNetNetwork.Name())
		lbClientIPv4, lbClientIPv6 := lbClientNetworkInterface.IPv4, lbClientNetworkInterface.IPv6
		framework.Logf("found external container lbclient IPs: IPv4 %q, IPv6: %q", lbClientIPv4, lbClientIPv6)
		if strings.Contains(targetPodLogs, lbClientIPv4) {
			framework.Logf("found the expected srcIP %s!", lbClientIPv4)
		} else if strings.Contains(targetPodLogs, lbClientIPv6) {
			framework.Logf("found the expected srcIP %s!", lbClientIPv6)
		} else {
			framework.Failf("could not get expected srcIP!, target pod logs:\n%q", targetPodLogs)
		}

		ginkgo.By("label " + nonBackendNodeName + " as egressIP assignable")
		e2enode.AddOrUpdateLabelOnNode(f.ClientSet, nonBackendNodeName, "k8s.ovn.org/egress-assignable", "dummy")

		ginkgo.By("Create an EgressIP object with one egress IP defined")
		// Assign the egress IP without conflicting with any node IP,
		// the kind subnet is /16 or /64 so the following should be fine.
		var egressIP1 net.IP
		if utilnet.IsIPv6String(svcLoadBalancerIP) {
			egressIP1, err = ipalloc.NewPrimaryIPv6()
		} else {
			egressIP1, err = ipalloc.NewPrimaryIPv4()
		}
		framework.ExpectNoError(err, "must allocate new Node IP for EgressIP IP")
		var egressIPConfig = fmt.Sprintf(`apiVersion: k8s.ovn.org/v1
kind: EgressIP
metadata:
    name: ` + "egressip" + `
spec:
    egressIPs:
    - ` + egressIP1.String() + `
    podSelector:
        matchLabels:
            app: nginx
    namespaceSelector:
        matchLabels:
            kubernetes.io/metadata.name: ` + namespaceName + `
`)
		if err := os.WriteFile("egressip.yaml", []byte(egressIPConfig), 0644); err != nil {
			framework.Failf("Unable to write CRD config to disk: %v", err)
		}
		ginkgo.DeferCleanup(func() error {
			e2ekubectl.RunKubectlOrDie("default", "delete", "-f", "egressip.yaml", "--ignore-not-found=true")
			if err := os.Remove("egressip.yaml"); err != nil {
				return fmt.Errorf("unable to remove the CRD config from disk: %v", err)
			}
			return nil
		})

		framework.Logf("Create the EgressIP configuration")
		e2ekubectl.RunKubectlOrDie("default", "create", "-f", "egressip.yaml")

		ginkgo.By("4. Check that the status is of length one and that it is assigned to " + nonBackendNodeName)
		err = wait.PollImmediate(retryInterval, retryTimeout, func() (bool, error) {
			egressIP := egressIPs{}
			egressIPStdout, err := e2ekubectl.RunKubectl("default", "get", "eip", "-o", "json")
			if err != nil {
				framework.Logf("Error: failed to get the EgressIP object, err: %v", err)
				return false, nil
			}
			json.Unmarshal([]byte(egressIPStdout), &egressIP)
			if len(egressIP.Items) > 1 {
				framework.Failf("Didn't expect to retrieve more than one egress IP during the execution of this test, saw: %v", len(egressIP.Items))
			}
			return egressIP.Items[0].Status.Items[0].Node == nonBackendNodeName, nil
		})
		if err != nil {
			framework.Failf("Error: expected to have 1 egress IP assignment")
		}

		ginkgo.By("by sending a UDP packet to service " + svcName + " with type=LoadBalancer in namespace " + namespaceName + " with backend pod " + backendName)
		output, err = infraprovider.Get().ExecExternalContainerCommand(lbClientExternalContainer, cmd)
		framework.ExpectNoError(err, "failed to connect to load balancer service")
		framework.Logf("netcat command output %s", output)

		// Check that sourceIP of the LBService is preserved
		ginkgo.By("ensure the sourceIP of the external container is preserved!")
		targetPodLogs, err = e2ekubectl.RunKubectl("default", "logs", "-l", "app=nginx", "--container", "udp-server")
		framework.ExpectNoError(err, "failed to inspect logs in backend pods")
		framework.Logf("%v", targetPodLogs)
		if strings.Count(targetPodLogs, lbClientIPv4) >= 2 {
			framework.Logf("found the expected srcIP %s!", lbClientIPv4)
		} else if strings.Count(targetPodLogs, lbClientIPv6) >= 2 {
			framework.Logf("found the expected srcIP %s!", lbClientIPv6)
		} else {
			framework.Failf("could not get expected srcIP!, target pod logs:\n%q", targetPodLogs)
		}
	})
})

func getEndpointsForService(c clientset.Interface, namespace, serviceName string) (*v1.Endpoints, error) {
	endpoints, err := c.CoreV1().Endpoints(namespace).Get(context.TODO(), serviceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return endpoints, nil
}

func getNodeIP(c clientset.Interface, nodeName string) (string, error) {
	node, err := c.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	ips := e2enode.GetAddresses(node, v1.NodeInternalIP)
	return ips[0], nil
}

func buildAndRunCommand(command string) error {
	cmd := strings.Split(command, " ")
	output, err := exec.Command(cmd[0], cmd[1:]...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to run %q: %s (%s)", strings.Join(cmd, " "), err, output)
	}
	return err
}

func getServiceLoadBalancerIP(c clientset.Interface, namespace, serviceName string) (string, error) {
	svc, err := c.CoreV1().Services(namespace).Get(context.TODO(), serviceName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if len(svc.Status.LoadBalancer.Ingress) != 1 {
		return "", fmt.Errorf("service %s has no load balancer IPs", serviceName)
	}
	return svc.Status.LoadBalancer.Ingress[0].IP, nil
}

func setupIPv4NetworkForExternalClient(svcLoadBalancerIP string, svcLoadBalancerPort int, nodeIP string) {
	// The external client configuration done in install_metallb can not be used because routes for external client
	// installed in K8s node https://github.com/ovn-org/ovn-kubernetes/blob/master/contrib/kind.sh#L1045-L1047
	// are ignored in shared gateway mode and traffic coming back from pod is put on the docker bridge directly by
	// br-ex flows which needs to be handled in host(or vm) network.
	// Hence the following set of ip commands set up two networks called bridge (192.168.222.0/24) and
	// client 192.168.223.0/24 on the host(or vm) machine. The external client (192.168.223.2) runs on the client
	// network which tries to connect with load balancer service via bridge network. There would be also a route
	// created for the load balancer service forwarding packet into the node which hosts one of the endpoint.
	//                                     +------------------------------------+
	//                                     |         kind-ovn-cluster           |
	//                                     |                                    |
	//                                     |                                    |
	//                                     +----------------+-------------------+
	//                                                      |
	//                                                      |
	//                                                      |
	//                                                      |
	//                                                      |
	//                   +----------------------------------+-------------------------------------------+
	//                   |                                  172.18.0.1                                  |
	//                   |                                                     ip route add 192.168.223.0/24 via 192.168.222.2
	//                   |                                                     ip route add <svc-ip> via|<endpoint-node-ip>
	//                   |                                                     iptables -t filter -I FORWARD -d <svc-ip> -p tcp -m tcp --dport <svc-port> -j ACCEPT
	//                   |                                                                              |
	//                   |  vm                                    192.168.222.1                         |
	//                   +----------------------------------------+-------------------------------------+
	//                                                            |
	//                                                            |
	//                +-----------------------+         +---------+-----------------+
	//                |                       |         |         192.168.222.2     |
	//                |                       |         |                           |
	//                |          192.168.223.2+---------+ 192.168.223.1             |
	//                |  client               |         |                     bridge|
	//                +-----------------------+         +---------------------------+
	setupNetNamespaceAndLinks()
	err := buildAndRunCommand("sudo ip addr add 192.168.222.1/24 dev vmtobridge")
	framework.ExpectNoError(err, "failed to add ip address on vmtobridge gateway interface")
	err = buildAndRunCommand("sudo ip netns exec bridge ip addr add 192.168.222.2/24 dev bridgetovm")
	framework.ExpectNoError(err, "failed to add ip address on bridgetovm interface")
	err = buildAndRunCommand("sudo ip netns exec bridge ip addr add 192.168.223.1/24 dev bridgetoclient")
	framework.ExpectNoError(err, "failed to add ip address on bridgetoclient gateway interface")
	err = buildAndRunCommand("sudo ip netns exec client ip addr add 192.168.223.2/24 dev clienttobridge")
	framework.ExpectNoError(err, "failed to add ip address on clienttobridge interface")

	err = buildAndRunCommand("sudo ip netns exec client ip route add default via 192.168.223.1")
	framework.ExpectNoError(err, "failed to add default route on client netns")
	err = buildAndRunCommand("sudo ip netns exec bridge ip route add default via 192.168.222.1")
	framework.ExpectNoError(err, "failed to add default route on bridge netns")

	buildAndRunCommand("sudo ip route delete 192.168.223.0/24")
	err = buildAndRunCommand("sudo ip route add 192.168.223.0/24 via 192.168.222.2")
	framework.ExpectNoError(err, "failed to add route for client to handle reverse service traffic")

	err = buildAndRunCommand(fmt.Sprintf("sudo iptables -t filter -I FORWARD -d %s -p tcp -m tcp --dport %d -j ACCEPT", svcLoadBalancerIP, svcLoadBalancerPort))
	framework.ExpectNoError(err, "failed to add iptables rule for service")

	err = buildAndRunCommand(fmt.Sprintf("sudo ip route add %s via %s", svcLoadBalancerIP, nodeIP))
	framework.ExpectNoError(err, "failed to add route for external load balancer service")
}

func cleanupIPv4NetworkForExternalClient(svcLoadBalancerIP string, svcLoadBalancerPort int) {
	cleanupNetNamespace()
	buildAndRunCommand("sudo ip route delete 192.168.223.0/24 via 192.168.222.2")
	buildAndRunCommand(fmt.Sprintf("sudo ip route delete %s", svcLoadBalancerIP))
	buildAndRunCommand(fmt.Sprintf("sudo iptables -t filter -D FORWARD -d %s -p tcp -m tcp --dport %d -j ACCEPT", svcLoadBalancerIP, svcLoadBalancerPort))
}

func setupIPv6NetworkForExternalClient(svcLoadBalancerIP string, svcLoadBalancerPort int, nodeIP string) {
	// The external client configuration done in install_metallb can not be used because routes for external client
	// installed in K8s node https://github.com/ovn-org/ovn-kubernetes/blob/master/contrib/kind.sh#L1045-L1047
	// are ignored in shared gateway mode and traffic coming back from pod is put on the docker bridge directly by
	// br-ex flows which needs to be handled in host(or vm) network.
	// Hence the following set of ip -6 commands set up two IPv6 networks called bridge (fc00:f853:ccd:e222::0/64) and
	// client fc00:f853:ccd:e223::0/64 on the host(or vm) machine. The external client (fc00:f853:ccd:e223::2) runs on the client
	// network which tries to connect with load balancer service via bridge network. There would be also a route
	// created for the load balancer service forwarding packet into the node which hosts one of the endpoint.
	//                                               +------------------------------+
	//                                               |        kind-ovn-cluster      |
	//                                               +---------------+--------------+
	//                                                               |
	//                                     +-------------------------+----------------------------------------------+
	//                                     |                      fc00:f853:ccd:e793::1                             |
	//                                     |                                       ip -6 route add fc00:f853:ccd:e223::2 dev vmtobridge via fc00:f853:ccd:e222::2
	//                                     |                                       ip -6 route add %s via %s", svcLoadBalancerIP, nodeIP
	//                                     |        vm                                                              |
	//                                     +-----------------------------------------------------------+------------+
	//                                                                                                 | fc00:f853:ccd:e222::1/64
	//                                                                                                 |
	//                                                                                                 | fc00:f853:ccd:e222::2/64
	//              +--------------------------------------------+                              +------+-------------+
	//              |                                            |                              | ip -6 route add default dev bridgetovm via fc00:f853:ccd:e222::1
	//              |                             fc00:f853:ccd:e223::2/64----------------fc00:f853:ccd:e223::1/64   |
	//              |     client                                 |                              |           bridge   |
	//              |ip -6 route add default dev clienttobridge via fc00:f853:ccd:e223::1       +--------------------+
	//              +--------------------------------------------+
	setupNetNamespaceAndLinks()
	err := buildAndRunCommand("sudo ip -6 addr add fc00:f853:ccd:e222::1/64 dev vmtobridge")
	framework.ExpectNoError(err, "failed to add ip address on vmtobridge gateway interface")
	err = buildAndRunCommand("sudo ip netns exec bridge ip -6 addr add fc00:f853:ccd:e222::2/64 dev bridgetovm")
	framework.ExpectNoError(err, "failed to add ip address on bridgetovm interface")
	err = buildAndRunCommand("sudo ip netns exec bridge ip -6 addr add fc00:f853:ccd:e223::1/64 dev bridgetoclient")
	framework.ExpectNoError(err, "failed to add ip address on bridgetoclient gateway interface")
	err = buildAndRunCommand("sudo ip netns exec client ip -6 addr add fc00:f853:ccd:e223::2/64 dev clienttobridge")
	framework.ExpectNoError(err, "failed to add ip address on clienttobridge interface")

	err = buildAndRunCommand("sudo ip netns exec bridge sysctl -w net.ipv6.conf.all.forwarding=1")
	framework.ExpectNoError(err, "failed to enable ipv6 packet forwarding on bridge net namespace")

	err = buildAndRunCommand("sudo ip netns exec client ip -6 route add default dev clienttobridge via fc00:f853:ccd:e223::1")
	framework.ExpectNoError(err, "failed to add default route on client netns")
	err = buildAndRunCommand("sudo ip netns exec bridge ip -6 route add default dev bridgetovm via fc00:f853:ccd:e222::1")
	framework.ExpectNoError(err, "failed to add default route on bridge netns")

	err = buildAndRunCommand("sudo ip -6 route add fc00:f853:ccd:e223::2 dev vmtobridge via fc00:f853:ccd:e222::2")
	framework.ExpectNoError(err, "failed to add route for client to handle reverse service traffic")

	err = buildAndRunCommand(fmt.Sprintf("sudo ip -6 route add %s via %s", svcLoadBalancerIP, nodeIP))
	framework.ExpectNoError(err, "failed to add route for external load balancer service")

	err = buildAndRunCommand(fmt.Sprintf("sudo ip6tables -t filter -I FORWARD -d %s -p tcp -m tcp --dport %d -j ACCEPT", svcLoadBalancerIP, svcLoadBalancerPort))
	framework.ExpectNoError(err, "failed to add iptables rule for service")
}

func cleanupIPv6NetworkForExternalClient(svcLoadBalancerIP string, svcLoadBalancerPort int) {
	cleanupNetNamespace()
	buildAndRunCommand("sudo ip -6 route delete fc00:f853:ccd:e223::2")
	buildAndRunCommand(fmt.Sprintf("sudo ip -6 route delete %s", svcLoadBalancerIP))
	buildAndRunCommand(fmt.Sprintf("sudo ip6tables -t filter -D FORWARD -d %s -p tcp -m tcp --dport %d -j ACCEPT", svcLoadBalancerIP, svcLoadBalancerPort))
}

func setupNetNamespaceAndLinks() {
	err := buildAndRunCommand("sudo ip netns add bridge")
	framework.ExpectNoError(err, "failed to add brige network namespace")
	err = buildAndRunCommand("sudo ip netns add client")
	framework.ExpectNoError(err, "failed to add client network namespace")

	err = buildAndRunCommand("sudo ip link add vmtobridge type veth peer name bridgetovm")
	framework.ExpectNoError(err, "failed to add veth pair for bridge")
	err = buildAndRunCommand("sudo ip link add clienttobridge type veth peer name bridgetoclient")
	framework.ExpectNoError(err, "failed to add veth pair for client")
	err = buildAndRunCommand("sudo ip link set bridgetovm netns bridge")
	framework.ExpectNoError(err, "failed to move bridgetovm into bridge netns")
	err = buildAndRunCommand("sudo ip link set bridgetoclient netns bridge")
	framework.ExpectNoError(err, "failed to move bridgetoclient into bridge netns")
	err = buildAndRunCommand("sudo ip link set clienttobridge netns client")
	framework.ExpectNoError(err, "failed to move clienttobridge into client netns")

	err = buildAndRunCommand("sudo ip link set vmtobridge up")
	framework.ExpectNoError(err, "failed to get vmtobridge up")
	err = buildAndRunCommand("sudo ip netns exec bridge ip link set bridgetovm up")
	framework.ExpectNoError(err, "failed to get bridgetovm up")

	err = buildAndRunCommand("sudo ip netns exec bridge ip link set bridgetoclient up")
	framework.ExpectNoError(err, "failed to get bridgetoclient up")
	err = buildAndRunCommand("sudo ip netns exec client ip link set clienttobridge up")
	framework.ExpectNoError(err, "failed to get clienttobridge up")
}

func cleanupNetNamespace() {
	buildAndRunCommand("sudo ip netns delete bridge")
	buildAndRunCommand("sudo ip netns delete client")
}
