set -euxo pipefail

set -a
    KIND_CLUSTER_NAME=ovn
    KIND_INSTALL_INGRESS=true
    KIND_ALLOW_SYSTEM_WRITES=true
    OVN_HYBRID_OVERLAY_ENABLE=false
    OVN_MULTICAST_ENABLE=true
    OVN_EMPTY_LB_EVENTS=true
    OVN_HA=false
    OVN_DISABLE_SNAT_MULTIPLE_GWS=true
    KIND_INSTALL_METALLB=false
    OVN_GATEWAY_MODE=local
    OVN_SECOND_BRIDGE=false
    ENABLE_MULTI_NET=true
    ENABLE_NETWORK_SEGMENTATION=true
    DISABLE_UDN_HOST_ISOLATION=true
    PLATFORM_IPV4_SUPPORT=true
    PLATFORM_IPV6_SUPPORT=true
    KIND_INSTALL_KUBEVIRT=false
    OVN_COMPACT_MODE=false
    OVN_DUMMY_GATEWAY_BRIDGE=false
    OVN_ENABLE_INTERCONNECT=true
    OVN_DISABLE_FORWARDING=false
    OVN_ENABLE_DNSNAMERESOLVER=false
    OVN_NETWORK_QOS_ENABLE=false
    ENABLE_ROUTE_ADVERTISEMENTS=false
    ADVERTISE_DEFAULT_NETWORK=false
set +a

build_master() {
  local builddir
  builddir=$(mktemp -d "/tmp/XXXXXX")
  pushd "${builddir}"
  git clone https://github.com/ovn-kubernetes/ovn-kubernetes.git
  cd ovn-kubernetes
  # Build binaries
  make -C ./go-controller

  # Build image
  make -C ./dist/images IMAGE="${MASTER_IMAGE}" OCI_BIN="docker" fedora-image
  popd
  rm -rf "${builddir}"
}

run_master_kind() {
  pushd contrib
  ./kind.sh --ovn-image "${MASTER_IMAGE}"
  popd
}

build_latest_manifests() {
  pushd contrib
  ./kind.sh -manifests --deploy
  popd
}

remove_one_pod() {
  JSON_PATCH=$(printf '{
     "spec": {
       "template": {
         "spec": {
           "affinity": {
             "nodeAffinity": {
               "requiredDuringSchedulingIgnoredDuringExecution": {
                 "nodeSelectorTerms": [
                   {
                     "matchExpressions": [
                       {
                         "key": "kubernetes.io/hostname",
                         "operator": "NotIn",
                         "values": [
                           "%s"
                         ]
                       }
                     ]
                   }
                 ]
               }
             }
           }
         }
       }
     }
   }' "$1")
  kubectl patch daemonset client -n test-l2 -p "${JSON_PATCH}"
}

bring_all_pods() {
  kubectl patch daemonset client -n test-l2 --type='json' -p='[{"op": "remove", "path": "/spec/template/spec/affinity"}]'
}

kubectl_wait_daemonset() {
  kubectl -n ovn-kubernetes rollout status daemonset/$1
}

MASTER_IMAGE="localhost/ovn-daemonset-fedora:master"
OVN_IMAGE="localhost/ovn-daemonset-fedora:dev"

#build_master
run_master_kind
kubectl apply -f ~/kube-test/pods/udnl2-ds.yaml
kubectl apply -f ~/kube-test/pods/udnl3-pod.yaml
remove_one_pod ovn-control-plane
#remove_one_pod ovn-worker
#sleep 15
build_latest_manifests
kubectl rollout restart ds/ovnkube-node -n ovn-kubernetes

#kubectl apply -f ovnkube-identity.yaml
#kubectl_wait_daemonset ovnkube-identity
#kubectl -n ovn-kubernetes set image ds/ovnkube-node ovnkube-controller=${OVN_IMAGE}
#kubectl rollout restart ds/ovnkube-node -n ovn-kubernetes
