#!/usr/bin/env bash
set -xeo pipefail

cluster_version=${CLUSTER_VERSION:-1.34.2}
cluster_name=${CLUSTER_NAME:-sriov-cluster}
domain_name=$cluster_name.lab

k8s_network_cidr=${K8S_NETWORK_CIDR:-10.100.0.0/24}

here="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
root="$(readlink --canonicalize "$here/..")"

NUM_OF_WORKERS=${NUM_OF_WORKERS:-2}
total_number_of_nodes=$((1 + NUM_OF_WORKERS))

export KUBECONFIG=$HOME/.kcli/clusters/$cluster_name/auth/kubeconfig
export PATH=$PWD:$PATH

check_requirements() {
  for cmd in kcli virsh podman make helm; do
    if ! command -v "$cmd" &> /dev/null; then
      echo "$cmd is not available"
      exit 1
    fi
  done
  return 0
}

recreate_cluster() {
  echo "## checking requirements"
  check_requirements
  echo "## delete existing cluster name $cluster_name"
  kcli delete cluster $cluster_name -y
  kcli delete network $cluster_name -y

  kcli delete network k8s -y 2>/dev/null || true
  kcli delete network $cluster_name -y 2>/dev/null || true
  kcli create network -c $k8s_network_cidr k8s
  kcli create network -c 10.100.251.0/24 --nodhcp -i $cluster_name

  cat <<EOF > ./${cluster_name}-plan.yaml
version: $cluster_version
ctlplane_memory: 4096
worker_memory: 4096
pool: default
disk_size: 50
network: k8s
domain: $domain_name
ctlplanes: 1
workers: $NUM_OF_WORKERS
ingress: false
machine: q35
engine: crio
sdn:
multus: false
autolabeller: false
vmrules:
  - $cluster_name-.*:
      nets:
        - name: k8s
        - name: $cluster_name
          type: igb
          vfio: true
          noconf: true
          numa: 1
      numcpus: 6
      numa:
        - id: 0
          vcpus: 0,2,4
          memory: 2048
        - id: 1
          vcpus: 1,3,5
          memory: 2048
EOF

  kcli create cluster generic --paramfile ./${cluster_name}-plan.yaml $cluster_name
}

wait_nodes() {
ATTEMPTS=0
MAX_ATTEMPTS=72
ready=false
sleep_time=10

until $ready || [ $ATTEMPTS -eq $MAX_ATTEMPTS ]
do
    echo "waiting for cluster to be ready"
    if [ `kubectl get node | grep Ready | wc -l` == $total_number_of_nodes ]; then
        echo "cluster is ready"
        ready=true
    else
        echo "cluster is not ready yet"
        sleep $sleep_time
    fi
    ATTEMPTS=$((ATTEMPTS+1))
done

if ! $ready; then
    echo "Timed out waiting for cluster to be ready"
    kubectl get nodes
    exit 1
fi

echo kubectl get nodes -owide
}

tweak_nodes() {
#TODO add disable keealived option in kcli?
echo "## disable keepalived on ctlplane"
kcli ssh ${cluster_name}-ctlplane-0 << EOF
sudo su
vip=\$(ip addr show eth0 | grep 'inet.*secondary' | awk '{print \$2}')
systemctl stop keepalived
systemctl disable keepalived
[ -n "\$vip" ] && ip addr del \$vip dev \$(ip route show default | awk '/default/ {print \$5}') 2>/dev/null || true
EOF

# replace VIP with nodeIP
sed -i "s|https://.*:6443|https://${ctlplane_ip}:6443|" "$KUBECONFIG"

echo "## label cluster workers as worker"
for ((num=0; num<NUM_OF_WORKERS; num++))
do
    kubectl label node $cluster_name-worker-$num.$domain_name node-role.kubernetes.io/worker= --overwrite
    # replace VIP with nodeIP
    kcli ssh ${cluster_name}-worker-$num << EOF
sudo sed -i "/ api.${cluster_name}.${domain_name}/d" /etc/hosts
echo "${ctlplane_ip} api.${cluster_name}.${domain_name}" | sudo tee -a /etc/hosts
EOF
done
}

function update_host() {
    node_name=$1
    kcli ssh $node_name << EOF
sudo su
mkdir -p /var/log/journal
systemd-tmpfiles --create --prefix /var/log/journal
systemctl restart systemd-journald
echo '$insecure_registry' > /etc/containers/registries.conf.d/003-internal.conf
systemctl restart crio
systemctl stop NetworkManager
# pin the DHCP-acquired address on the underlay so the lease can't expire
addr=\$(ip -4 -o a show dev eth0 | awk '{print \$4; exit}')
[ -n "\$addr" ] && ip addr change \$addr dev eth0 valid_lft forever preferred_lft forever
modprobe br_netfilter
ethtool --offload eth1 rx off tx off
ethtool -K eth1 gso off
echo 4 > /sys/class/net/eth1/device/sriov_numvfs

# Bind VF index 3 (the 4th VF is filtered out) to vfio-pci so the VFIO code paths
# can be exercised on this otherwise kernel-driver SR-IOV setup.
# The kcli VM has no vIOMMU, so we enable noiommu mode — fine for
# testing the device-plugin / DRA plumbing, not for production
# isolation guarantees.
modprobe vfio
modprobe vfio_pci
modprobe vfio_iommu_type1
if [ -e /sys/module/vfio/parameters/enable_unsafe_noiommu_mode ]; then
  echo 1 > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode
else
  echo "WARNING: kernel lacks CONFIG_VFIO_NOIOMMU; vfio-pci bind may fail" >&2
fi
vf3_bdf=\$(basename \$(readlink -f /sys/class/net/eth1/device/virtfn3))
if [ -n "\$vf3_bdf" ]; then
  if [ -L "/sys/bus/pci/devices/\$vf3_bdf/driver" ]; then
    echo "\$vf3_bdf" > /sys/bus/pci/devices/\$vf3_bdf/driver/unbind
  fi
  echo "vfio-pci" > /sys/bus/pci/devices/\$vf3_bdf/driver_override
  echo "\$vf3_bdf" > /sys/bus/pci/drivers/vfio-pci/bind
  echo "Bound VF \$vf3_bdf to vfio-pci"
else
  echo "WARNING: could not resolve VF3 BDF under eth1; skipping vfio bind" >&2
fi
EOF
}

create_registry() {
kubectl create namespace container-registry

echo "## deploy internal registry"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: registry-pv
spec:
  capacity:
    storage: 60Gi
  volumeMode: Filesystem
  accessModes:
  - ReadWriteOnce
  persistentVolumeReclaimPolicy: Delete
  storageClassName: registry-local-storage
  local:
    path: /mnt/
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
          - ${cluster_name}-ctlplane-0.${domain_name}
EOF

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: registry-pv-claim
  namespace: container-registry
spec:
  accessModes:
    - ReadWriteOnce
  volumeMode: Filesystem
  resources:
    requests:
      storage: 60Gi
  storageClassName: registry-local-storage
EOF

cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: registry
  namespace: container-registry
spec:
  replicas: 1
  selector:
    matchLabels:
      app: registry
  template:
    metadata:
      labels:
        app: registry
    spec:
      hostNetwork: true
      tolerations:
        - operator: Exists
      containers:
      - image: quay.io/libpod/registry:2.8.2
        imagePullPolicy: Always
        name: registry
        volumeMounts:
        - name: data
          mountPath: /var/lib/registry
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: registry-pv-claim
      terminationGracePeriodSeconds: 10
EOF
}

sudo -v
recreate_cluster
wait_nodes

# discover DHCP-assigned ctlplane IP instead of using VIP
ctlplane_ip=$(kcli info vm $cluster_name-ctlplane-0 | awk '/^ip:/{print $2}')
if [ -z "$ctlplane_ip" ]; then
    echo "could not determine ctlplane IP" >&2
    exit 1
fi
echo "## ctlplane IP: $ctlplane_ip"

tweak_nodes

controller_ip=$ctlplane_ip
insecure_registry="[[registry]]
location = \"$controller_ip:5000\"
insecure = true

[aliases]
\"golang\" = \"docker.io/library/golang\"
"

sudo bash -c "cat << EOF > /etc/containers/registries.conf.d/003-${cluster_name}.conf
$insecure_registry
EOF
"

update_host $cluster_name-ctlplane-0
for ((num=0; num<NUM_OF_WORKERS; num++))
do
  update_host $cluster_name-worker-$num
done

create_registry

TIMEOUT=400

kubectl -n container-registry wait --for=condition=available deploy/registry --timeout=${TIMEOUT}s
kubectl apply -f "https://raw.githubusercontent.com/k8snetworkplumbingwg/multi-networkpolicy/refs/tags/v1.0.1/scheme.yml"
kubectl apply -f "https://raw.githubusercontent.com/k8snetworkplumbingwg/ipamclaims/v0.5.1-alpha/artifacts/k8s.cni.cncf.io_ipamclaims.yaml"

OVN_IMAGE="$ctlplane_ip:5000/ovn-daemonset-fedora:dev"
if [ -z "${SKIP_BUILD}" ]; then
  echo "## build ovn-kubernetes image"
  pushd "${root}/dist/images"
  make fedora-image OCI_BIN=podman
  popd
fi
podman tag ovn-daemonset-fedora:latest "${OVN_IMAGE}"
podman push --tls-verify=false "${OVN_IMAGE}"

echo "## deploy ovn-kubernetes"
k8s_api_server="https://${ctlplane_ip}:6443"

for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
    kubectl label node $node k8s.ovn.org/zone-name=$node --overwrite
done

# Install thin Multus (kcli's default install is thick; we want thin to match the
# reference cluster). Override --multus-conf-file from "auto" so the installer
# uses our ConfigMap content instead of inlining the on-disk OVN-K conf.
echo "## install thin Multus"
kubectl apply -f https://raw.githubusercontent.com/k8snetworkplumbingwg/multus-cni/master/deployments/multus-daemonset.yml

helm install ovn-kubernetes "${root}/helm/ovn-kubernetes" \
    -f "${root}/helm/ovn-kubernetes/values-single-node-zone.yaml" \
    --set k8sAPIServer="${k8s_api_server}" \
    --set global.image.repository="$ctlplane_ip:5000/ovn-daemonset-fedora" \
    --set global.image.tag="dev" \
    --set global.image.pullPolicy="Always" \
    --set global.enableMultiNetwork=true \
    --set global.enableNetworkSegmentation=true \
    --set global.enableDRA=true \
    --set 'global.draFilter=!("k8s.ovn.org/ifName" in attributes) || attributes["k8s.ovn.org/ifName"].StringValue != "eth4"'
echo "## wait for ovn-kubernetes to be ready"
kubectl -n ovn-kubernetes wait --for=condition=ready pod -l app=ovnkube-node --timeout=${TIMEOUT}s
kubectl -n ovn-kubernetes wait --for=condition=ready pod -l name=ovnkube-control-plane --timeout=${TIMEOUT}s

echo "## wait for coredns"
kubectl -n kube-system wait --for=condition=available deploy/coredns --timeout=${TIMEOUT}s

# Make every pod's eth0 come from the OVN-K primary network via Multus.
kubectl -n kube-system patch ds kube-multus-ds --type=json -p='[
  {"op":"replace","path":"/spec/template/spec/containers/0/args","value":[
    "--multus-conf-file=/tmp/multus-conf/cni-conf.json",
    "--cni-conf-dir=/host/etc/cni/net.d"
  ]}
]'
echo "## configure default network as ovn-primary NAD"
kubectl apply -f - <<'NAD'
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata:
  name: ovn-primary
  namespace: default
spec:
  config: '{ "cniVersion": "0.4.0", "name": "ovn-primary", "netAttachDefName": "default/ovn-primary", "type": "ovn-k8s-cni-overlay" }'
NAD

# Point Multus at the NAD as the cluster default network. Multus parses
# clusterNetwork via multusNamespace (default kube-system) rather than splitting
# a "ns/name" form, so set both explicitly.
kubectl apply -f - <<'CM'
kind: ConfigMap
apiVersion: v1
metadata:
  name: multus-cni-config
  namespace: kube-system
  labels:
    tier: node
    app: multus
data:
  cni-conf.json: |-
    {
      "cniVersion": "0.4.0",
      "name": "multus-cni-network",
      "type": "multus",
      "clusterNetwork": "ovn-primary",
      "multusNamespace": "default",
      "logFile": "/var/log/multus.log",
      "logLevel": "debug",
      "kubeconfig": "/etc/cni/net.d/multus.d/multus.kubeconfig"
    }
CM
kubectl -n kube-system patch ds kube-multus-ds -p '{
    "spec":{"template":{"spec":{
      "containers":[{
        "name":"kube-multus",
        "args":[
          "--multus-conf-file=/tmp/multus-conf/00-multus.conf",
          "--cni-conf-dir=/host/etc/cni/net.d",
          "--multus-bin-file=/host/opt/cni/bin/multus"
        ]
      }],
      "volumes":[{
        "name":"multus-cfg",
        "configMap":{
          "name":"multus-cni-config",
          "items":[{"key":"cni-conf.json","path":"00-multus.conf"}]
        }
      }]
    }}}
  }'
kubectl -n kube-system rollout restart ds -l app=multus

# TODO delete kube-proxy
