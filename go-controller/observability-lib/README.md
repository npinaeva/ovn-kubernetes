An example program that uses the observability library to decode and print samples can be found in
[ovnkubeobserv.go](../cmd/ovnkube-observ/ovnkubeobserv.go). The compiled binary for this program is 15 MB.
To get the binary, run 
```shell
hack/build-go.sh cmd/ovnkube-observ
```
The binary will be created under `./_output/go/bin`.

Extra installation steps on a RHEL node for testing
```shell
# get some rhel9 ovn releaser, e.g. from https://brewweb.engineering.redhat.com/brew/buildinfo?buildID=3026196
dnf install ovn23.06-23.06.3-36.el9fdp.x86_64.rpm ovn23.06-central-23.06.3-36.el9fdp.x86_64.rpm

sudo mkdir -p /etc/ovn /var/run/ovn /var/log/ovn
sudo ovsdb-tool create /etc/ovn/ovnnb_db.db /usr/share/ovn/ovn-nb.ovsschema

sudo ovsdb-server /etc/ovn/ovnnb_db.db --remote=punix:/var/run/ovn/ovnnb_db.sock \
     --remote=db:OVN_Northbound,NB_Global,connections \
     --private-key=db:OVN_Northbound,SSL,private_key \
     --certificate=db:OVN_Northbound,SSL,certificate \
     --bootstrap-ca-cert=db:OVN_Northbound,SSL,ca_cert \
     --pidfile=/var/run/ovn/ovnnb-server.pid --detach --log-file=/var/log/ovn/ovnnb-server.log

ovn-nbctl show # should just succeed without any output and errors
ovn-nbctl ls-add test
ovn-nbctl --name="nice sample!" acl-add test to-lport 1001 "ip4.src == 1.1.1.1" drop
```

The expected "decoded" message from this setup is "nice sample!", you can check it if you run `ovnkube-observ` binary
with `-enable-enrichment` flag.