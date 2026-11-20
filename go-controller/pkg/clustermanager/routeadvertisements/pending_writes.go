// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package routeadvertisements

import (
	"sync"
	"time"

	nadtypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	nadlisters "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"
	frrtypes "github.com/metallb/frr-k8s/api/v1beta1"
	frrlisters "github.com/metallb/frr-k8s/pkg/client/listers/api/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/client-go/tools/cache"
)

// A deleted object can be kept by a finalizer, and a relist can skip a version
// that is not comparable. Past this point we act on the cache as it is rather
// than block the RouteAdvertisements.
const pendingWriteTimeout = 10 * time.Second

// pendingWrite is a write of this controller that its informer has not
// delivered yet.
type pendingWrite struct {
	ra string
	// resourceVersion returned by a create, update or patch. The write is
	// observed once the informer delivers the object at this version or a
	// later one.
	resourceVersion string
	// uid of a deleted object, set instead of resourceVersion. The delete is
	// observed once the cache no longer holds an object with this UID.
	uid     ktypes.UID
	expires time.Time
}

// pendingWrites holds, by namespace/name, the FRRConfiguration and NAD writes
// that are still on their way back through the informers. Reconciling a
// RouteAdvertisements against a cache that misses its own last writes could
// publish an EgressIP on its new node before the old node's withdrawal is
// visible, or advance NAD intent ahead of the FRR state. The reconcile waits
// for them instead of reading the API. Resource versions of one resource type
// are ordered, so a cache that has moved past our version has seen our write,
// even if a relist never delivered that exact version.
type pendingWrites struct {
	sync.Mutex
	frr map[string]pendingWrite
	nad map[string]pendingWrite
	// held are the RouteAdvertisements whose reconcile was put off for these
	// writes. Only they need to run again when a write is observed.
	held map[string]bool

	frrLister frrlisters.FRRConfigurationLister
	nadLister nadlisters.NetworkAttachmentDefinitionLister
}

func newPendingWrites(frrLister frrlisters.FRRConfigurationLister, nadLister nadlisters.NetworkAttachmentDefinitionLister) *pendingWrites {
	return &pendingWrites{
		frr:       map[string]pendingWrite{},
		nad:       map[string]pendingWrite{},
		held:      map[string]bool{},
		frrLister: frrLister,
		nadLister: nadLister,
	}
}

func (p *pendingWrites) add(writes map[string]pendingWrite, key string, write pendingWrite) {
	p.Lock()
	defer p.Unlock()
	now := time.Now()
	// Cleanup of expired entries: a deleted RouteAdvertisements is not
	// reconciled again to drop its own.
	for k, w := range writes {
		if now.After(w.expires) {
			delete(writes, k)
		}
	}
	write.expires = now.Add(pendingWriteTimeout)
	writes[key] = write
}

func (p *pendingWrites) frrWritten(ra string, cfg *frrtypes.FRRConfiguration) {
	if cfg.ResourceVersion != "" {
		p.add(p.frr, cache.MetaObjectToName(cfg).String(), pendingWrite{ra: ra, resourceVersion: cfg.ResourceVersion})
	}
}

func (p *pendingWrites) frrDeleted(ra string, cfg *frrtypes.FRRConfiguration) {
	if cfg.UID != "" {
		p.add(p.frr, cache.MetaObjectToName(cfg).String(), pendingWrite{ra: ra, uid: cfg.UID})
	}
}

func (p *pendingWrites) nadWritten(ra string, nad *nadtypes.NetworkAttachmentDefinition) {
	if nad.ResourceVersion != "" {
		p.add(p.nad, cache.MetaObjectToName(nad).String(), pendingWrite{ra: ra, resourceVersion: nad.ResourceVersion})
	}
}

// reached reports whether the cached version is the written one or a later
// one. Versions that cannot be ordered, such as those of a fake client, only
// match when equal.
func reached(cached, written string) bool {
	if order, err := resourceversion.CompareResourceVersion(cached, written); err == nil {
		return order >= 0
	}
	return cached == written
}

// observed is called from the informer handlers with every delivered version
// of an object. Once the version of a pending write is reached, the entry is
// dropped and the RouteAdvertisements that made the write is returned, along
// with whether its reconcile is held back for it. own is set if the event is
// that write itself: it tells the RouteAdvertisements nothing new and can be
// ignored. A later version is someone else's change and must still be handled.
func (p *pendingWrites) observed(writes map[string]pendingWrite, key, resourceVersion string) (ra string, held, own bool) {
	p.Lock()
	defer p.Unlock()
	write, ok := writes[key]
	if !ok || write.uid != "" || !reached(resourceVersion, write.resourceVersion) {
		return "", false, false
	}
	delete(writes, key)
	return write.ra, p.held[write.ra], write.resourceVersion == resourceVersion
}

func (p *pendingWrites) frrObserved(cfg *frrtypes.FRRConfiguration) (ra string, held, own bool) {
	return p.observed(p.frr, cache.MetaObjectToName(cfg).String(), cfg.ResourceVersion)
}

func (p *pendingWrites) nadObserved(nad *nadtypes.NetworkAttachmentDefinition) (ra string, held, own bool) {
	return p.observed(p.nad, cache.MetaObjectToName(nad).String(), nad.ResourceVersion)
}

// frrDeletionObserved is observed for an FRRConfiguration that is gone from
// the cache.
func (p *pendingWrites) frrDeletionObserved(key string) (string, bool) {
	p.Lock()
	defer p.Unlock()
	write, ok := p.frr[key]
	if !ok || write.uid == "" {
		return "", false
	}
	delete(p.frr, key)
	return write.ra, p.held[write.ra]
}

// wait is the gate at the start of a RouteAdvertisements reconcile. It
// returns zero if the caches hold every write this RA made, and otherwise
// how long the reconcile should be put off.
//
// It goes over the RA's pending entries and compares each with the cached
// object: a create, update or patch is done once the cache has reached that
// resource version, a delete once the cache no longer has that UID. This catches an
// event that arrived before its write was recorded, which the handlers could
// not match. Entries that are done or expired are dropped. If any remain, the
// RA is marked as held, so that the event for one of them reconciles it
// again, and the time until the last of them expires is returned.
func (p *pendingWrites) wait(raName string) time.Duration {
	p.Lock()
	defer p.Unlock()
	now := time.Now()
	var wait time.Duration
	// settle drops an entry that is done or expired and extends the wait otherwise.
	settle := func(writes map[string]pendingWrite, key string, write pendingWrite, cached metav1.Object, found bool) {
		deleted := write.uid != "" && (!found || cached.GetUID() != write.uid)
		written := write.uid == "" && found && reached(cached.GetResourceVersion(), write.resourceVersion)
		if deleted || written || now.After(write.expires) {
			delete(writes, key)
			return
		}
		wait = max(wait, write.expires.Sub(now))
	}
	for key, write := range p.frr {
		if write.ra != raName {
			continue
		}
		namespace, name, _ := cache.SplitMetaNamespaceKey(key)
		cfg, err := p.frrLister.FRRConfigurations(namespace).Get(name)
		settle(p.frr, key, write, cfg, err == nil)
	}
	for key, write := range p.nad {
		if write.ra != raName {
			continue
		}
		namespace, name, _ := cache.SplitMetaNamespaceKey(key)
		nad, err := p.nadLister.NetworkAttachmentDefinitions(namespace).Get(name)
		settle(p.nad, key, write, nad, err == nil)
	}
	if wait > 0 {
		p.held[raName] = true
	} else {
		delete(p.held, raName)
	}
	return wait
}
