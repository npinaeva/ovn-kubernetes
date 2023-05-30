package selector_based_handler

import (
	"fmt"
	"sync"

	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"

	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// PodExtraHandler will be caled on pod events, it is also designed to insert db ops into
// LSP creation/deletion transaction to ensure atomicity. For this purpose we have AddPodOps and DeletePodOps.
// Since the handler is selector-based, some pods may be selected after they were created with label update event,
// in this case AddPod and DeletePod will be called.x
type PodExtraHandler interface {
	ProcessExisting(lspGetter LSPGetter, objs []interface{}) error
	AddPodOps(pod *kapi.Pod, portUUID string) (ops []ovsdb.Operation, err error)
	AddPod(pod *kapi.Pod, lspGetter LSPGetter) error
	DeletePodOps(pod *kapi.Pod, portUUID string) (ops []ovsdb.Operation, err error)
	DeletePod(pod *kapi.Pod, lspGetter LSPGetter) error
}

type LSPInfo struct {
	UUID string
	Err  error
}

// LSPGetter is a controller-dependent function, that should return all LSPs manages by that controller
// for a given pod. There may be multiple LSPs if there are many NADs for the same network.
type LSPGetter func(pod *kapi.Pod) []LSPInfo

type podPostHandlerWithSelector struct {
	selectorInfo
	PodExtraHandler
}

type PodExtraHandlersManager struct {
	lock      sync.RWMutex
	inf       cache.SharedIndexInformer
	handlers  map[int]podPostHandlerWithSelector
	nextIdx   int
	lspGetter LSPGetter
}

func NewPodPostHandlersManager(podInformer cache.SharedIndexInformer, lspGetter LSPGetter) *PodExtraHandlersManager {
	return &PodExtraHandlersManager{
		inf:       podInformer,
		nextIdx:   0,
		handlers:  map[int]podPostHandlerWithSelector{},
		lspGetter: lspGetter,
	}
}

func (h *PodExtraHandlersManager) AddHandler(namespace string, selector labels.Selector, newHandler PodExtraHandler) (int, error) {
	h.lock.Lock()
	defer h.lock.Unlock()
	// handle existing objects
	selInfo := selectorInfo{
		namespace: namespace,
		selector:  selector,
		objType:   factory.PodType,
	}
	items := make([]interface{}, 0)

	for _, obj := range h.inf.GetStore().List() {
		filtered, err := filterFunc(selInfo, obj)
		if err != nil {
			return NoHandler, err
		}
		if filtered {
			items = append(items, obj)
		}
	}

	// Process existing items as a set so the caller can clean up
	// after a restart or whatever. We will wrap it with retries to ensure it succeeds.
	// Being so, ProcessExisting is expected to be idem-potent!
	if err := newHandler.ProcessExisting(h.lspGetter, items); err != nil {
		return NoHandler, fmt.Errorf("failed in ProcessExisting %v: %v", items, err)
	}

	idx := h.nextIdx
	h.nextIdx += 1
	h.handlers[idx] = podPostHandlerWithSelector{
		selInfo,
		newHandler,
	}
	return idx, nil
}

func (h *PodExtraHandlersManager) DeleteHandler(idx int) {
	h.lock.Lock()
	defer h.lock.Unlock()
	delete(h.handlers, idx)
}

func (h *PodExtraHandlersManager) AddPodOps(pod *kapi.Pod, portUUID string) (ops []ovsdb.Operation, err error) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	for _, handler := range h.handlers {
		filtered, err := filterFunc(handler.selectorInfo, pod)
		if err != nil {
			return nil, err
		}
		if filtered {
			newOps, err := handler.AddPodOps(pod, portUUID)
			if err != nil {
				return nil, err
			}
			ops = append(ops, newOps...)
		}
	}
	return ops, nil
}

func (h *PodExtraHandlersManager) UpdatePod(oldPod, newPod *kapi.Pod) error {
	h.lock.RLock()
	defer h.lock.RUnlock()
	for _, handler := range h.handlers {
		newer, err := filterFunc(handler.selectorInfo, newPod)
		if err != nil {
			return err
		}
		older, err := filterFunc(handler.selectorInfo, oldPod)
		if err != nil {
			return err
		}
		switch {
		case newer && !older:
			err = handler.AddPod(newPod, h.lspGetter)
			if err != nil {
				return err
			}
		case !newer && older:
			err = handler.DeletePod(oldPod, h.lspGetter)
			if err != nil {
				return err
			}
		default:
			// do nothing
		}
	}
	return nil
}

func (h *PodExtraHandlersManager) DeletePodOps(pod *kapi.Pod, portUUID string) (ops []ovsdb.Operation, err error) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	for _, handler := range h.handlers {
		filtered, err := filterFunc(handler.selectorInfo, pod)
		if err != nil {
			return nil, err
		}
		if filtered {
			newOps, err := handler.DeletePodOps(pod, portUUID)
			if err != nil {
				return nil, err
			}
			ops = append(ops, newOps...)
		}
	}
	return ops, nil
}

func (h *PodExtraHandlersManager) LSPGetter() LSPGetter {
	return h.lspGetter
}
