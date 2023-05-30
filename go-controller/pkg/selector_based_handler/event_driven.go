package selector_based_handler

import (
	"fmt"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"reflect"
	"sync"
)

const NoHandler = -1

type EventBasedHandler interface {
	ProcessExisting([]interface{}) error
	OnAdd(obj interface{}) error
	OnUpdate(oldObj, newObj interface{}) error
	OnDelete(obj interface{}) error
}

type eventBasedHandlerWithSelector struct {
	selectorInfo
	EventBasedHandler
}

// implements retry.EventHandler
type EventBasedWatcher struct {
	retry.EmptyEventHandler
	objType  reflect.Type
	lock     sync.RWMutex
	inf      cache.SharedIndexInformer
	handlers map[int]eventBasedHandlerWithSelector
	nextIdx  int

	wf             *factory.WatchFactory
	wfHandler      *factory.Handler
	retryFramework *retry.RetryFramework
}

// NewEventBasedWatcher supports objType=[factory.PodSelectorType, factory.NamespaceSelectorType]
func NewEventBasedWatcher(objType reflect.Type, inf cache.SharedIndexInformer, wf *factory.WatchFactory,
	stopChan <-chan struct{}, doneWg *sync.WaitGroup) *EventBasedWatcher {
	handlers := &EventBasedWatcher{
		objType:  objType,
		inf:      inf,
		nextIdx:  0,
		handlers: map[int]eventBasedHandlerWithSelector{},
		wf:       wf,
	}

	resourceHandler := &retry.ResourceHandler{
		HasUpdateFunc:          false,
		NeedsUpdateDuringRetry: false,
		ObjType:                objType,
		EventHandler:           handlers,
	}
	handlers.retryFramework = retry.NewRetryFramework(
		stopChan,
		doneWg,
		wf,
		resourceHandler,
	)

	return handlers
}

func (h *EventBasedWatcher) Watch() error {
	// already Watching
	if h.wfHandler != nil {
		return nil
	}
	wfHandler, err := h.retryFramework.WatchResource()

	if err != nil {
		return err
	}

	h.wfHandler = wfHandler
	return nil
}

func (h *EventBasedWatcher) Stop() {
	if h.wfHandler != nil {
		h.wf.RemoveHandler(h.objType, h.wfHandler)
		h.wfHandler = nil
	}
}

func (h *EventBasedWatcher) AddHandler(namespace string, selector labels.Selector, newHandler EventBasedHandler) (int, error) {
	h.lock.Lock()
	defer h.lock.Unlock()
	// handle existing objects
	selInfo := selectorInfo{
		namespace: namespace,
		selector:  selector,
		objType:   h.objType,
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
	if err := newHandler.ProcessExisting(items); err != nil {
		return NoHandler, fmt.Errorf("failed in ProcessExisting %v: %v", items, err)
	}

	idx := h.nextIdx
	h.nextIdx += 1
	h.handlers[idx] = eventBasedHandlerWithSelector{
		selInfo,
		newHandler,
	}
	return idx, nil
}

func (h *EventBasedWatcher) DeleteHandler(idx int) {
	h.lock.Lock()
	defer h.lock.Unlock()
	delete(h.handlers, idx)
}

// GetResourceFromInformerCache returns the latest state of the object, given an object key and its type.
// from the informers cache.
func (h *EventBasedWatcher) GetResourceFromInformerCache(key string) (interface{}, error) {
	var obj interface{}
	var namespace, name string
	var err error

	namespace, name, err = cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to split key %s: %v", key, err)
	}

	switch h.objType {
	case factory.PodSelectorType:
		obj, err = h.wf.GetPod(namespace, name)
	case factory.NamespaceSelectorType:
		obj, err = h.wf.GetNamespace(name)
	default:
		err = fmt.Errorf("object type %s not supported, cannot retrieve it from informers cache",
			h.objType)
	}
	return obj, err
}

// AddResource adds the specified object to the cluster according to its type and returns the error,
// if any, yielded during object creation.
// Given an object to add and a boolean specifying if the function was executed from iterateRetryResources
func (h *EventBasedWatcher) AddResource(obj interface{}, fromRetryLoop bool) error {
	h.lock.RLock()
	defer h.lock.RUnlock()
	for _, handler := range h.handlers {
		filtered, err := filterFunc(handler.selectorInfo, obj)
		if err != nil {
			return err
		}
		if filtered {
			err = handler.OnAdd(obj)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// UpdateResource updates the specified object in the cluster to its version in newObj according to its
// type and returns the error, if any, yielded during the object update.
// Given an old and a new object; The inRetryCache boolean argument is to indicate if the given resource
// is in the retryCache or not.
func (h *EventBasedWatcher) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	h.lock.RLock()
	defer h.lock.RUnlock()
	for _, handler := range h.handlers {
		newer, err := filterFunc(handler.selectorInfo, newObj)
		if err != nil {
			return err
		}
		older, err := filterFunc(handler.selectorInfo, oldObj)
		if err != nil {
			return err
		}
		switch {
		case newer && older:
			err = handler.OnUpdate(oldObj, newObj)
			if err != nil {
				return err
			}
		case newer && !older:
			err = handler.OnAdd(newObj)
			if err != nil {
				return err
			}
		case !newer && older:
			err = handler.OnDelete(oldObj)
			if err != nil {
				return err
			}
		default:
			// do nothing
		}
	}
	return nil
}

func (h *EventBasedWatcher) DeleteResource(obj, cachedObj interface{}) error {
	h.lock.RLock()
	defer h.lock.RUnlock()
	for _, handler := range h.handlers {
		filtered, err := filterFunc(handler.selectorInfo, obj)
		if err != nil {
			return err
		}
		if filtered {
			err = handler.OnDelete(obj)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// IsObjectInTerminalState returns true if the given object is a in terminal state.
// This is used now for pods that are either in a PodSucceeded or in a PodFailed state.
func (h *EventBasedWatcher) IsObjectInTerminalState(obj interface{}) bool {
	switch h.objType {
	case factory.PodSelectorType:
		pod := obj.(*kapi.Pod)
		return util.PodCompleted(pod)

	default:
		return false
	}
}
