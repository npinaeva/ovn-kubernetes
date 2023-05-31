package ovn

import (
	"fmt"
	"reflect"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"k8s.io/client-go/tools/cache"
)

// newNetpolRetryFramework builds and returns a retry framework for the input resource
// type and assigns all ovnk-master-specific function attributes in the returned struct;
// these functions will then be called by the retry logic in the retry package when
// WatchResource() is called.
// newNetpolRetryFramework takes as input a resource type (required)
// and the following optional parameters: a namespace and a label filter for the
// shared informer, a sync function to process all objects of this type at startup,
// and resource-specific extra parameters (used now for network-policy-dependant types).
// newNetpolRetryFramework is also called directly by the watchers that are
// dynamically created when a network policy is added:
// PeerNamespaceSelectorType
func (bnc *BaseNetworkController) newNetpolRetryFramework(
	objectType reflect.Type,
	syncFunc func([]interface{}) error,
	extraParameters interface{}) *retry.RetryFramework {
	eventHandler := &networkControllerPolicyEventHandler{
		objType:         objectType,
		watchFactory:    bnc.watchFactory,
		bnc:             bnc,
		extraParameters: extraParameters, // in use by network policy dynamic watchers
		syncFunc:        syncFunc,
	}
	resourceHandler := &retry.ResourceHandler{
		HasUpdateFunc:          hasResourceAnUpdateFunc(objectType),
		NeedsUpdateDuringRetry: needsUpdateDuringRetry(objectType),
		ObjType:                objectType,
		EventHandler:           eventHandler,
	}
	return retry.NewRetryFramework(
		bnc.stopChan,
		bnc.wg,
		bnc.watchFactory,
		resourceHandler,
	)
}

// event handlers handles policy related events
type networkControllerPolicyEventHandler struct {
	retry.EmptyEventHandler
	watchFactory    *factory.WatchFactory
	objType         reflect.Type
	bnc             *BaseNetworkController
	extraParameters interface{}
	syncFunc        func([]interface{}) error
}

// GetResourceFromInformerCache returns the latest state of the object, given an object key and its type.
// from the informers cache.
func (h *networkControllerPolicyEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	var obj interface{}
	var name string
	var err error

	_, name, err = cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to split key %s: %v", key, err)
	}

	switch h.objType {
	case factory.PeerNamespaceSelectorType:
		obj, err = h.watchFactory.GetNamespace(name)

	default:
		err = fmt.Errorf("object type %s not supported, cannot retrieve it from informers cache",
			h.objType)
	}
	return obj, err
}

// AddResource adds the specified object to the cluster according to its type and returns the error,
// if any, yielded during object creation.
// Given an object to add and a boolean specifying if the function was executed from iterateRetryResources
func (h *networkControllerPolicyEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	switch h.objType {
	case factory.PeerNamespaceSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerNamespaceSelectorAdd(extraParameters.np, extraParameters.gp, obj)

	default:
		return fmt.Errorf("no add function for object type %s", h.objType)
	}
}

// UpdateResource updates the specified object in the cluster to its version in newObj according to its
// type and returns the error, if any, yielded during the object update.
// Given an old and a new object; The inRetryCache boolean argument is to indicate if the given resource
// is in the retryCache or not.
func (h *networkControllerPolicyEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	return fmt.Errorf("no update function for object type %s", h.objType)
}

// DeleteResource deletes the object from the cluster according to the delete logic of its resource type.
// Given an object and optionally a cachedObj; cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *networkControllerPolicyEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	switch h.objType {
	case factory.PeerNamespaceSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerNamespaceSelectorDel(extraParameters.np, extraParameters.gp, obj)
	default:
		return fmt.Errorf("object type %s not supported", h.objType)
	}
}
