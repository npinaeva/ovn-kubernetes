package selector_based_handler

import (
	"fmt"
	"reflect"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"

	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type selectorInfo struct {
	namespace string
	selector  labels.Selector
	objType   reflect.Type
}

func filterFunc(o selectorInfo, obj interface{}) (bool, error) {
	objMeta, err := getObjectMeta(o.objType, obj)
	if err != nil {
		return false, fmt.Errorf("failed to check label selector for object %+v: %w", obj, err)
	}
	sel := o.selector
	namespace := o.namespace
	if namespace == "" && sel == nil {
		// Unfiltered handler
		return true, nil
	}

	if namespace != "" && objMeta.Namespace != namespace {
		return false, nil
	}
	if sel != nil && !sel.Matches(labels.Set(objMeta.Labels)) {
		return false, nil
	}
	return true, nil
}

func getObjectMeta(objType reflect.Type, obj interface{}) (*metav1.ObjectMeta, error) {
	switch objType {
	case factory.PodSelectorType:
		if pod, ok := obj.(*kapi.Pod); ok {
			return &pod.ObjectMeta, nil
		}
	case factory.NamespaceSelectorType:
		if namespace, ok := obj.(*kapi.Namespace); ok {
			return &namespace.ObjectMeta, nil
		}
	}
	return nil, fmt.Errorf("cannot get ObjectMeta from type %v", objType)
}
