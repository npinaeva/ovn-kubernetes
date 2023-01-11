/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// Code generated by lister-gen. DO NOT EDIT.

package v1

import (
	v1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/podnetwork/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// PodNetworkLister helps list PodNetworks.
// All objects returned here must be treated as read-only.
type PodNetworkLister interface {
	// List lists all PodNetworks in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1.PodNetwork, err error)
	// PodNetworks returns an object that can list and get PodNetworks.
	PodNetworks(namespace string) PodNetworkNamespaceLister
	PodNetworkListerExpansion
}

// podNetworkLister implements the PodNetworkLister interface.
type podNetworkLister struct {
	indexer cache.Indexer
}

// NewPodNetworkLister returns a new PodNetworkLister.
func NewPodNetworkLister(indexer cache.Indexer) PodNetworkLister {
	return &podNetworkLister{indexer: indexer}
}

// List lists all PodNetworks in the indexer.
func (s *podNetworkLister) List(selector labels.Selector) (ret []*v1.PodNetwork, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.PodNetwork))
	})
	return ret, err
}

// PodNetworks returns an object that can list and get PodNetworks.
func (s *podNetworkLister) PodNetworks(namespace string) PodNetworkNamespaceLister {
	return podNetworkNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// PodNetworkNamespaceLister helps list and get PodNetworks.
// All objects returned here must be treated as read-only.
type PodNetworkNamespaceLister interface {
	// List lists all PodNetworks in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1.PodNetwork, err error)
	// Get retrieves the PodNetwork from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1.PodNetwork, error)
	PodNetworkNamespaceListerExpansion
}

// podNetworkNamespaceLister implements the PodNetworkNamespaceLister
// interface.
type podNetworkNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all PodNetworks in the indexer for a given namespace.
func (s podNetworkNamespaceLister) List(selector labels.Selector) (ret []*v1.PodNetwork, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.PodNetwork))
	})
	return ret, err
}

// Get retrieves the PodNetwork from the indexer for a given namespace and name.
func (s podNetworkNamespaceLister) Get(name string) (*v1.PodNetwork, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1.Resource("podnetwork"), name)
	}
	return obj.(*v1.PodNetwork), nil
}
