// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AdvertisementType is a kind of route published by a RouteAdvertisements.
// +kubebuilder:validation:Enum=PodNetwork;EgressIP
type AdvertisementType string

const (
	// PodNetwork means the pod subnets of the network are advertised.
	PodNetwork AdvertisementType = "PodNetwork"
	// EgressIP means the EgressIPs of the network are advertised.
	EgressIP AdvertisementType = "EgressIP"
)

// NodeNetworkStatus reports the state of one network on one node. It is
// written by ovnkube-cluster-manager and read by the node that it names.
//
// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:path=nodenetworkstatuses,scope=Cluster
// +kubebuilder:singular=nodenetworkstatus
// +kubebuilder:object:root=true
// +kubebuilder:selectablefield:JSONPath=".spec.nodeName"
// +kubebuilder:selectablefield:JSONPath=".spec.networkName"
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=".spec.nodeName"
// +kubebuilder:printcolumn:name="Network",type=string,JSONPath=".spec.networkName"
// +kubebuilder:printcolumn:name="Advertised VRFs",type=string,JSONPath=".spec.routeAdvertisements[*].vrf"
type NodeNetworkStatus struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	// +required
	Spec NodeNetworkStatusSpec `json:"spec"`
}

type NodeNetworkStatusSpec struct {
	// NodeName is the node this state belongs to.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nodeName is immutable"
	// +required
	NodeName string `json:"nodeName"`

	// NetworkName is the OVN network this state belongs to.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="networkName is immutable"
	// +required
	NetworkName string `json:"networkName"`

	// RouteAdvertisements lists the RouteAdvertisements whose routes for this
	// network are published from this node. An entry is present only while
	// the corresponding FRR configuration exists, so the node can stop
	// translating pod traffic to its own address as soon as it sees one.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=64
	// +optional
	RouteAdvertisements []NodeNetworkRouteAdvertisement `json:"routeAdvertisements,omitempty"`
}

// NodeNetworkRouteAdvertisement is the contribution of one RouteAdvertisements
// to a network on a node.
type NodeNetworkRouteAdvertisement struct {
	// Name is the name of the RouteAdvertisements.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +required
	Name string `json:"name"`

	// VRF is the VRF the routes are advertised in. It is always resolved:
	// "default" for the default VRF, otherwise the VRF name, never "auto".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +required
	VRF string `json:"vrf"`

	// Advertisements lists what is advertised in that VRF.
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=2
	// +required
	Advertisements []AdvertisementType `json:"advertisements"`
}

// NodeNetworkStatusList contains a list of NodeNetworkStatus.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type NodeNetworkStatusList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeNetworkStatus `json:"items"`
}
