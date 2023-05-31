package selector_based_controllers

import (
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = ginkgo.Describe("utils: shortLabelSelectorString function", func() {
	ginkgo.It("handles LabelSelectorRequirement.Values order", func() {
		ls1 := &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "key",
				Operator: "",
				Values:   []string{"v1", "v2", "v3"},
			}},
		}
		ls2 := &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "key",
				Operator: "",
				Values:   []string{"v3", "v2", "v1"},
			}},
		}
		gomega.Expect(shortLabelSelectorString(ls1)).To(gomega.Equal(shortLabelSelectorString(ls2)))
	})
	ginkgo.It("handles MatchExpressions order", func() {
		ls1 := &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      "key1",
					Operator: "",
					Values:   []string{"v1", "v2", "v3"},
				},
				{
					Key:      "key2",
					Operator: "",
					Values:   []string{"v1", "v2", "v3"},
				},
				{
					Key:      "key3",
					Operator: "",
					Values:   []string{"v1", "v2", "v3"},
				},
			},
		}
		ls2 := &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      "key2",
					Operator: "",
					Values:   []string{"v1", "v2", "v3"},
				},
				{
					Key:      "key1",
					Operator: "",
					Values:   []string{"v1", "v2", "v3"},
				},
				{
					Key:      "key3",
					Operator: "",
					Values:   []string{"v1", "v2", "v3"},
				},
			},
		}
		gomega.Expect(shortLabelSelectorString(ls1)).To(gomega.Equal(shortLabelSelectorString(ls2)))
	})
	ginkgo.It("handles MatchLabels order", func() {
		ls1 := &metav1.LabelSelector{
			MatchLabels: map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"},
		}
		ls2 := &metav1.LabelSelector{
			MatchLabels: map[string]string{"k2": "v2", "k1": "v1", "k3": "v3"},
		}
		gomega.Expect(shortLabelSelectorString(ls1)).To(gomega.Equal(shortLabelSelectorString(ls2)))
	})
})
