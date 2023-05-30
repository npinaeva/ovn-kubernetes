package selector_based_controllers

import (
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func GetPodSelectorKey(podSelector, namespaceSelector *metav1.LabelSelector, namespace string) string {
	var namespaceKey string
	if namespaceSelector == nil {
		// namespace is static
		namespaceKey = namespace
	} else {
		namespaceKey = shortLabelSelectorString(namespaceSelector)
	}
	return namespaceKey + "_" + shortLabelSelectorString(podSelector)
}

// sortedLSRString is based on *LabelSelectorRequirement.String(),
// but adds sorting for Values
func sortedLSRString(lsr *metav1.LabelSelectorRequirement) string {
	if lsr == nil {
		return "nil"
	}
	lsrValues := make([]string, 0, len(lsr.Values))
	lsrValues = append(lsrValues, lsr.Values...)
	sort.Strings(lsrValues)
	s := strings.Join([]string{`LSR{`,
		`Key:` + fmt.Sprintf("%v", lsr.Key) + `,`,
		`Operator:` + fmt.Sprintf("%v", lsr.Operator) + `,`,
		`Values:` + fmt.Sprintf("%v", lsrValues) + `,`,
		`}`,
	}, "")
	return s
}

// shortLabelSelectorString is based on *LabelSelector.String(),
// but makes sure to generate the same string for equivalent selectors (by additional sorting).
// It also tries to reduce return string length, since this string will be put to the db ad ExternalID.
func shortLabelSelectorString(sel *metav1.LabelSelector) string {
	if sel == nil {
		return "nil"
	}
	var repeatedStringForMatchExpressions, mapStringForMatchLabels string
	if len(sel.MatchExpressions) > 0 {
		repeatedStringForMatchExpressions = "ME:{"
		matchExpressions := make([]string, 0, len(sel.MatchExpressions))
		for _, f := range sel.MatchExpressions {
			matchExpressions = append(matchExpressions, sortedLSRString(&f))
		}
		// sort match expressions to not depend on MatchExpressions order
		sort.Strings(matchExpressions)
		repeatedStringForMatchExpressions += strings.Join(matchExpressions, ",")
		repeatedStringForMatchExpressions += "}"
	} else {
		repeatedStringForMatchExpressions = ""
	}
	keysForMatchLabels := make([]string, 0, len(sel.MatchLabels))
	for k := range sel.MatchLabels {
		keysForMatchLabels = append(keysForMatchLabels, k)
	}
	sort.Strings(keysForMatchLabels)
	if len(keysForMatchLabels) > 0 {
		mapStringForMatchLabels = "ML:{"
		for _, k := range keysForMatchLabels {
			mapStringForMatchLabels += fmt.Sprintf("%v: %v,", k, sel.MatchLabels[k])
		}
		mapStringForMatchLabels += "}"
	} else {
		mapStringForMatchLabels = ""
	}
	s := "LS{"
	if mapStringForMatchLabels != "" {
		s += mapStringForMatchLabels + ","
	}
	if repeatedStringForMatchExpressions != "" {
		s += repeatedStringForMatchExpressions + ","
	}
	s += "}"
	return s
}

func verifyNamespacedObjSelectors(objSelector, namespaceSelector *metav1.LabelSelector,
	namespace string) (objSel, nsSel labels.Selector, err error) {
	if objSelector == nil {
		err = fmt.Errorf("object selector is nil")
		return
	}
	if namespaceSelector == nil && namespace == "" {
		err = fmt.Errorf("namespace selector is nil and namespace is empty")
		return
	}
	if namespaceSelector != nil {
		// namespace will be ignored in this case
		namespace = ""
	}
	if namespaceSelector != nil {
		nsSel, err = metav1.LabelSelectorAsSelector(namespaceSelector)
		if err != nil {
			err = fmt.Errorf("can't parse namespace selector %v: %w", namespaceSelector, err)
			return
		}
	}

	objSel, err = metav1.LabelSelectorAsSelector(objSelector)
	if err != nil {
		err = fmt.Errorf("can't parse object selector %v: %w", objSelector, err)
		return
	}
	return
}
