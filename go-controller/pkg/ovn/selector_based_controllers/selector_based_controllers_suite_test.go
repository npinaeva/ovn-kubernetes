package selector_based_controllers

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSelectorBasedControllers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Selector Based Controller Operations Suite")
}
