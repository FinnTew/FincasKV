package tests

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"
)

func TestFincasKV(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FincasKV E2E Suite")
}
