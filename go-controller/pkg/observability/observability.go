package observability

import (
	"fmt"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	libovsdb "github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	libovsdbops "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdb/ops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
)

// OVN observ app IDs. Be sure to always add new apps in the end.
const (
	DropSamplingID = iota + 1
	ACLNewTrafficSamplingID
	ACLEstTrafficSamplingID
)

const DefaultObservabilityCollectorSetID = 28

type Manager struct {
	nbClient libovsdbclient.Client
	// collector set id -> collector UUID
	collectorUUIDs map[int]string
	samplingConfig *libovsdbops.SamplingConfig
}

func NewManager(nbClient libovsdbclient.Client) *Manager {
	return &Manager{
		nbClient:       nbClient,
		collectorUUIDs: make(map[int]string),
		samplingConfig: &libovsdbops.SamplingConfig{
			ACLSampleNew: true,
			ACLSampleEst: true,
		},
	}
}

func (m *Manager) Config() *libovsdbops.SamplingConfig {
	return m.samplingConfig
}

func (m *Manager) Init() error {
	if !config.OVNKubernetesFeature.EnableObservability {
		return nil
	}
	if err := m.setSamplingAppIDs(); err != nil {
		return err
	}
	if err := m.createDefaultCollector(); err != nil {
		return err
	}
	return nil
}

func (m *Manager) setSamplingAppIDs() error {
	var ops []libovsdb.Operation
	var err error
	for _, appConfig := range []struct {
		id   int
		name nbdb.SamplingAppName
	}{
		{
			id:   DropSamplingID,
			name: nbdb.SamplingAppNameDropSampling,
		},
		{
			id:   ACLNewTrafficSamplingID,
			name: nbdb.SamplingAppNameACLNewTrafficSampling,
		},
		{
			id:   ACLEstTrafficSamplingID,
			name: nbdb.SamplingAppNameACLEstTrafficSampling,
		},
	} {
		samplingApp := &nbdb.SamplingApp{
			ID:   appConfig.id,
			Name: appConfig.name,
		}
		ops, err = libovsdbops.CreateOrUpdateSamplingAppsOps(m.nbClient, ops, samplingApp)
		if err != nil {
			return fmt.Errorf("error creating or updating sampling app %s: %w", appConfig.name, err)
		}
	}
	_, err = libovsdbops.TransactAndCheck(m.nbClient, ops)
	return err
}

func (m *Manager) createDefaultCollector() error {
	collector := &nbdb.SampleCollector{
		SetID:       DefaultObservabilityCollectorSetID,
		Name:        "best-observability-collector",
		Probability: percentToProbability(100),
	}
	err := libovsdbops.CreateOrUpdateSampleCollector(m.nbClient, collector)
	m.collectorUUIDs[DefaultObservabilityCollectorSetID] = collector.UUID
	m.samplingConfig.CollectorUUIDs = append(m.samplingConfig.CollectorUUIDs, collector.UUID)
	return err
}

func percentToProbability(percent int) int {
	return 65535 * percent / 100
}
