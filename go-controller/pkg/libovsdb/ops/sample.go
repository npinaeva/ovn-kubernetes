package ops

import (
	"hash/fnv"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/model"
	libovsdb "github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
)

func CreateOrUpdateSampleCollector(nbClient libovsdbclient.Client, collector *nbdb.SampleCollector) error {
	opModel := operationModel{
		Model:          collector,
		OnModelUpdates: onModelUpdatesAllNonDefault(),
		ErrNotFound:    false,
		BulkOp:         false,
	}

	m := newModelClient(nbClient)
	_, err := m.CreateOrUpdate(opModel)
	return err
}

func UpdateDefaultSampleCollectorsOps(nbClient libovsdbclient.Client, ops []libovsdb.Operation, sample *nbdb.Sample) ([]libovsdb.Operation, error) {
	opModel := operationModel{
		Model:          sample,
		OnModelUpdates: []interface{}{sample.Collectors},
		ErrNotFound:    false,
		BulkOp:         false,
	}

	modelClient := newModelClient(nbClient)
	return modelClient.CreateOrUpdateOps(ops, opModel)
}

func CreateOrUpdateSamplingAppsOps(nbClient libovsdbclient.Client, ops []libovsdb.Operation, samplingApps ...*nbdb.SamplingApp) ([]libovsdb.Operation, error) {
	opModels := make([]operationModel, 0, len(samplingApps))
	for i := range samplingApps {
		// can't use i in the predicate, for loop replaces it in-memory
		samplingApp := samplingApps[i]
		opModel := operationModel{
			Model:          samplingApp,
			OnModelUpdates: onModelUpdatesAllNonDefault(),
			ErrNotFound:    false,
			BulkOp:         false,
		}
		opModels = append(opModels, opModel)
	}

	modelClient := newModelClient(nbClient)
	return modelClient.CreateOrUpdateOps(ops, opModels...)
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

type SamplingConfig struct {
	CollectorUUIDs []string
	ACLSampleNew   bool
	ACLSampleEst   bool
}

func addSample(c *SamplingConfig, opModels []operationModel, model model.Model) []operationModel {
	if c == nil || len(c.CollectorUUIDs) == 0 || !config.OVNKubernetesFeature.EnableObservability {
		return opModels
	}
	switch t := model.(type) {
	case *nbdb.ACL:
		return createOrUpdateSampleForACL(opModels, c, t)
	}
	return opModels
}

func getSampleMutableFields(sample *nbdb.Sample) []interface{} {
	return []interface{}{&sample.Collectors}
}

// createOrUpdateSampleForACL should be called before acl operationModel is appended to opModels.
func createOrUpdateSampleForACL(opModels []operationModel, c *SamplingConfig, acl *nbdb.ACL) []operationModel {
	if !c.ACLSampleEst && !c.ACLSampleNew {
		return opModels
	}
	sample := &nbdb.Sample{
		Collectors: c.CollectorUUIDs,
		// 32 bits
		Metadata: int(hashString(acl.ExternalIDs[PrimaryIDKey.String()])),
	}
	opModel := operationModel{
		Model: sample,
		DoAfter: func() {
			if c.ACLSampleEst {
				acl.SampleEst = &sample.UUID
			}
			if c.ACLSampleNew {
				acl.SampleNew = &sample.UUID
			}
		},
		OnModelUpdates: getSampleMutableFields(sample),
		ErrNotFound:    false,
		BulkOp:         false,
	}
	opModels = append(opModels, opModel)
	return opModels
}

func FindSample(nbClient libovsdbclient.Client, sampleMetadata int) (*nbdb.Sample, error) {
	sample := &nbdb.Sample{
		Metadata: sampleMetadata,
	}
	opModel := operationModel{
		Model:       sample,
		ErrNotFound: true,
		BulkOp:      false,
	}
	modelClient := newModelClient(nbClient)
	err := modelClient.Lookup(opModel)
	if err != nil {
		return nil, err
	}
	return sample, err
}
