package worker

import (
	"encoding/json"
	"fmt"

	"go.sia.tech/renterd/internal/observability"
	"go.sia.tech/renterd/slab"
)

// toHostInteractions receives an input of any kind and tries to transform it
// into a list of host interactions. If the input type is not recognized a panic
// is thrown as it indicates a developer error.
func toHostInteractions(ins []observability.Metric) []HostInteraction {
	his := make([]HostInteraction, len(ins))
	for i, in := range ins {
		switch t := in.(type) {
		case slab.MetricSlabTransfer:
			his[i] = transformMetricSlabTransfer(in.(slab.MetricSlabTransfer))
		case slab.MetricSlabDeletion:
			his[i] = transformMetricSlabDeletion(in.(slab.MetricSlabDeletion))
		default:
			panic(fmt.Sprintf("unknown type '%v'", t))
		}
	}
	return his
}

// transformSlabHostInteraction transforms the given metric to a host interaction.
func transformMetricSlabTransfer(st slab.MetricSlabTransfer) (hi HostInteraction) {
	decorateMetricsCommon(&hi, st.MetricSlabCommon)

	hi.Result, _ = json.Marshal(struct {
		Duration int64 `json:"dur"`
	}{
		Duration: st.Duration.Milliseconds(),
	})

	return hi
}

// transformMetricSlabDeletion transforms the given metric to a host interaction.
func transformMetricSlabDeletion(st slab.MetricSlabDeletion) (hi HostInteraction) {
	decorateMetricsCommon(&hi, st.MetricSlabCommon)

	hi.Result, _ = json.Marshal(struct {
		Duration int64  `json:"dur"`
		NumRoots uint64 `json:"roots"`
	}{
		Duration: st.Duration.Milliseconds(),
		NumRoots: st.NumRoots,
	})

	return hi
}

// decorateMetricsCommon decorates the given host interface with all common
// metrics.
func decorateMetricsCommon(hi *HostInteraction, sc slab.MetricSlabCommon) {
	hi.Timestamp = sc.Timestamp.Unix()
	hi.HostKey = sc.HostKey
	hi.Type = sc.Type

	if sc.Err != nil {
		hi.Error = sc.Err.Error()
	}
}
