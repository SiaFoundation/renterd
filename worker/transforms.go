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
func transformMetricSlabTransfer(st slab.MetricSlabTransfer) HostInteraction {
	hi := HostInteraction{
		Timestamp: st.Timestamp.Unix(),
		HostKey:   st.HostKey,
		Type:      st.Type,
		Error:     st.Error(),
	}

	durInMS := st.Duration.Milliseconds()
	if durInMS == 0 && st.Duration > 0 {
		durInMS = 1
	}

	hi.Metadata, _ = json.Marshal(struct {
		Duration int64 `json:"dur"`
	}{
		Duration: durInMS,
	})

	return hi
}

// transformMetricSlabDeletion transforms the given metric to a host interaction.
func transformMetricSlabDeletion(st slab.MetricSlabDeletion) HostInteraction {
	hi := HostInteraction{
		Timestamp: st.Timestamp.Unix(),
		HostKey:   st.HostKey,
		Type:      st.Type,
		Error:     st.Error(),
	}

	durInMS := st.Duration.Milliseconds()
	if durInMS == 0 && st.Duration > 0 {
		durInMS = 1
	}

	hi.Metadata, _ = json.Marshal(struct {
		Duration int64  `json:"dur"`
		NumRoots uint64 `json:"roots"`
	}{
		Duration: durInMS,
		NumRoots: st.NumRoots,
	})

	return hi
}
