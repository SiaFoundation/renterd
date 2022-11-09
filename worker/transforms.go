package worker

import (
	"encoding/json"
	"fmt"

	"go.sia.tech/renterd/slab"
)

// toHostInteractions receives an input of any kind and tries to transform it
// into a list of host interactions. If the input type is not recognized a panic
// is thrown as it indicates a developer error.
func toHostInteractions(ins []slab.TransferMetric) []HostInteraction {
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
	}

	if st.Err != nil {
		hi.Error = st.Err.Error()
	}

	hi.Result, _ = json.Marshal(struct {
		Duration int64 `json:"dur"`
	}{
		Duration: st.Duration.Milliseconds(),
	})

	return hi
}

// transformMetricSlabDeletion transforms the given metric to a host interaction.
func transformMetricSlabDeletion(sd slab.MetricSlabDeletion) HostInteraction {
	hi := HostInteraction{
		Timestamp: sd.Timestamp.Unix(),
		HostKey:   sd.HostKey,
		Type:      sd.Type,
	}

	if sd.Err != nil {
		hi.Error = sd.Err.Error()
	}

	hi.Result, _ = json.Marshal(struct {
		Duration int64  `json:"dur"`
		NumRoots uint64 `json:"roots"`
	}{
		Duration: sd.Duration.Milliseconds(),
		NumRoots: sd.NumRoots,
	})

	return hi
}
