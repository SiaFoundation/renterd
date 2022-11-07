package api

import (
	"encoding/json"
	"fmt"

	"go.sia.tech/renterd/slab"
)

// toHostInteractions receives an input of any kind and tries to transform it
// into a list of host interactions. If the input type is not recognized a panic
// is thrown as it indicates a developer error.
func toHostInteractions(in interface{}) []HostInteraction {
	switch t := in.(type) {
	case []slab.HostInteraction:
		his := make([]HostInteraction, len(in.([]slab.HostInteraction)))
		for i, shi := range in.([]slab.HostInteraction) {
			his[i] = transformSlabHostInteraction(shi)
		}
		return his
	default:
		panic(fmt.Sprintf("unknown type '%v'", t))
	}
}

// transformSlabHostInteraction transforms a HostInteraction from the slab
// package to a HostInteraction from the api package.
func transformSlabHostInteraction(shi slab.HostInteraction) HostInteraction {
	hi := HostInteraction{
		Timestamp: shi.Timestamp.Unix(),
		HostKey:   shi.HostKey,
		Type:      shi.Type,
		Error:     shi.Error(),
	}

	durInMS := shi.Duration.Milliseconds()
	if durInMS == 0 && shi.Duration > 0 {
		durInMS = 1
	}

	hi.Metadata, _ = json.Marshal(struct {
		Duration int64  `json:"dur"`
		NumRoots uint64 `json:"n,omitempty"`
	}{
		Duration: durInMS,
		NumRoots: shi.NumRoots,
	})

	return hi
}
