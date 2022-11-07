package api

import (
	"encoding/json"
	"fmt"

	"go.sia.tech/renterd/slab"
)

func toHostInteractions(in interface{}) []HostInteraction {
	switch t := in.(type) {
	case []*slab.HostInteraction:
		his := make([]HostInteraction, len(in.([]*slab.HostInteraction)))
		for i, shi := range in.([]*slab.HostInteraction) {
			his[i] = transformSlabHostInteraction(shi)
		}
		return his
	default:
		panic(fmt.Sprintf("unknown type '%v'", t))
	}
}

func transformSlabHostInteraction(shi *slab.HostInteraction) HostInteraction {
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
