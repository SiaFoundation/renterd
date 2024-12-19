package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMarshalHostScoreBreakdownJSON(t *testing.T) {
	hc := HostChecks{
		ScoreBreakdown: HostScoreBreakdown{
			Age:              1.1,
			Collateral:       1.1,
			Interactions:     1.1,
			StorageRemaining: 1.1,
			Uptime:           1.1,
			Version:          1.1,
			Prices:           1.1,
		},
	}
	b, err := json.MarshalIndent(hc, " ", " ")
	if err != nil {
		t.Fatal(err)
	} else if !strings.Contains(string(b), "\"score\": 1.9487171000000014") {
		t.Fatal("expected a score field")
	}
}
