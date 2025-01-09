package api

import (
	"go.sia.tech/jape"
	"go.sia.tech/renterd/internal/prometheus"
)

func WriteResponse(jc jape.Context, resp prometheus.Marshaller) {
	if resp == nil {
		return
	}

	var responseFormat string
	if jc.Check("failed to decode form", jc.DecodeForm("response", &responseFormat)) != nil {
		return
	}
	switch responseFormat {
	case "prometheus":
		enc := prometheus.NewEncoder(jc.ResponseWriter)
		if jc.Check("failed to marshal prometheus response", enc.Append(resp)) != nil {
			return
		}
	default:
		jc.Encode(resp)
	}
}
