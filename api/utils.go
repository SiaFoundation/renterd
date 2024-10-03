package api

import (
	"fmt"
	"net/http"

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

		v, ok := resp.(prometheus.Marshaller)
		if !ok {
			jc.Error(fmt.Errorf("type %T is not prometheus marshallable", resp), http.StatusInternalServerError)
			return
		}

		if jc.Check("failed to marshal prometheus response", enc.Append(v)) != nil {
			return
		}

	default:
		jc.Encode(resp)
	}
}
