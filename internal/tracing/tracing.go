package tracing

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	service        = "renterd"
	serviceVersion = "0.1.0"
	name           = "renterd"
)

var (
	Tracer = trace.NewNoopTracerProvider().Tracer("noop")
)

// Init initialises a new OpenTelemetry Tracer using information from the
// environment and process. For more information on available environment
// variables for configuration, check out
// https://opentelemetry.io/docs/reference/specification/sdk-environment-variables/.
func Init() error {
	resources, err := resource.New(context.Background(),
		resource.WithFromEnv(),
		resource.WithProcess(),
	)
	if err != nil {
		return err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resources),
	)
	otel.SetTracerProvider(provider)

	// Set global tracer.
	Tracer = otel.Tracer(name)

	return nil
}

// TracedHandler is a piece of http middleware that attaches a tracing span to
// every incoming request. The name used for the span is the path of the
// endpoint.
func TracedHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, span := Tracer.Start(req.Context(), req.URL.Path)
		defer span.End()

		h.ServeHTTP(w, req.WithContext(ctx))
	})
}
