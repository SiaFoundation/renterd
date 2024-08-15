package worker

import (
	"context"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
)

const (
	keyGougingChecker contextKey = "GougingChecker"
)

type contextKey string

func GougingCheckerFromContext(ctx context.Context, criticalMigration bool) (gouging.Checker, error) {
	gc, ok := ctx.Value(keyGougingChecker).(func(bool) (gouging.Checker, error))
	if !ok {
		panic("no gouging checker attached to the context") // developer error
	}
	return gc(criticalMigration)
}

func WithGougingChecker(ctx context.Context, cs gouging.ConsensusState, gp api.GougingParams) context.Context {
	return context.WithValue(ctx, keyGougingChecker, func(criticalMigration bool) (gouging.Checker, error) {
		return gouging.NewWorkerGougingChecker(ctx, cs, gp, criticalMigration)
	})
}
