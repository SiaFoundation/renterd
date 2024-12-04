package worker

import (
	"context"
	"fmt"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
)

const (
	keyGougingChecker contextKey = "GougingChecker"
)

type contextKey string

func GougingCheckerFromContext(ctx context.Context) (gouging.Checker, error) {
	gc, ok := ctx.Value(keyGougingChecker).(func() (gouging.Checker, error))
	if !ok {
		panic("no gouging checker attached to the context") // developer error
	}
	return gc()
}

func WithGougingChecker(ctx context.Context, cs gouging.ConsensusState, gp api.GougingParams) context.Context {
	return context.WithValue(ctx, keyGougingChecker, func() (gouging.Checker, error) {
		cs, err := cs.ConsensusState(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get consensus state: %w", err)
		}
		return newGougingChecker(gp.GougingSettings, cs), nil
	})
}

func newGougingChecker(settings api.GougingSettings, cs api.ConsensusState) gouging.Checker {
	return gouging.NewChecker(settings, cs, nil, nil)
}
