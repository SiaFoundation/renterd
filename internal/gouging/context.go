package gouging

import (
	"context"
	"fmt"

	"go.sia.tech/renterd/api"
)

const (
	keyGougingChecker contextKey = "GougingChecker"
)

type contextKey string

func CheckerFromContext(ctx context.Context) (Checker, error) {
	gc, ok := ctx.Value(keyGougingChecker).(func() (Checker, error))
	if !ok {
		panic("no gouging checker attached to the context") // developer error
	}
	return gc()
}

func WithChecker(ctx context.Context, cs ConsensusState, gp api.GougingParams) context.Context {
	return context.WithValue(ctx, keyGougingChecker, func() (Checker, error) {
		cs, err := cs.ConsensusState(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get consensus state: %w", err)
		}
		return newGougingChecker(gp.GougingSettings, cs), nil
	})
}

func newGougingChecker(settings api.GougingSettings, cs api.ConsensusState) Checker {
	return NewChecker(settings, cs)
}
