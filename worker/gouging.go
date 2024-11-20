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

func GougingCheckerFromContext(ctx context.Context, criticalMigration bool) (gouging.Checker, error) {
	gc, ok := ctx.Value(keyGougingChecker).(func(bool) (gouging.Checker, error))
	if !ok {
		panic("no gouging checker attached to the context") // developer error
	}
	return gc(criticalMigration)
}

func WithGougingChecker(ctx context.Context, cs gouging.ConsensusState, gp api.GougingParams) context.Context {
	return context.WithValue(ctx, keyGougingChecker, func(criticalMigration bool) (gouging.Checker, error) {
		cs, err := cs.ConsensusState(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get consensus state: %w", err)
		}
		return newGougingChecker(gp.GougingSettings, cs, criticalMigration), nil
	})
}

func newGougingChecker(settings api.GougingSettings, cs api.ConsensusState, criticalMigration bool) gouging.Checker {
	// adjust the max download price if we are dealing with a critical
	// migration that might be failing due to gouging checks
	if criticalMigration && settings.MigrationSurchargeMultiplier > 0 {
		if adjustedMaxDownloadPrice, overflow := settings.MaxDownloadPrice.Mul64WithOverflow(settings.MigrationSurchargeMultiplier); !overflow {
			settings.MaxDownloadPrice = adjustedMaxDownloadPrice
		}
	}
	return gouging.NewChecker(settings, cs)
}
