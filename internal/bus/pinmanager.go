package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

type (
	// An AutopilotStore stores autopilots.
	AutopilotStore interface {
		Autopilot(ctx context.Context, id string) (api.Autopilot, error)
		UpdateAutopilot(ctx context.Context, ap api.Autopilot) error
	}

	// PinManager is a service that manages price pinning.
	PinManager interface {
		Close(context.Context) error
		Run()
		TriggerUpdate()
	}

	// A SettingStore stores settings.
	SettingStore interface {
		Setting(ctx context.Context, key string) (string, error)
		UpdateSetting(ctx context.Context, key, value string) error
	}
)

type (
	pinManager struct {
		a           alerts.Alerter
		as          AutopilotStore
		ss          SettingStore
		broadcaster webhooks.Broadcaster

		updateInterval time.Duration
		rateWindow     time.Duration

		triggerChan chan struct{}
		closedChan  chan struct{}
		wg          sync.WaitGroup

		logger *zap.SugaredLogger

		mu            sync.Mutex
		rates         []float64
		ratesCurrency string
	}
)

// NewPinManager returns a new PinManager, responsible for pinning prices to a
// fixed value in an underlying currency. Note that the manager that is being
// returned is not running, this can be done by calling Run().
func NewPinManager(alerts alerts.Alerter, broadcaster webhooks.Broadcaster, as AutopilotStore, ss SettingStore, updateInterval, rateWindow time.Duration, l *zap.Logger) *pinManager {
	return &pinManager{
		a:           alerts,
		as:          as,
		ss:          ss,
		broadcaster: broadcaster,

		logger: l.Sugar().Named("pricemanager"),

		updateInterval: updateInterval,
		rateWindow:     rateWindow,

		triggerChan: make(chan struct{}, 1),
		closedChan:  make(chan struct{}),
	}
}

func (pm *pinManager) Close(ctx context.Context) error {
	close(pm.closedChan)

	doneChan := make(chan struct{})
	go func() {
		pm.wg.Wait()
		close(doneChan)
	}()

	select {
	case <-doneChan:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (pm *pinManager) Run() {
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()

		t := time.NewTicker(pm.updateInterval)
		defer t.Stop()

		var forced bool
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			err := pm.updatePrices(ctx, forced)
			if err != nil {
				pm.logger.Warn("failed to update prices", zap.Error(err))
				pm.a.RegisterAlert(ctx, newPricePinningFailedAlert(err))
			} else {
				pm.a.DismissAlerts(ctx, alertPricePinningID)
			}
			cancel()

			forced = false
			select {
			case <-pm.closedChan:
				return
			case <-pm.triggerChan:
				forced = true
			case <-t.C:
			}
		}
	}()
}

func (pm *pinManager) TriggerUpdate() {
	select {
	case pm.triggerChan <- struct{}{}:
	default:
	}
}

func (pm *pinManager) averageRate() decimal.Decimal {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	median, _ := stats.Median(pm.rates)
	return decimal.NewFromFloat(median)
}

func (pm *pinManager) pinnedSettings(ctx context.Context) (api.PricePinSettings, error) {
	var ps api.PricePinSettings
	if pss, err := pm.ss.Setting(ctx, api.SettingPricePinning); err != nil {
		return api.PricePinSettings{}, err
	} else if err := json.Unmarshal([]byte(pss), &ps); err != nil {
		pm.logger.Panicf("failed to unmarshal pinned settings '%s': %v", pss, err)
	}
	return ps, nil
}

func (pm *pinManager) rateExceedsThreshold(threshold float64) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// calculate mean
	mean, err := stats.Mean(pm.rates)
	if err != nil {
		pm.logger.Warnw("failed to calculate average rate", zap.Error(err))
		return false
	}

	// convert to decimals
	avg := decimal.NewFromFloat(mean)
	pct := decimal.NewFromFloat(threshold)
	cur := decimal.NewFromFloat(pm.rates[len(pm.rates)-1])

	// calculate whether the current rate exceeds the given threshold
	delta := cur.Sub(avg).Abs()
	exceeded := delta.GreaterThan(cur.Mul(pct))

	// log the result
	pm.logger.Debugw("rate exceeds threshold",
		"last", cur,
		"average", avg,
		"percentage", threshold,
		"delta", delta,
		"threshold", cur.Mul(pct),
		"exceeded", exceeded,
	)
	return exceeded
}

func (pm *pinManager) updateAutopilotSettings(ctx context.Context, autopilotID string, pins api.AutopilotPins, rate decimal.Decimal) error {
	var updated bool

	ap, err := pm.as.Autopilot(ctx, autopilotID)
	if err != nil {
		return err
	}

	// update allowance
	if pins.Allowance.IsPinned() {
		update, err := convertCurrencyToSC(decimal.NewFromFloat(pins.Allowance.Value), rate)
		if err != nil {
			pm.logger.Warnw("failed to convert allowance to currency", zap.Error(err))
		} else {
			bkp := ap.Config.Contracts.Allowance
			ap.Config.Contracts.Allowance = update
			if err := ap.Config.Validate(); err != nil {
				pm.logger.Warnw("failed to update autopilot setting, new allowance makes the setting invalid", zap.Error(err))
				ap.Config.Contracts.Allowance = bkp
			} else {
				pm.logger.Infow("updating autopilot allowance", "old", bkp, "new", ap.Config.Contracts.Allowance, "rate", rate, "autopilot", autopilotID)
				updated = true
			}
		}
	}

	// return early if no updates took place
	if !updated {
		pm.logger.Infow("autopilots did not require price update", "rate", rate)
		return nil
	}

	// validate config
	err = ap.Config.Validate()
	if err != nil {
		pm.logger.Warnw("failed to update autopilot setting, new settings make the setting invalid", zap.Error(err))
		return err
	}

	// update autopilto
	return pm.as.UpdateAutopilot(ctx, ap)
}

func (pm *pinManager) updateExchangeRates(currency string, rate float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// update last currency
	if pm.ratesCurrency != currency {
		pm.ratesCurrency = currency
		pm.rates = nil
	}

	// update last rate
	pm.rates = append(pm.rates, rate)
	if len(pm.rates) >= int(pm.rateWindow/pm.updateInterval) {
		pm.rates = pm.rates[1:]
	}

	return nil
}

func (pm *pinManager) updateGougingSettings(ctx context.Context, pins api.GougingSettingsPins, rate decimal.Decimal) error {
	var updated bool

	// fetch gouging settings
	var gs api.GougingSettings
	if gss, err := pm.ss.Setting(ctx, api.SettingGouging); err != nil {
		return err
	} else if err := json.Unmarshal([]byte(gss), &gs); err != nil {
		pm.logger.Panicf("failed to unmarshal gouging settings '%s': %v", gss, err)
		return err
	}

	// update max download price
	if pins.MaxDownload.IsPinned() {
		update, err := convertCurrencyToSC(decimal.NewFromFloat(pins.MaxDownload.Value), rate)
		if err != nil {
			pm.logger.Warn("failed to convert max download price to currency")
		} else if !gs.MaxDownloadPrice.Equals(update) {
			gs.MaxDownloadPrice = update
			pm.logger.Infow("updating max download price", "old", gs.MaxDownloadPrice, "new", update, "rate", rate)
			updated = true
		}
	}

	// update max RPC price
	if pins.MaxRPCPrice.IsPinned() {
		update, err := convertCurrencyToSC(decimal.NewFromFloat(pins.MaxRPCPrice.Value), rate)
		if err != nil {
			pm.logger.Warnw("failed to convert max RPC price to currency", zap.Error(err))
		} else if !gs.MaxRPCPrice.Equals(update) {
			pm.logger.Infow("updating max RPC price", "old", gs.MaxRPCPrice, "new", update, "rate", rate)
			gs.MaxRPCPrice = update
			updated = true
		}
	}

	// update max storage price
	if pins.MaxStorage.IsPinned() {
		maxStorageCurr, err := convertCurrencyToSC(decimal.NewFromFloat(pins.MaxStorage.Value), rate)
		if err != nil {
			pm.logger.Warnw("failed to convert max storage price to currency", zap.Error(err))
		} else if update := maxStorageCurr.Div64(1e12).Div64(144 * 30); !gs.MaxStoragePrice.Equals(update) { // convert from SC/TB/month to SC/byte/block
			pm.logger.Infow("updating max storage price", "old", gs.MaxStoragePrice, "new", update, "rate", rate)
			gs.MaxStoragePrice = update
			updated = true
		}
	}

	// update max upload price
	if pins.MaxUpload.IsPinned() {
		update, err := convertCurrencyToSC(decimal.NewFromFloat(pins.MaxUpload.Value), rate)
		if err != nil {
			pm.logger.Warnw("failed to convert max upload price to currency", zap.Error(err))
		} else if !gs.MaxUploadPrice.Equals(update) {
			pm.logger.Infow("updating max upload price", "old", gs.MaxUploadPrice, "new", update, "rate", rate)
			gs.MaxUploadPrice = update
			updated = true
		}
	}

	// return early if no updates took place
	if !updated {
		pm.logger.Infow("gouging prices did not require price update", "rate", rate)
		return nil
	}

	// validate settings
	err := gs.Validate()
	if err != nil {
		pm.logger.Warnw("failed to update gouging setting, new settings make the setting invalid", zap.Error(err))
		return err
	}

	// update settings
	bytes, _ := json.Marshal(gs)
	err = pm.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes))

	// broadcast event
	if err == nil {
		pm.broadcaster.BroadcastAction(ctx, webhooks.Event{
			Module: api.ModuleSetting,
			Event:  api.EventUpdate,
			Payload: api.EventSettingUpdate{
				Key:       api.SettingGouging,
				Update:    string(bytes),
				Timestamp: time.Now().UTC(),
			},
		})
	}

	return err
}

func (pm *pinManager) updatePrices(ctx context.Context, forced bool) error {
	pm.logger.Debugw("updating prices", zap.Bool("forced", forced))

	// fetch pinned settings
	settings, err := pm.pinnedSettings(ctx)
	if errors.Is(err, api.ErrSettingNotFound) {
		pm.logger.Debug("price pinning not configured, skipping price update")
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to fetch pinned settings: %w", err)
	} else if !settings.Enabled {
		pm.logger.Debug("price pinning is disabled, skipping price update")
		return nil
	}

	// fetch exchange rate
	rate, err := NewForexClient(settings.ForexEndpointURL).SiacoinExchangeRate(ctx, settings.Currency)
	if err != nil {
		return fmt.Errorf("failed to fetch exchange rate for '%s': %w", settings.Currency, err)
	} else if rate <= 0 {
		return fmt.Errorf("exchange rate for '%s' must be positive: %f", settings.Currency, rate)
	}

	// update exchange rates
	err = pm.updateExchangeRates(settings.Currency, rate)
	if err != nil {
		return err
	}

	// return early if the rate does not exceed the threshold
	if !forced && !pm.rateExceedsThreshold(settings.Threshold) {
		pm.logger.Debug(
			"rate does not exceed threshold, skipping price update",
			zap.Stringer("threshold", decimal.NewFromFloat(settings.Threshold)),
			zap.Stringer("rate", decimal.NewFromFloat(rate)),
		)
		return nil
	}

	// update gouging settings
	update := pm.averageRate()
	err = pm.updateGougingSettings(ctx, settings.GougingSettingsPins, update)
	if err != nil {
		pm.logger.Warnw("failed to update gouging settings", zap.Error(err))
	}

	// update autopilot settings
	for ap, pins := range settings.Autopilots {
		err = pm.updateAutopilotSettings(ctx, ap, pins, update)
		if err != nil {
			pm.logger.Warnw("failed to update autopilot settings", zap.String("autopilot", ap), zap.Error(err))
		}
	}

	return nil
}

// convertCurrencyToSC converts a value in an external currency and an exchange
// rate to Siacoins.
func convertCurrencyToSC(target decimal.Decimal, rate decimal.Decimal) (types.Currency, error) {
	if rate.IsZero() {
		return types.Currency{}, nil
	}

	i := target.Div(rate).Mul(decimal.New(1, 24)).BigInt()
	if i.Sign() < 0 {
		return types.Currency{}, errors.New("negative currency")
	} else if i.BitLen() > 128 {
		return types.Currency{}, errors.New("currency overflow")
	}
	return types.NewCurrency(i.Uint64(), i.Rsh(i, 64).Uint64()), nil
}
