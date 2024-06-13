package node

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings/pin"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

const (
	testAutopilotID    = "default"
	testUpdateInterval = 100 * time.Millisecond
)

type mockERP struct {
	mu       sync.Mutex
	value    float64
	currency string
}

func (erp *mockERP) updateRate(value float64) {
	erp.mu.Lock()
	defer erp.mu.Unlock()
	erp.value = value
	time.Sleep(2 * testUpdateInterval)
}

type mockEventsBroadcaster struct{}

func (meb *mockEventsBroadcaster) BroadcastEvent(e webhooks.EventWebhook) {}

func newTestExchangeRateProvider() *mockERP {
	return &mockERP{currency: "usd", value: 1}
}

func (erp *mockERP) SiacoinExchangeRate(_ context.Context, currency string) (float64, error) {
	erp.mu.Lock()
	defer erp.mu.Unlock()

	if !strings.EqualFold(currency, erp.currency) {
		return 0, errors.New("currency not found")
	}
	return erp.value, nil
}

type mockStore struct {
	mu         sync.Mutex
	settings   map[string]string
	autopilots map[string]api.Autopilot
}

func newTestStore() *mockStore {
	s := &mockStore{
		autopilots: make(map[string]api.Autopilot),
		settings:   make(map[string]string),
	}

	// add default price pin - and gouging settings
	b, _ := json.Marshal(build.DefaultPricePinSettings)
	s.settings[api.SettingPricePinning] = string(b)
	b, _ = json.Marshal(build.DefaultGougingSettings)
	s.settings[api.SettingGouging] = string(b)

	// add default autopilot
	s.autopilots[testAutopilotID] = api.Autopilot{
		ID: testAutopilotID,
		Config: api.AutopilotConfig{
			Contracts: api.ContractsConfig{
				Allowance: types.Siacoins(1),
			},
		},
	}

	return s
}

func (ms *mockStore) gougingSettings() api.GougingSettings {
	val, err := ms.Setting(context.Background(), api.SettingGouging)
	if err != nil {
		panic(err)
	}
	var gs api.GougingSettings
	if err := json.Unmarshal([]byte(val), &gs); err != nil {
		panic(err)
	}
	return gs
}

func (ms *mockStore) updatPinnedSettings(pps api.PricePinSettings) {
	b, _ := json.Marshal(pps)
	ms.UpdateSetting(context.Background(), api.SettingPricePinning, string(b))
	time.Sleep(2 * testUpdateInterval)
}

func (ms *mockStore) Setting(ctx context.Context, key string) (string, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.settings[key], nil
}

func (ms *mockStore) UpdateSetting(ctx context.Context, key, value string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.settings[key] = value
	return nil
}

func (ms *mockStore) Autopilot(ctx context.Context, id string) (api.Autopilot, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.autopilots[id], nil
}

func (ms *mockStore) UpdateAutopilot(ctx context.Context, autopilot api.Autopilot) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.autopilots[autopilot.ID] = autopilot
	return nil
}

func TestPinManager(t *testing.T) {
	// mock dependencies
	ms := newTestStore()
	erp := newTestExchangeRateProvider()
	eb := &mockEventsBroadcaster{}

	// start a pinmanager
	pm := NewPinManager(erp, eb, ms, ms, testUpdateInterval, time.Minute, zap.NewNop())
	if err := pm.Run(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pm.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// define a small helper to fetch the price manager's rates
	rates := func() []float64 {
		t.Helper()
		pm.mu.Lock()
		defer pm.mu.Unlock()
		return pm.rates
	}

	// assert price manager is disabled by default
	if cnt := len(rates()); cnt != 0 {
		t.Fatalf("expected no rates, got %d", cnt)
	}

	// enable price pinning
	pps := build.DefaultPricePinSettings
	pps.Disabled = false
	pps.Currency = "usd"
	pps.Threshold = 0.5
	ms.updatPinnedSettings(pps)

	// assert price manager is running now
	if cnt := len(rates()); cnt < 1 {
		t.Fatal("expected at least one rate")
	}

	// update exchange rate and fetch current gouging settings
	erp.updateRate(2.5)
	gs := ms.gougingSettings()

	// configure all pins but disable them for now
	pps.GougingSettingsPins.MaxContractPrice = api.Pin{Value: 3, Pinned: false}
	pps.GougingSettingsPins.MaxDownload = api.Pin{Value: 3, Pinned: false}
	pps.GougingSettingsPins.MaxRPCPrice = api.Pin{Value: 3, Pinned: false}
	pps.GougingSettingsPins.MaxStorage = api.Pin{Value: 3, Pinned: false}
	pps.GougingSettingsPins.MaxUpload = api.Pin{Value: 3, Pinned: false}
	pps.GougingSettingsPins.MinMaxEphemeralAccount = api.Pin{Value: 3, Pinned: false}
	ms.updatPinnedSettings(pps)

	// assert gouging settings are unchanged
	if gss := ms.gougingSettings(); !reflect.DeepEqual(gs, gss) {
		t.Fatalf("expected gouging settings to be the same, got %v", gss)
	}

	// enable the max contractprice pin, with the threshold at 0.5 it should remain unchanged
	pps.GougingSettingsPins.MaxContractPrice.Pinned = true
	ms.updatPinnedSettings(pps)
	if gss := ms.gougingSettings(); !reflect.DeepEqual(gs, gss) {
		t.Fatalf("expected gouging settings to be the same, got %v", gss)
	}

	// lower the threshold, gouging settings should be updated
	pps.Threshold = 0.05
	ms.updatPinnedSettings(pps)
	if gss := ms.gougingSettings(); gss.MaxContractPrice.Equals(gs.MaxContractPrice) {
		t.Fatalf("expected gouging settings to be updated, got %v = %v", gss.MaxContractPrice, gs.MaxContractPrice)
	}

	// enable the rest of the pins
	pps.GougingSettingsPins.MaxDownload.Pinned = true
	pps.GougingSettingsPins.MaxRPCPrice.Pinned = true
	pps.GougingSettingsPins.MaxStorage.Pinned = true
	pps.GougingSettingsPins.MaxUpload.Pinned = true
	pps.GougingSettingsPins.MinMaxEphemeralAccount.Pinned = true
	ms.updatPinnedSettings(pps)

	// assert they're all updated
	if gss := ms.gougingSettings(); gss.MaxDownloadPrice.Equals(gs.MaxDownloadPrice) ||
		gss.MaxRPCPrice.Equals(gs.MaxRPCPrice) ||
		gss.MaxStoragePrice.Equals(gs.MaxStoragePrice) ||
		gss.MaxUploadPrice.Equals(gs.MaxUploadPrice) ||
		gss.MinMaxEphemeralAccountBalance.Equals(gs.MinMaxEphemeralAccountBalance) {
		t.Fatalf("expected gouging settings to be updated, got %v = %v", gss, gs)
	}

	// increase rate so average isn't catching up to us
	erp.updateRate(3)

	// fetch autopilot
	ap, _ := ms.Autopilot(context.Background(), testAutopilotID)

	// add autopilot pin, but disable it
	pins := api.AutopilotPins{
		Allowance: api.Pin{
			Pinned: false,
			Value:  2,
		},
	}
	pps.Autopilots = map[string]api.AutopilotPins{testAutopilotID: pins}
	ms.updatPinnedSettings(pps)

	// assert autopilot was not updated
	if app, _ := ms.Autopilot(context.Background(), testAutopilotID); !app.Config.Contracts.Allowance.Equals(ap.Config.Contracts.Allowance) {
		t.Fatalf("expected autopilot to not be updated, got %v = %v", app.Config.Contracts.Allowance, ap.Config.Contracts.Allowance)
	}

	// enable the pin
	pins.Allowance.Pinned = true
	pps.Autopilots[testAutopilotID] = pins
	ms.updatPinnedSettings(pps)

	// assert autopilot was updated
	if app, _ := ms.Autopilot(context.Background(), testAutopilotID); app.Config.Contracts.Allowance.Equals(ap.Config.Contracts.Allowance) {
		t.Fatalf("expected autopilot to be updated, got %v = %v", app.Config.Contracts.Allowance, ap.Config.Contracts.Allowance)
	}
}

// TestConvertConvertCurrencyToSC tests the conversion of a currency to Siacoins.
func TestConvertConvertCurrencyToSC(t *testing.T) {
	tests := []struct {
		target   decimal.Decimal
		rate     decimal.Decimal
		expected types.Currency
		err      error
	}{
		{decimal.NewFromFloat(1), decimal.NewFromFloat(1), types.Siacoins(1), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(2), types.Siacoins(1).Div64(2), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(0.5), types.Siacoins(2), nil},
		{decimal.NewFromFloat(0.5), decimal.NewFromFloat(0.5), types.Siacoins(1), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(0.001), types.Siacoins(1000), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(0), types.Currency{}, nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(-1), types.Currency{}, errors.New("negative currency")},
		{decimal.NewFromFloat(-1), decimal.NewFromFloat(1), types.Currency{}, errors.New("negative currency")},
		{decimal.New(1, 50), decimal.NewFromFloat(0.1), types.Currency{}, errors.New("currency overflow")},
	}
	for i, test := range tests {
		if result, err := pin.ConvertCurrencyToSC(test.target, test.rate); test.err != nil {
			if err == nil {
				t.Fatalf("%d: expected error, got nil", i)
			} else if err.Error() != test.err.Error() {
				t.Fatalf("%d: expected %v, got %v", i, test.err, err)
			}
		} else if !test.expected.Equals(result) {
			t.Fatalf("%d: expected %d, got %d", i, test.expected, result)
		}
	}
}
