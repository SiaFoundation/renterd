package bus

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/alerts"
	"go.sia.tech/renterd/v2/api"
	"go.uber.org/zap"
)

const (
	testUpdateInterval = 100 * time.Millisecond
)

type mockAlerter struct {
	mu     sync.Mutex
	alerts []alerts.Alert
}

func (ma *mockAlerter) Alerts(ctx context.Context, opts alerts.AlertsOpts) (resp alerts.AlertsResponse, err error) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	return alerts.AlertsResponse{Alerts: ma.alerts}, nil
}

func (ma *mockAlerter) RegisterAlert(_ context.Context, a alerts.Alert) error {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	for _, alert := range ma.alerts {
		if alert.ID == a.ID {
			return nil
		}
	}
	ma.alerts = append(ma.alerts, a)
	return nil
}

func (ma *mockAlerter) DismissAlerts(_ context.Context, ids ...types.Hash256) error {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	for _, id := range ids {
		for i, a := range ma.alerts {
			if a.ID == id {
				ma.alerts = append(ma.alerts[:i], ma.alerts[i+1:]...)
				break
			}
		}
	}
	return nil
}

type mockExplorer struct {
	mu          sync.Mutex
	rate        float64
	unreachable bool
}

func (e *mockExplorer) Enabled() bool {
	return true
}

func (e *mockExplorer) BaseURL() string {
	return ""
}

func (e *mockExplorer) SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.unreachable {
		return 0, errors.New("unreachable")
	}
	return e.rate, nil
}

func (e *mockExplorer) setRate(rate float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rate = rate
}

func (e *mockExplorer) setUnreachable(unreachable bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.unreachable = unreachable
}

type mockPinStore struct {
	mu sync.Mutex
	gs api.GougingSettings
	ps api.PinnedSettings
}

func newTestStore() *mockPinStore {
	return &mockPinStore{
		gs: api.DefaultGougingSettings,
		ps: api.DefaultPinnedSettings,
	}
}

func (ms *mockPinStore) GougingSettings(ctx context.Context) (api.GougingSettings, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.gs, nil
}

func (ms *mockPinStore) UpdateGougingSettings(ctx context.Context, gs api.GougingSettings) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.gs = gs
	return nil
}

func (ms *mockPinStore) PinnedSettings(ctx context.Context) (api.PinnedSettings, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.ps, nil
}

func (ms *mockPinStore) UpdatePinnedSettings(ctx context.Context, ps api.PinnedSettings) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	b, err := json.Marshal(ps)
	if err != nil {
		return err
	}
	var cloned api.PinnedSettings
	if err := json.Unmarshal(b, &cloned); err != nil {
		return err
	}
	ms.ps = cloned
	return nil
}

func TestPinManager(t *testing.T) {
	// mock dependencies
	a := &mockAlerter{}
	e := &mockExplorer{rate: 1}
	s := newTestStore()

	// create a pinmanager
	pm := NewPinManager(a, e, s, testUpdateInterval, time.Minute, zap.NewNop())
	defer func() {
		if err := pm.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// waitForUpdate waits for the price manager to update
	waitForUpdate := func() {
		t.Helper()
		pm.triggerChan <- false
		time.Sleep(testUpdateInterval)
	}

	// enable price pinning
	ps := api.DefaultPinnedSettings
	ps.Currency = "usd"
	ps.Threshold = 0.5
	s.UpdatePinnedSettings(context.Background(), ps)

	// fetch current gouging settings
	gs, _ := s.GougingSettings(context.Background())

	// configure all pins but disable them for now
	ps.GougingSettingsPins.MaxDownload = api.Pin{Value: 3, Pinned: false}
	ps.GougingSettingsPins.MaxStorage = api.Pin{Value: 3, Pinned: false}
	ps.GougingSettingsPins.MaxUpload = api.Pin{Value: 3, Pinned: false}
	s.UpdatePinnedSettings(context.Background(), ps)

	// assert gouging settings are unchanged
	if gss, _ := s.GougingSettings(context.Background()); !reflect.DeepEqual(gs, gss) {
		t.Fatalf("expected gouging settings to be the same, got %v", gss)
	}

	// enable the max download pin
	ps.GougingSettingsPins.MaxDownload.Pinned = true
	s.UpdatePinnedSettings(context.Background(), ps)
	waitForUpdate()

	// assert prices are not updated
	if gss, _ := s.GougingSettings(context.Background()); !reflect.DeepEqual(gs, gss) {
		t.Fatalf("expected gouging settings to be the same, got %v expected %v", gss, gs)
	}

	// adjust and lower the threshold
	e.setRate(1.5)
	ps.Threshold = 0.05
	s.UpdatePinnedSettings(context.Background(), ps)
	waitForUpdate()

	// assert prices are updated
	if gss, _ := s.GougingSettings(context.Background()); gss.MaxDownloadPrice.Equals(gs.MaxDownloadPrice) {
		t.Fatalf("expected gouging settings to be updated, got %v = %v", gss.MaxDownloadPrice, gs.MaxDownloadPrice)
	}

	// enable the rest of the pins
	ps.GougingSettingsPins.MaxDownload.Pinned = true
	ps.GougingSettingsPins.MaxStorage.Pinned = true
	ps.GougingSettingsPins.MaxUpload.Pinned = true
	s.UpdatePinnedSettings(context.Background(), ps)
	waitForUpdate()

	// assert they're all updated
	if gss, _ := s.GougingSettings(context.Background()); gss.MaxDownloadPrice.Equals(gs.MaxDownloadPrice) ||
		gss.MaxStoragePrice.Equals(gs.MaxStoragePrice) ||
		gss.MaxUploadPrice.Equals(gs.MaxUploadPrice) {
		t.Fatalf("expected gouging settings to be updated, got %v = %v", gss, gs)
	}

	// increase rate so average isn't catching up to us
	e.setRate(3)

	// make explorer return an error
	e.setUnreachable(true)
	waitForUpdate()

	// assert alert was registered
	res, _ := a.Alerts(context.Background(), alerts.AlertsOpts{})
	if len(res.Alerts) == 0 {
		t.Fatalf("expected 1 alert, got %d", len(a.alerts))
	}

	// make explorer return a valid response
	e.setUnreachable(false)
	waitForUpdate()

	// assert alert was dismissed
	res, _ = a.Alerts(context.Background(), alerts.AlertsOpts{})
	if len(res.Alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(a.alerts))
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
		if result, err := convertCurrencyToSC(test.target, test.rate); test.err != nil {
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
