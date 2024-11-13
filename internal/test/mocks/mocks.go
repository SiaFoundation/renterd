package mocks

import (
	"context"
	"errors"
	"log"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/memory"
	"go.sia.tech/renterd/webhooks"
)

type accountsMock struct{}

func (*accountsMock) Accounts(context.Context, string) ([]api.Account, error) {
	return nil, nil
}

func (*accountsMock) UpdateAccounts(context.Context, []api.Account) error {
	return nil
}

var _ alerts.Alerter = (*alerterMock)(nil)

type alerterMock struct{}

func (*alerterMock) Alerts(_ context.Context, opts alerts.AlertsOpts) (resp alerts.AlertsResponse, err error) {
	return alerts.AlertsResponse{}, nil
}
func (*alerterMock) RegisterAlert(context.Context, alerts.Alert) error     { return nil }
func (*alerterMock) DismissAlerts(context.Context, ...types.Hash256) error { return nil }

var _ gouging.ConsensusState = (*Chain)(nil)

type Chain struct {
	cs api.ConsensusState
}

func NewChain(cs api.ConsensusState) *Chain {
	return &Chain{cs: cs}
}

func (c *Chain) ConsensusState(ctx context.Context) (api.ConsensusState, error) {
	return c.cs, nil
}

func (c *Chain) UpdateHeight(bh uint64) {
	c.cs.BlockHeight = bh
}

type busMock struct {
	*alerterMock
	*accountsMock
	*Chain
	*contractLockerMock
	*ContractStore
	*HostStore
	*ObjectStore
	*settingStoreMock
	*syncerMock
	*s3Mock
	*webhookBroadcasterMock
	*webhookStoreMock
}

func NewBus(cs *ContractStore, hs *HostStore, os *ObjectStore) *busMock {
	return &busMock{
		alerterMock:            &alerterMock{},
		accountsMock:           &accountsMock{},
		Chain:                  &Chain{},
		contractLockerMock:     newContractLockerMock(),
		ContractStore:          cs,
		HostStore:              hs,
		ObjectStore:            os,
		settingStoreMock:       &settingStoreMock{},
		syncerMock:             &syncerMock{},
		webhookBroadcasterMock: &webhookBroadcasterMock{},
	}
}

func (b *busMock) FundAccount(ctx context.Context, acc rhpv3.Account, fcid types.FileContractID, desired types.Currency) (types.Currency, error) {
	return types.ZeroCurrency, nil
}

var ErrSectorOutOfBounds = errors.New("sector out of bounds")

type Host struct {
	hk types.PublicKey
	hi api.Host
}

func NewHost(hk types.PublicKey) *Host {
	return &Host{
		hk: hk,
		hi: api.Host{
			PublicKey: hk,
			Scanned:   true,
		},
	}
}

func (h *Host) UpdatePriceTable(pt api.HostPriceTable) {
	h.hi.PriceTable = pt
}

func (h *Host) PriceTable() api.HostPriceTable {
	return h.hi.PriceTable
}

func (h *Host) PublicKey() types.PublicKey {
	return h.hk
}

type (
	Memory        struct{}
	MemoryManager struct{ memBlockChan chan struct{} }
)

func NewMemoryManager() *MemoryManager {
	mm := &MemoryManager{memBlockChan: make(chan struct{})}
	close(mm.memBlockChan)
	return mm
}

func (mm *MemoryManager) Block() func() {
	select {
	case <-mm.memBlockChan:
	default:
		log.Fatal("already blocking")
	}
	blockChan := make(chan struct{})
	mm.memBlockChan = blockChan
	return func() { close(blockChan) }
}

func (m *Memory) Release()           {}
func (m *Memory) ReleaseSome(uint64) {}

func (mm *MemoryManager) Limit(amt uint64) (memory.MemoryManager, error) {
	return mm, nil
}

func (mm *MemoryManager) Status() memory.Status { return memory.Status{} }

func (mm *MemoryManager) AcquireMemory(ctx context.Context, amt uint64) memory.Memory {
	<-mm.memBlockChan
	return &Memory{}
}

type settingStoreMock struct{}

func (*settingStoreMock) GougingParams(context.Context) (api.GougingParams, error) {
	return api.GougingParams{}, nil
}

func (*settingStoreMock) UploadParams(context.Context) (api.UploadParams, error) {
	return api.UploadParams{}, nil
}

type syncerMock struct{}

func (*syncerMock) BroadcastTransaction(context.Context, []types.Transaction) error {
	return nil
}

func (*syncerMock) SyncerPeers(context.Context) ([]string, error) {
	return nil, nil
}

var _ webhooks.Broadcaster = (*webhookBroadcasterMock)(nil)

type webhookBroadcasterMock struct{}

func (*webhookBroadcasterMock) BroadcastAction(context.Context, webhooks.Event) error {
	return nil
}

type webhookStoreMock struct{}

func (*webhookStoreMock) RegisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	return nil
}

func (*webhookStoreMock) UnregisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	return nil
}
