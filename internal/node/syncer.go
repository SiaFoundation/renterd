package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/syncer"
	"go.uber.org/zap"
)

type Syncer interface {
	Addr() string
	BroadcastHeader(h gateway.BlockHeader)
	BroadcastTransactionSet([]types.Transaction)
	Close(ctx context.Context) error
	Connect(ctx context.Context, addr string) (*syncer.Peer, error)
	Peers() []*syncer.Peer
}

type closeSyncer struct {
	*syncer.Syncer
	l       net.Listener
	errChan chan error
}

func (s *closeSyncer) Close(ctx context.Context) error {
	if err := s.l.Close(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-s.errChan:
		return err
	}
}

// NewSyncer creates a syncer using the given configuration. The syncer that is
// returned is already running, closing it will close the underlying listener
// causing the syncer to stop.
func NewSyncer(cfg BusConfig, cm syncer.ChainManager, ps syncer.PeerStore, logger *zap.Logger) (Syncer, error) {
	// bootstrap peers
	if cfg.Bootstrap {
		peers, err := peers(cfg.Network)
		if err != nil {
			return nil, err
		}
		for _, addr := range peers {
			if err := ps.AddPeer(addr); err != nil {
				return nil, fmt.Errorf("%w: failed to add bootstrap peer '%s'", err, addr)
			}
		}
	}

	// create the syncer, peers will reject us if our hostname is empty or
	// unspecified, so use a loopback address if necessary
	l, err := net.Listen("tcp", cfg.GatewayAddr)
	if err != nil {
		return nil, err
	}
	syncerAddr := l.Addr().String()
	host, port, _ := net.SplitHostPort(syncerAddr)
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		syncerAddr = net.JoinHostPort("127.0.0.1", port)
	}
	s := syncer.New(l, cm, ps, gateway.Header{
		GenesisID:  cfg.Genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerAddr,
	}, options(cfg, logger)...)

	// wrap the syncer so we can gracefully close it by closing the underlying
	// listener and waiting for the syncer to exit
	cs := &closeSyncer{s, l, make(chan error, 1)}
	go func() {
		cs.errChan <- s.Run()
		close(cs.errChan)
	}()

	return cs, nil
}

func options(cfg BusConfig, logger *zap.Logger) (opts []syncer.Option) {
	opts = append(opts,
		syncer.WithLogger(logger.Named("syncer")),
		syncer.WithSendBlocksTimeout(time.Minute),
	)

	if cfg.SyncerPeerDiscoveryInterval > 0 {
		opts = append(opts, syncer.WithPeerDiscoveryInterval(cfg.SyncerPeerDiscoveryInterval))
	}
	if cfg.SyncerSyncInterval > 0 {
		opts = append(opts, syncer.WithSyncInterval(cfg.SyncerSyncInterval))
	}

	if cfg.SyncerPeerDiscoveryInterval > 0 {
		opts = append(opts, syncer.WithPeerDiscoveryInterval(cfg.SyncerPeerDiscoveryInterval))
	}

	return
}

func peers(network *consensus.Network) ([]string, error) {
	if network == nil {
		return nil, errors.New("cannot bootstrap without a network")
	}

	switch network.Name {
	case "mainnet":
		return syncer.MainnetBootstrapPeers, nil
	case "zen":
		return syncer.ZenBootstrapPeers, nil
	case "anagami":
		return syncer.AnagamiBootstrapPeers, nil
	default:
		return nil, fmt.Errorf("no available bootstrap peers for unknown network '%s'", network.Name)
	}
}
