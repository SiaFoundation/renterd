package bus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/syncer"
	"go.uber.org/zap"
)

type Syncer interface {
	io.Closer
	Addr() string
	BroadcastHeader(h gateway.BlockHeader)
	BroadcastTransactionSet([]types.Transaction)
	Connect(ctx context.Context, addr string) (*syncer.Peer, error)
	Peers() []*syncer.Peer
}

// NewSyncer creates a syncer using the given configuration. The syncer that is
// returned is already running, closing it will close the underlying listener
// causing the syncer to stop.
func NewSyncer(cfg NodeConfig, cm syncer.ChainManager, ps syncer.PeerStore, logger *zap.Logger) (Syncer, error) {
	// validate config
	if cfg.Bootstrap && cfg.Network == nil {
		return nil, errors.New("cannot bootstrap without a network")
	}

	// bootstrap the syncer
	if cfg.Bootstrap {
		peers, err := peers(cfg.Network.Name)
		if err != nil {
			return nil, err
		}
		for _, addr := range peers {
			if err := ps.AddPeer(addr); err != nil {
				return nil, fmt.Errorf("%w: failed to add bootstrap peer '%s'", err, addr)
			}
		}
	}

	// create syncer
	l, err := net.Listen("tcp", cfg.GatewayAddr)
	if err != nil {
		return nil, err
	}
	syncerAddr := l.Addr().String()

	// peers will reject us if our hostname is empty or unspecified, so use loopback
	host, port, _ := net.SplitHostPort(syncerAddr)
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		syncerAddr = net.JoinHostPort("127.0.0.1", port)
	}

	// create header
	header := gateway.Header{
		GenesisID:  cfg.Genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerAddr,
	}

	// start the syncer
	s := syncer.New(l, cm, ps, header, options(cfg, logger)...)
	go s.Run(context.Background())

	return s, nil
}

func options(cfg NodeConfig, logger *zap.Logger) (opts []syncer.Option) {
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

	return
}

func peers(network string) ([]string, error) {
	switch network {
	case "mainnet":
		return syncer.MainnetBootstrapPeers, nil
	case "zen":
		return syncer.ZenBootstrapPeers, nil
	case "anagami":
		return syncer.AnagamiBootstrapPeers, nil
	default:
		return nil, fmt.Errorf("no available bootstrap peers for unknown network '%s'", network)
	}
}
