package node

import (
	"errors"
	"fmt"
	"net"

	"go.sia.tech/core/gateway"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/renterd/chain"
	"go.uber.org/zap"
)

type nodeSyncer struct {
	*syncer.Syncer
	l net.Listener
}

func (s *nodeSyncer) Close() error {
	return s.l.Close()
}

func NewSyncer(cfg BusConfig, cm syncer.ChainManager, ps syncer.PeerStore, logger *zap.Logger) (chain.Syncer, error) {
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
	go s.Run()

	return &nodeSyncer{s, l}, nil
}

func options(cfg BusConfig, logger *zap.Logger) (opts []syncer.Option) {
	opts = append(opts, syncer.WithLogger(logger.Named("syncer")))

	if cfg.SyncerSyncInterval > 0 {
		opts = append(opts, syncer.WithSyncInterval(cfg.SyncerSyncInterval))
	}

	if cfg.SyncerPeerDiscoveryInterval > 0 {
		opts = append(opts, syncer.WithPeerDiscoveryInterval(cfg.SyncerPeerDiscoveryInterval))
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
