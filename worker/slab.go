package worker

import (
	"context"
	"io"

	"go.sia.tech/renterd/slab"
)

type slabMover struct {
	pool *slab.SessionPool
}

func (sm slabMover) withHosts(ctx context.Context, contracts []Contract, fn func([]slab.Host) error) (err error) {
	var hosts []slab.Host
	for _, c := range contracts {
		hosts = append(hosts, sm.pool.Session(c.HostKey, c.HostIP, c.ID, c.RenterKey))
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			for _, h := range hosts {
				sm.pool.UnlockContract(h.(*slab.Session))
			}
		case <-ctx.Done():
			for _, h := range hosts {
				sm.pool.ForceClose(h.(*slab.Session))
			}
		}
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	return fn(hosts)
}

func (sm slabMover) withMetrics(ctx context.Context, fn func(context.Context) error) (his []HostInteraction, err error) {
	err = fn(slab.ContextWithMetricsRecorder(ctx))
	return toHostInteractions(slab.RecorderFromContext(ctx).Metrics()), err
}

func (sm slabMover) UploadSlab(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []Contract) (s slab.Slab, his []HostInteraction, err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	his, err = sm.withMetrics(ctx, func(ctx context.Context) error {
		return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
			s, err = slab.UploadSlab(ctx, r, m, n, hosts)
			return err
		})
	})
	return
}

func (sm slabMover) DownloadSlab(ctx context.Context, w io.Writer, s slab.Slice, contracts []Contract) (his []HostInteraction, err error) {
	return sm.withMetrics(ctx, func(ctx context.Context) error {
		return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
			return slab.DownloadSlab(ctx, w, s, hosts)
		})
	})
}

func (sm slabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []Contract) (his []HostInteraction, err error) {
	return sm.withMetrics(ctx, func(ctx context.Context) error {
		return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
			return slab.DeleteSlabs(ctx, slabs, hosts)
		})
	})
}

func (sm slabMover) MigrateSlab(ctx context.Context, s *slab.Slab, currentHeight uint64, from, to []Contract) (his []HostInteraction, err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	return sm.withMetrics(ctx, func(ctx context.Context) error {
		return sm.withHosts(ctx, from, func(fromHosts []slab.Host) error {
			return sm.withHosts(ctx, to, func(toHosts []slab.Host) error {
				return slab.MigrateSlab(ctx, s, fromHosts, toHosts)
			})
		})
	})
}

func newSlabMover() slabMover {
	return slabMover{
		pool: slab.NewSessionPool(),
	}
}
