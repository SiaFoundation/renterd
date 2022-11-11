package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/slab"
	"go.sia.tech/siad/types"
)

type noopMetricsRecorder struct{}

func (noopMetricsRecorder) RecordMetric(slab.Metric) {}

func toHostInteraction(m slab.Metric) hostdb.Interaction {
	transform := func(timestamp time.Time, typ string, err error, res interface{}) hostdb.Interaction {
		hi := hostdb.Interaction{
			Timestamp: timestamp,
			Type:      typ,
			Success:   err == nil,
		}
		if err == nil {
			hi.Result, _ = json.Marshal(res)
		} else {
			hi.Result = []byte(`"` + err.Error() + `"`)
		}
		return hi
	}

	switch m := m.(type) {
	case slab.MetricSectorUpload:
		return transform(m.Timestamp, "sector upload", m.Err, struct {
			Elapsed time.Duration  `json:"elapsed"`
			Cost    types.Currency `json:"cost"`
		}{m.Elapsed, m.Cost})
	case slab.MetricSectorDownload:
		return transform(m.Timestamp, "sector download", m.Err, struct {
			Elapsed    time.Duration  `json:"elapsed"`
			Downloaded uint64         `json:"downloaded"`
			Cost       types.Currency `json:"cost"`
		}{m.Elapsed, m.Downloaded, m.Cost})
	case slab.MetricSectorDeletion:
		return transform(m.Timestamp, "sector delete", m.Err, struct {
			Elapsed  time.Duration  `json:"elapsed"`
			Cost     types.Currency `json:"cost"`
			NumRoots uint64         `json:"numRoots"`
		}{m.Elapsed, m.Cost, m.NumRoots})
	default:
		panic(fmt.Sprintf("unhandled type %T", m))
	}
}

type slabMover struct {
	pool *slab.SessionPool
}

func (sm slabMover) withHosts(ctx context.Context, contracts []Contract, fn func([]slab.Host) error) (err error) {
	// TODO: send metrics to bus, rather than throwing them away
	var mr noopMetricsRecorder
	var hosts []slab.Host
	for _, c := range contracts {
		hosts = append(hosts, sm.pool.Session(c.HostKey, c.HostIP, c.ID, c.RenterKey, &mr))
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
	err = fn(hosts)
	return err
}

func (sm slabMover) UploadSlab(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []Contract) (s slab.Slab, err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	err = sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		s, err = slab.UploadSlab(ctx, r, m, n, hosts)
		return err
	})
	return
}

func (sm slabMover) DownloadSlab(ctx context.Context, w io.Writer, s slab.Slice, contracts []Contract) (err error) {
	return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		return slab.DownloadSlab(ctx, w, s, hosts)
	})
}

func (sm slabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []Contract) (err error) {
	return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		return slab.DeleteSlabs(ctx, slabs, hosts)
	})
}

func (sm slabMover) MigrateSlab(ctx context.Context, s *slab.Slab, currentHeight uint64, from, to []Contract) (err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	return sm.withHosts(ctx, append(from, to...), func(hosts []slab.Host) error {
		from, to := hosts[:len(from)], hosts[len(from):]
		return slab.MigrateSlab(ctx, s, from, to)
	})
}

func newSlabMover() slabMover {
	return slabMover{
		pool: slab.NewSessionPool(),
	}
}
