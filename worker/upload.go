package worker

import (
	"context"
	"fmt"
	"io"
	"mime"
	"path/filepath"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/memory"
	"go.sia.tech/renterd/internal/upload"
	"go.uber.org/zap"
)

const (
	defaultPackedSlabsLockDuration  = 10 * time.Minute
	defaultPackedSlabsUploadTimeout = 10 * time.Minute
)

func (w *Worker) upload(ctx context.Context, bucket, key string, rs api.RedundancySettings, r io.Reader, hosts []upload.HostInfo, opts ...upload.Option) (_ string, err error) {
	// apply the options
	up := upload.DefaultParameters(bucket, key, rs)
	for _, opt := range opts {
		opt(&up)
	}

	// if not given, try decide on a mime type using the file extension
	if !up.Multipart && up.MimeType == "" {
		up.MimeType = mime.TypeByExtension(filepath.Ext(up.Key))

		// if mime type is still not known, wrap the reader with a mime reader
		if up.MimeType == "" {
			up.MimeType, r, err = upload.NewMimeReader(r)
			if err != nil {
				return
			}
		}
	}

	// perform the upload
	bufferSizeLimitReached, eTag, err := w.uploadManager.Upload(ctx, r, hosts, up)
	if err != nil {
		return "", err
	}

	// return early if worker was shut down or if we don't have to consider
	// packed uploads
	if w.isStopped() || !up.Packing {
		return eTag, nil
	}

	// try and upload one slab synchronously
	if bufferSizeLimitReached {
		mem := w.uploadManager.AcquireMemory(ctx, up.RS.SlabSize())
		if mem != nil {
			defer mem.Release()

			// fetch packed slab to upload
			packedSlabs, err := w.bus.PackedSlabsForUpload(ctx, defaultPackedSlabsLockDuration, uint8(up.RS.MinShards), uint8(up.RS.TotalShards), 1)
			if err != nil {
				w.logger.With(zap.Error(err)).Error("couldn't fetch packed slabs from bus")
			} else if len(packedSlabs) > 0 {
				// upload packed slab
				if err := w.uploadPackedSlab(ctx, mem, packedSlabs[0], up.RS); err != nil {
					w.logger.With(zap.Error(err)).Error("failed to upload packed slab")
				}
			}
		}
	}

	// make sure there's a goroutine uploading any packed slabs
	go w.threadedUploadPackedSlabs(up.RS)

	return eTag, nil
}

func (w *Worker) threadedUploadPackedSlabs(rs api.RedundancySettings) {
	key := fmt.Sprintf("%d-%d", rs.MinShards, rs.TotalShards)
	w.uploadsMu.Lock()
	if _, ok := w.uploadingPackedSlabs[key]; ok {
		w.uploadsMu.Unlock()
		return
	}
	w.uploadingPackedSlabs[key] = struct{}{}
	w.uploadsMu.Unlock()

	// make sure we mark uploading packed slabs as false when we're done
	defer func() {
		w.uploadsMu.Lock()
		delete(w.uploadingPackedSlabs, key)
		w.uploadsMu.Unlock()
	}()

	// derive a context that we can use as an interrupt in case of an error or shutdown.
	interruptCtx, interruptCancel := context.WithCancel(w.shutdownCtx)
	defer interruptCancel()

	var wg sync.WaitGroup
	for {
		// block until we have memory
		mem := w.uploadManager.AcquireMemory(interruptCtx, rs.SlabSize())
		if mem == nil {
			break // interrupted
		}

		// fetch packed slab to upload
		packedSlabs, err := w.bus.PackedSlabsForUpload(interruptCtx, defaultPackedSlabsLockDuration, uint8(rs.MinShards), uint8(rs.TotalShards), 1)
		if err != nil {
			w.logger.Errorf("couldn't fetch packed slabs from bus: %v", err)
			mem.Release()
			break
		}

		// no more packed slabs to upload
		if len(packedSlabs) == 0 {
			mem.Release()
			break
		}

		wg.Add(1)
		go func(ps api.PackedSlab) {
			defer wg.Done()
			defer mem.Release()

			// we use the background context here, but apply a sane timeout,
			// this ensures ongoing uploads are handled gracefully during
			// shutdown
			ctx, cancel := context.WithTimeout(context.Background(), defaultPackedSlabsUploadTimeout)
			defer cancel()

			// upload packed slab
			if err := w.uploadPackedSlab(ctx, mem, ps, rs); err != nil {
				w.logger.Error(err)
				interruptCancel() // prevent new uploads from being launched
			}
		}(packedSlabs[0])
	}

	// wait for all threads to finish
	wg.Wait()
}

func (w *Worker) hostContracts(ctx context.Context) (hosts []upload.HostInfo, _ error) {
	usableHosts, err := w.bus.UsableHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch usable hosts from bus: %v", err)
	}

	hmap := make(map[types.PublicKey]api.HostInfo)
	for _, h := range usableHosts {
		hmap[h.PublicKey] = h
	}

	contracts, err := w.bus.Contracts(ctx, api.ContractsOpts{FilterMode: api.ContractFilterModeGood})
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch contracts from bus: %v", err)
	}

	for _, c := range contracts {
		if h, ok := hmap[c.HostKey]; ok {
			hosts = append(hosts, upload.HostInfo{
				HostInfo:            h,
				ContractEndHeight:   c.WindowEnd,
				ContractID:          c.ID,
				ContractRenewedFrom: c.RenewedFrom,
			})
		}
	}
	return
}

func (w *Worker) uploadPackedSlab(ctx context.Context, mem memory.Memory, ps api.PackedSlab, rs api.RedundancySettings) error {
	// fetch host & contract info
	contracts, err := w.hostContracts(ctx)
	if err != nil {
		return fmt.Errorf("couldn't fetch contracts from bus: %v", err)
	}

	// fetch upload params
	up, err := w.bus.UploadParams(ctx)
	if err != nil {
		return fmt.Errorf("couldn't fetch upload params from bus: %v", err)
	}

	// attach gouging checker to the context
	ctx = gouging.WithChecker(ctx, w.bus, up.GougingParams)

	// upload packed slab
	err = w.uploadManager.UploadPackedSlab(ctx, rs, ps, mem, contracts, up.CurrentHeight)
	if err != nil {
		return fmt.Errorf("couldn't upload packed slab, err: %v", err)
	}

	return nil
}
