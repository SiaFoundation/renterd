package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/tracing"
	"go.sia.tech/renterd/object"
	"lukechampine.com/frand"
)

const (
	contractLockingUploadPriority   = 1
	contractLockingDownloadPriority = 2
)

var (
	errUnusedHost            = errors.New("host not used")
	errGougingHost           = errors.New("host is gouging")
	errNonFundedHost         = errors.New("host is not funded")
	errDownloadSectorTimeout = errors.New("download sector timed out")
	errUploadSectorTimeout   = errors.New("upload sector timed out")
)

// A sectorStore stores contract data.
type sectorStore interface {
	Contract() types.FileContractID
	HostKey() types.PublicKey
	UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (types.Hash256, error)
	DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error
	DeleteSectors(ctx context.Context, roots []types.Hash256) error
}

type storeProvider interface {
	withHostV2(context.Context, types.FileContractID, types.PublicKey, string, func(sectorStore) error) (err error)
	withHostV3(context.Context, types.FileContractID, types.PublicKey, string, string, func(sectorStore) error) (err error)
}

func parallelUploadSlab(ctx context.Context, sp storeProvider, shards [][]byte, contracts []api.ContractMetadata, locker contractLocker, uploadSectorTimeout time.Duration) ([]object.Sector, []int, error) {
	// ensure the context is cancelled when the slab is uploaded
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if len(contracts) < len(shards) {
		return nil, nil, fmt.Errorf("not enough hosts to upload slab, %v<%v", len(contracts), len(shards))
	}

	type req struct {
		contract   api.ContractMetadata
		shardIndex int
	}
	type resp struct {
		req  req
		root types.Hash256
		err  error
	}
	respChan := make(chan resp, 2*len(contracts)) // every host can send up to 2 responses
	worker := func(r req) {
		doneChan := make(chan struct{})

		// Trace the upload.
		ctx, span := tracing.Tracer.Start(ctx, "upload-request")
		span.SetAttributes(attribute.Stringer("host", r.contract.HostKey))
		span.SetAttributes(attribute.Stringer("contract", r.contract.ID))

		go func(r req) {
			defer close(doneChan)

			lockID, err := locker.AcquireContract(ctx, r.contract.ID, contractLockingUploadPriority, 30*time.Second)
			if err != nil {
				respChan <- resp{r, types.Hash256{}, err}
				span.SetStatus(codes.Error, "acquiring the contract failed")
				span.RecordError(err)
				return
			}

			var res resp
			_ = sp.withHostV2(ctx, r.contract.ID, r.contract.HostKey, r.contract.HostIP, func(ss sectorStore) error {
				root, err := ss.UploadSector(ctx, (*[rhpv2.SectorSize]byte)(shards[r.shardIndex]))
				if err != nil {
					span.SetStatus(codes.Error, "uploading the sector failed")
					span.RecordError(err)
				}
				res = resp{r, root, err}
				return err
			})

			_ = locker.ReleaseContract(ctx, r.contract.ID, lockID)
			respChan <- res
		}(r)

		if uploadSectorTimeout > 0 {
			timer := time.NewTimer(uploadSectorTimeout)
			select {
			case <-timer.C:
				span.SetAttributes(attribute.Bool("slow", true))
				respChan <- resp{
					req: r,
					err: errUploadSectorTimeout}
			case <-doneChan:
				if !timer.Stop() {
					<-timer.C
				}
			}
		}

		<-doneChan
		span.End()
	}

	// spawn workers and send initial requests
	hostIndex := 0
	inflight := 0
	for i := range shards {
		go worker(req{contracts[hostIndex], i})
		hostIndex++
		inflight++
	}

	// collect responses
	var errs HostErrorSet
	sectors := make([]object.Sector, len(shards))
	rem := len(shards)
	for rem > 0 && inflight > 0 {
		resp := <-respChan
		if !errors.Is(resp.err, errUploadSectorTimeout) {
			inflight--
		}

		if resp.err != nil {
			errs = append(errs, &HostError{resp.req.contract.HostKey, resp.err})
			// try next host
			if hostIndex < len(contracts) {
				go worker(req{contracts[hostIndex], resp.req.shardIndex})
				hostIndex++
				inflight++
			}
		} else if sectors[resp.req.shardIndex].Root == (types.Hash256{}) {
			sectors[resp.req.shardIndex] = object.Sector{
				Host: resp.req.contract.HostKey,
				Root: resp.root,
			}
			rem--
		}
	}
	if rem > 0 {
		return nil, nil, errs
	}

	// make hosts map
	hostsMap := make(map[types.PublicKey]int)
	for i, c := range contracts {
		hostsMap[c.HostKey] = i
	}

	// collect slow host indices
	var slowHosts []int
	for _, he := range errs {
		if errors.Is(he, errUploadSectorTimeout) {
			if _, exists := hostsMap[he.HostKey]; !exists {
				panic("host not found in hostsmap")
			}
			slowHosts = append(slowHosts, hostsMap[he.HostKey])
		}
	}
	return sectors, slowHosts, nil
}

func uploadSlab(ctx context.Context, sp storeProvider, r io.Reader, m, n uint8, contracts []api.ContractMetadata, locker contractLocker, uploadSectorTimeout time.Duration) (object.Slab, int, []int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "uploadSlab")
	defer span.End()

	buf := make([]byte, int(m)*rhpv2.SectorSize)
	shards := make([][]byte, n)
	length, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return object.Slab{}, 0, nil, err
	}
	s := object.Slab{
		Key:       object.GenerateEncryptionKey(),
		MinShards: m,
	}
	s.Encode(buf, shards)
	s.Encrypt(shards)

	sectors, slowHosts, err := parallelUploadSlab(ctx, sp, shards, contracts, locker, uploadSectorTimeout)
	if err != nil {
		return object.Slab{}, 0, nil, err
	}

	s.Shards = sectors
	return s, length, slowHosts, nil
}

func parallelDownloadSlab(ctx context.Context, sp storeProvider, ss object.SlabSlice, contracts []api.ContractMetadata, downloadSectorTimeout time.Duration) ([][]byte, []int, error) {
	// ensure the context is cancelled when the slab is downloaded
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// check whether we can recover the slab
	if len(contracts) < int(ss.MinShards) {
		return nil, nil, errors.New("not enough hosts to recover slab")
	}

	type req struct {
		hostIndex int
	}
	type resp struct {
		req   req
		shard []byte
		err   error
	}
	respChan := make(chan resp, 2*len(contracts)) // every host can send up to 2 responses
	worker := func(r req) {
		doneChan := make(chan struct{})

		// Trace the download.
		ctx, span := tracing.Tracer.Start(ctx, "download-request")
		span.SetAttributes(attribute.Stringer("host", contracts[r.hostIndex].HostKey))

		go func(r req) {
			defer close(doneChan)
			c := contracts[r.hostIndex]
			var shard *object.Sector
			for i := range ss.Shards {
				if ss.Shards[i].Host == c.HostKey {
					shard = &ss.Shards[i]
					break
				}
			}
			if shard == nil {
				respChan <- resp{r, nil, fmt.Errorf("host %v, err: %w", c.HostKey, errUnusedHost)}
				return
			}

			offset, length := ss.SectorRegion()
			_ = sp.withHostV3(ctx, c.ID, c.HostKey, c.HostIP, c.SiamuxAddr, func(ss sectorStore) error {
				buf := bytes.NewBuffer(make([]byte, 0, rhpv2.SectorSize))
				err := ss.DownloadSector(ctx, buf, shard.Root, uint64(offset), uint64(length))
				if err != nil {
					span.SetStatus(codes.Error, "downloading the sector failed")
					span.RecordError(err)
				}
				respChan <- resp{r, buf.Bytes(), err}
				return err
			})
		}(r)

		if downloadSectorTimeout > 0 {
			timer := time.NewTimer(downloadSectorTimeout)
			select {
			case <-timer.C:
				span.SetAttributes(attribute.Bool("slow", true))
				respChan <- resp{
					req: r,
					err: errDownloadSectorTimeout}
			case <-doneChan:
				if !timer.Stop() {
					<-timer.C
				}
			}
		}

		<-doneChan
		span.End()
	}

	// spawn workers and send initial requests
	hostIndex := 0
	inflight := 0
	for i := uint8(0); i < ss.MinShards; i++ {
		go worker(req{hostIndex})
		hostIndex++
		inflight++
	}
	// collect responses
	var errs HostErrorSet
	shards := make([][]byte, len(ss.Shards))
	rem := ss.MinShards
	for rem > 0 && inflight > 0 {
		resp := <-respChan
		if !errors.Is(resp.err, errDownloadSectorTimeout) {
			inflight--
		}

		if resp.err != nil {
			errs = append(errs, &HostError{contracts[resp.req.hostIndex].HostKey, resp.err})
			// try next host
			if hostIndex < len(contracts) {
				go worker(req{hostIndex})
				hostIndex++
				inflight++
			}
		} else {
			for i := range ss.Shards {
				if ss.Shards[i].Host == contracts[resp.req.hostIndex].HostKey && len(shards[i]) == 0 {
					shards[i] = resp.shard
					rem--
					break
				}
			}
		}
	}
	if rem > 0 {
		return nil, nil, errs
	}

	// make hosts map
	hostsMap := make(map[types.PublicKey]int)
	for i, h := range contracts {
		hostsMap[h.HostKey] = i
	}

	// collect bad host indices
	var badHosts []int
	for _, he := range errs {
		if errors.Is(he, errNonFundedHost) ||
			errors.Is(he, errGougingHost) ||
			errors.Is(he, errDownloadSectorTimeout) {
			if _, exists := hostsMap[he.HostKey]; !exists {
				panic("host not found in hostsmap")
			}
			badHosts = append(badHosts, hostsMap[he.HostKey])
		}
	}

	return shards, badHosts, nil
}

func downloadSlab(ctx context.Context, sp storeProvider, out io.Writer, ss object.SlabSlice, contracts []api.ContractMetadata, downloadSectorTimeout time.Duration) ([]int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "parallelDownloadSlab")
	defer span.End()

	shards, badHosts, err := parallelDownloadSlab(ctx, sp, ss, contracts, downloadSectorTimeout)
	if err != nil {
		return nil, err
	}
	ss.Decrypt(shards)
	err = ss.Recover(out, shards)
	if err != nil {
		return nil, err
	}
	return badHosts, nil
}

// slabsForDownload returns the slices that comprise the specified offset-length
// span within slabs.
func slabsForDownload(slabs []object.SlabSlice, offset, length int64) []object.SlabSlice {
	// mutate a copy
	slabs = append([]object.SlabSlice(nil), slabs...)

	firstOffset := offset
	for i, ss := range slabs {
		if firstOffset <= int64(ss.Length) {
			slabs = slabs[i:]
			break
		}
		firstOffset -= int64(ss.Length)
	}
	slabs[0].Offset += uint32(firstOffset)
	slabs[0].Length -= uint32(firstOffset)

	lastLength := length
	for i, ss := range slabs {
		if lastLength <= int64(ss.Length) {
			slabs = slabs[:i+1]
			break
		}
		lastLength -= int64(ss.Length)
	}
	slabs[len(slabs)-1].Length = uint32(lastLength)
	return slabs
}

func deleteSlabs(ctx context.Context, slabs []object.Slab, hosts []sectorStore) error {
	rootsBysectorStore := make(map[types.PublicKey][]types.Hash256)
	for _, s := range slabs {
		for _, sector := range s.Shards {
			rootsBysectorStore[sector.Host] = append(rootsBysectorStore[sector.Host], sector.Root)
		}
	}

	errChan := make(chan *HostError)
	for _, h := range hosts {
		go func(h sectorStore) {
			// NOTE: if host is not storing any sectors, the map lookup will return
			// nil, making this a no-op
			err := h.DeleteSectors(ctx, rootsBysectorStore[h.HostKey()])
			if err != nil {
				errChan <- &HostError{h.HostKey(), err}
			} else {
				errChan <- nil
			}
		}(h)
	}

	var errs HostErrorSet
	for range hosts {
		if err := <-errChan; err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func migrateSlab(ctx context.Context, sp storeProvider, s *object.Slab, contracts []api.ContractMetadata, locker contractLocker, downloadSectorTimeout, uploadSectorTimeout time.Duration) error {
	ctx, span := tracing.Tracer.Start(ctx, "migrateSlab")
	defer span.End()

	hostsMap := make(map[types.PublicKey]struct{})
	usedMap := make(map[types.PublicKey]struct{})

	// make a map of good hosts
	for _, c := range contracts {
		hostsMap[c.HostKey] = struct{}{}
	}

	// collect indices of shards that need to be migrated
	var shardIndices []int
	for i, shard := range s.Shards {
		// bad host
		if _, exists := hostsMap[shard.Host]; !exists {
			shardIndices = append(shardIndices, i)
			continue
		}

		// reused host
		_, exists := usedMap[shard.Host]
		if exists {
			shardIndices = append(shardIndices, i)
			continue
		}

		usedMap[shard.Host] = struct{}{}
	}

	// if all shards are on good hosts, we're done
	if len(shardIndices) == 0 {
		return nil
	}

	// perform some sanity checks
	if len(s.Shards)-len(shardIndices) < int(s.MinShards) {
		return fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-len(shardIndices), int(s.MinShards))
	} else if len(shardIndices) > len(contracts) {
		return errors.New("not enough hosts to migrate shard")
	}

	// download + reconstruct slab
	ss := object.SlabSlice{
		Slab:   *s,
		Offset: 0,
		Length: uint32(s.MinShards) * rhpv2.SectorSize,
	}
	shards, badHosts, err := parallelDownloadSlab(ctx, sp, ss, contracts, downloadSectorTimeout)
	if err != nil {
		return fmt.Errorf("failed to download slab for migration: %w", err)
	}
	ss.Decrypt(shards)
	if err := s.Reconstruct(shards); err != nil {
		return fmt.Errorf("failed to reconstruct shards downloaded for migration: %w", err)
	}
	s.Encrypt(shards)

	// filter it down to the shards we need to migrate
	for i, si := range shardIndices {
		shards[i] = shards[si]
	}
	shards = shards[:len(shardIndices)]

	// filter out the hosts we used already
	filtered := contracts[:0]
	for _, c := range contracts {
		if _, used := usedMap[c.HostKey]; !used {
			filtered = append(filtered, c)
		}
	}

	// randomize order of hosts to make sure we don't migrate to the same hosts all the time
	frand.Shuffle(len(filtered), func(i, j int) { filtered[i], filtered[j] = filtered[j], filtered[i] })

	// move bad hosts to the back of the array, a bad host is a host that timed
	// out, is out of funds or is gouging its prices
	bad := make(map[types.PublicKey]int)
	for _, h := range badHosts {
		bad[contracts[h].HostKey]++
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		return bad[filtered[i].HostKey] < bad[filtered[j].HostKey]
	})

	// reupload those shards
	uploaded, _, err := parallelUploadSlab(ctx, sp, shards, filtered, locker, uploadSectorTimeout)
	if err != nil {
		return fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// overwrite the unhealthy shards with the newly migrated ones
	for i, si := range shardIndices {
		s.Shards[si] = uploaded[i]
	}
	return nil
}
