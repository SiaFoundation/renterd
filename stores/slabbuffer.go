package stores

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
	"lukechampine.com/frand"
)

var (
	errBufferNotFound = errors.New("buffer not found")
)

type SlabBuffer struct {
	dbID     uint
	filename string
	slabKey  object.EncryptionKey
	maxSize  int64

	mu          sync.Mutex
	file        *os.File
	lockedUntil time.Time
	size        int64
	syncErr     error
}

type bufferGroupID [6]byte

type SlabBufferManager struct {
	bufferedSlabCompletionThreshold int64
	dir                             string
	s                               *SQLStore

	mu                sync.Mutex
	completeBuffers   map[bufferGroupID][]*SlabBuffer
	incompleteBuffers map[bufferGroupID][]*SlabBuffer
	buffersByKey      map[string]*SlabBuffer
}

func newSlabBufferManager(sqlStore *SQLStore, slabBufferCompletionThreshold int64, partialSlabDir string) (*SlabBufferManager, error) {
	if slabBufferCompletionThreshold < 0 || slabBufferCompletionThreshold > 1<<22 {
		return nil, fmt.Errorf("invalid slabBufferCompletionThreshold %v", slabBufferCompletionThreshold)
	}

	// load existing buffers
	var buffers []dbBufferedSlab
	err := sqlStore.db.
		Joins("DBSlab").
		Find(&buffers).
		Error
	if err != nil {
		return nil, err
	}
	mgr := &SlabBufferManager{
		bufferedSlabCompletionThreshold: slabBufferCompletionThreshold,
		dir:                             partialSlabDir,
		s:                               sqlStore,
		completeBuffers:                 make(map[bufferGroupID][]*SlabBuffer),
		incompleteBuffers:               make(map[bufferGroupID][]*SlabBuffer),
		buffersByKey:                    make(map[string]*SlabBuffer),
	}
	for _, buffer := range buffers {
		if buffer.DBSlab.ID == 0 {
			// Buffer doesn't have a slab. We can delete it.
			sqlStore.logger.Warn(fmt.Sprintf("buffer %v has no associated slab, deleting it", buffer.Filename))
			if err := sqlStore.db.Delete(&buffer).Error; err != nil {
				return nil, fmt.Errorf("failed to delete buffer %v: %v", buffer.ID, err)
			}
			if err := os.RemoveAll(filepath.Join(partialSlabDir, buffer.Filename)); err != nil {
				return nil, fmt.Errorf("failed to remove buffer file %v: %v", buffer.Filename, err)
			}
			continue
		}
		// Get the encryption key.
		var ec object.EncryptionKey
		if err := ec.UnmarshalBinary(buffer.DBSlab.Key); err != nil {
			return nil, err
		}
		// Open the file.
		file, err := os.OpenFile(filepath.Join(partialSlabDir, buffer.Filename), os.O_RDWR, 0600)
		if err != nil {
			_ = sqlStore.alerts.RegisterAlert(sqlStore.shutdownCtx, alerts.Alert{
				ID:       types.HashBytes([]byte(buffer.Filename)),
				Severity: alerts.SeverityCritical,
				Message:  "failed to read buffer file on startup",
				Data: map[string]interface{}{
					"filename": buffer.Filename,
					"slabKey":  ec,
				},
				Timestamp: time.Now(),
			})
			sqlStore.logger.Errorf("failed to open buffer file %v for slab %v: %v", buffer.Filename, buffer.DBSlab.Key, err)
			continue
		}
		// Get the size of the buffer by looking at all slices using it
		var size int64
		err = sqlStore.db.Model(&dbSlab{}).
			Joins("INNER JOIN slices sli ON slabs.id = sli.db_slab_id").
			Select("COALESCE(MAX(offset+length), 0) as Size").
			Where("slabs.db_buffered_slab_id = ?", buffer.ID).
			Scan(&size).
			Error
		if err != nil {
			return nil, err
		}
		// Create the slab buffer.
		sb := &SlabBuffer{
			dbID:     buffer.ID,
			filename: buffer.Filename,
			slabKey:  ec,
			maxSize:  int64(bufferedSlabSize(buffer.DBSlab.MinShards)),
			file:     file,
			size:     size,
		}
		// Add the buffer to the manager.
		gid := bufferGID(buffer.DBSlab.MinShards, buffer.DBSlab.TotalShards, uint32(buffer.DBSlab.DBContractSetID))
		if size >= int64(sb.maxSize-slabBufferCompletionThreshold) {
			mgr.completeBuffers[gid] = append(mgr.completeBuffers[gid], sb)
		} else {
			mgr.incompleteBuffers[gid] = append(mgr.incompleteBuffers[gid], sb)
		}
		mgr.buffersByKey[sb.slabKey.String()] = sb
	}
	return mgr, nil
}

func bufferGID(minShards, totalShards uint8, contractSet uint32) bufferGroupID {
	var bgid bufferGroupID
	bgid[0] = minShards
	bgid[1] = totalShards
	binary.LittleEndian.PutUint32(bgid[2:], contractSet)
	return bgid
}

func (mgr *SlabBufferManager) Close() error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	var errs []error
	for _, buffers := range mgr.buffersByKey {
		if err := buffers.file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	mgr.buffersByKey = nil
	mgr.incompleteBuffers = nil
	mgr.completeBuffers = nil
	return errors.Join(errs...)
}

func (mgr *SlabBufferManager) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet uint) ([]object.SlabSlice, int64, error) {
	gid := bufferGID(minShards, totalShards, uint32(contractSet))

	// Sanity check input.
	slabSize := bufferedSlabSize(minShards)
	if minShards == 0 || totalShards == 0 || minShards > totalShards {
		return nil, 0, fmt.Errorf("invalid shard configuration: minShards=%v, totalShards=%v", minShards, totalShards)
	} else if contractSet == 0 {
		return nil, 0, fmt.Errorf("contract set must be set")
	} else if len(data) > slabSize {
		return nil, 0, fmt.Errorf("data size %v exceeds size of a slab %v", len(data), slabSize)
	}

	// Deep copy available buffers. We don't want to block the manager while we
	// perform disk I/O.
	mgr.mu.Lock()
	buffers := append([]*SlabBuffer{}, mgr.incompleteBuffers[gid]...)
	mgr.mu.Unlock()

	// Find a buffer to use. We use at most 1 existing buffer + either 1 buffer
	// that can fit the remainder of the data or 1 new buffer to avoid splitting
	// the data over too many slabs.
	var slab object.SlabSlice
	var slabs []object.SlabSlice
	var err error
	var usedBuffers []*SlabBuffer
	for _, buffer := range buffers {
		var used bool
		slab, data, used, err = buffer.recordAppend(data, len(usedBuffers) > 0, minShards, mgr.bufferedSlabCompletionThreshold)
		if err != nil {
			return nil, 0, err
		}
		if used {
			usedBuffers = append(usedBuffers, buffer)
			slabs = append(slabs, slab)
		}
		if len(data) == 0 {
			break // done
		}
	}

	// If there is still data left, create a new buffer.
	if len(data) > 0 {
		var sb *SlabBuffer
		err = mgr.s.retryTransaction(ctx, func(tx *gorm.DB) error {
			sb, err = createSlabBuffer(tx, contractSet, mgr.dir, minShards, totalShards)
			return err
		})
		if err != nil {
			return nil, 0, err
		}
		var used bool
		slab, data, used, err = sb.recordAppend(data, true, minShards, mgr.bufferedSlabCompletionThreshold)
		if err != nil {
			return nil, 0, err
		}
		if len(data) > 0 || !used {
			panic("remaining data after creating new buffer")
		}
		usedBuffers = append(usedBuffers, sb)
		slabs = append(slabs, slab)

		// Add new buffer to the list of incomplete buffers.
		mgr.mu.Lock()
		mgr.incompleteBuffers[gid] = append(mgr.incompleteBuffers[gid], sb)
		mgr.buffersByKey[sb.slabKey.String()] = sb
		mgr.mu.Unlock()
	}

	// Commit all used buffers to disk.
	for _, buffer := range usedBuffers {
		complete, err := buffer.commitAppend(mgr.bufferedSlabCompletionThreshold)
		if err != nil {
			return nil, 0, err
		}
		// Move the buffer from incomplete to complete if it is now complete.
		if complete {
			mgr.markBufferComplete(buffer, gid)
		}
	}
	return slabs, mgr.BufferSize(gid), nil
}

func (mgr *SlabBufferManager) BufferSize(gid bufferGroupID) (total int64) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for _, buffer := range mgr.completeBuffers[gid] {
		total += buffer.maxSize
	}
	for _, buffer := range mgr.incompleteBuffers[gid] {
		total += buffer.maxSize
	}
	return
}

func (mgr *SlabBufferManager) FetchPartialSlab(ctx context.Context, ec object.EncryptionKey, offset, length uint32) ([]byte, error) {
	mgr.mu.Lock()
	buffer, exists := mgr.buffersByKey[ec.String()]
	mgr.mu.Unlock()
	if !exists {
		return nil, api.ErrObjectNotFound
	}

	data := make([]byte, length)
	_, err := buffer.file.ReadAt(data, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("failed to read data from buffer (offset: %v, length: %v): %w", offset, length, err)
	}
	return data, nil
}

func (mgr *SlabBufferManager) SlabBuffers() (sbs []api.SlabBuffer) {
	// Fetch buffers.
	mgr.mu.Lock()
	var completeBuffers, incompleteBuffers []*SlabBuffer
	for _, buffers := range mgr.completeBuffers {
		completeBuffers = append(completeBuffers, buffers...)
	}
	for _, buffers := range mgr.incompleteBuffers {
		incompleteBuffers = append(incompleteBuffers, buffers...)
	}
	mgr.mu.Unlock()

	// Convert them.
	convertBuffer := func(buffer *SlabBuffer, complete bool) api.SlabBuffer {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return api.SlabBuffer{
			ContractSet: "", // filled in by caller
			Complete:    complete,
			Filename:    buffer.filename,
			Size:        buffer.size,
			MaxSize:     buffer.maxSize,
			Locked:      time.Now().Before(buffer.lockedUntil),
		}
	}
	for _, buffer := range completeBuffers {
		sbs = append(sbs, convertBuffer(buffer, true))
	}
	for _, buffer := range incompleteBuffers {
		sbs = append(sbs, convertBuffer(buffer, false))
	}
	return sbs
}

func (mgr *SlabBufferManager) SlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set uint, limit int) (slabs []api.PackedSlab, _ error) {
	mgr.mu.Lock()
	buffers := mgr.completeBuffers[bufferGID(minShards, totalShards, uint32(set))]
	mgr.mu.Unlock()

	for _, buffer := range buffers {
		if !buffer.acquireForUpload(lockingDuration) {
			continue
		}
		data := make([]byte, buffer.size)
		_, err := buffer.file.ReadAt(data, 0)
		if err != nil {
			mgr.s.alerts.RegisterAlert(ctx, alerts.Alert{
				ID:       types.HashBytes([]byte(buffer.filename)),
				Severity: alerts.SeverityCritical,
				Message:  "failed to read data from buffer",
				Data: map[string]interface{}{
					"filename": buffer.filename,
					"slabKey":  buffer.slabKey,
				},
				Timestamp: time.Now(),
			})
			mgr.s.logger.Error(ctx, fmt.Sprintf("failed to read buffer %v: %s", buffer.filename, err))
			return nil, err
		}
		slabs = append(slabs, api.PackedSlab{
			BufferID: buffer.dbID,
			Data:     data,
			Key:      buffer.slabKey,
		})
		if len(slabs) == limit {
			break
		}
	}
	return slabs, nil
}

func (mgr *SlabBufferManager) RemoveBuffers(fileNames ...string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	buffersToDelete := make(map[string]struct{})
	for _, path := range fileNames {
		buffersToDelete[path] = struct{}{}
	}
	for gid := range mgr.completeBuffers {
		buffers := mgr.completeBuffers[gid]
		for i := 0; i < len(buffers); i++ {
			if _, exists := buffersToDelete[buffers[i].filename]; !exists {
				continue
			}
			// Close the file and remove it from disk. If this fails we only log
			// an error because the buffers are not meant to be used anymore
			// anyway.
			if err := buffers[i].file.Close(); err != nil {
				mgr.s.logger.Errorf("failed to close buffer %v: %v", buffers[i].filename, err)
			} else if err := os.RemoveAll(filepath.Join(mgr.dir, buffers[i].filename)); err != nil {
				mgr.s.logger.Errorf("failed to remove buffer %v: %v", buffers[i].filename, err)
			}
			delete(mgr.buffersByKey, buffers[i].slabKey.String())
			buffers[i] = buffers[len(buffers)-1]
			buffers = buffers[:len(buffers)-1]
			i--
		}
		mgr.completeBuffers[gid] = buffers
	}
}

func (buf *SlabBuffer) acquireForUpload(lockingDuration time.Duration) bool {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if time.Now().Before(buf.lockedUntil) {
		return false
	}
	buf.lockedUntil = time.Now().Add(lockingDuration)
	return true
}

func isCompleteBuffer(size, maxSize, completionThreshold int64) bool {
	return size+completionThreshold >= maxSize
}

func (buf *SlabBuffer) recordAppend(data []byte, mustFit bool, minShards uint8, completionThreshold int64) (object.SlabSlice, []byte, bool, error) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	remainingSpace := buf.maxSize - buf.size
	if isCompleteBuffer(buf.size, buf.maxSize, completionThreshold) {
		return object.SlabSlice{}, data, false, nil
	} else if int64(len(data)) <= remainingSpace {
		_, err := buf.file.WriteAt(data, buf.size)
		if err != nil {
			return object.SlabSlice{}, nil, true, err
		}
		slab := object.SlabSlice{
			Slab:   object.NewPartialSlab(buf.slabKey, minShards),
			Offset: uint32(buf.size),
			Length: uint32(len(data)),
		}
		buf.size += int64(len(data))
		return slab, nil, true, nil
	} else if !mustFit {
		_, err := buf.file.WriteAt(data[:remainingSpace], buf.size)
		if err != nil {
			return object.SlabSlice{}, nil, true, err
		}
		slab := object.SlabSlice{
			Slab:   object.NewPartialSlab(buf.slabKey, minShards),
			Offset: uint32(buf.size),
			Length: uint32(remainingSpace),
		}
		buf.size += remainingSpace
		return slab, data[remainingSpace:], true, nil
	} else {
		return object.SlabSlice{}, data, false, nil
	}
}

func (buf *SlabBuffer) commitAppend(completionThreshold int64) (bool, error) {
	// Fetch the current size first. We know that we have at least synced the
	// buffer up to this point upon success.
	buf.mu.Lock()
	if buf.syncErr != nil {
		buf.mu.Unlock()
		return false, buf.syncErr
	}
	syncSize := buf.size
	buf.mu.Unlock()

	// Sync the buffer to disk. No need for a lock here.
	err := buf.file.Sync()

	buf.mu.Lock()
	defer buf.mu.Unlock()
	buf.syncErr = err
	return isCompleteBuffer(syncSize, buf.maxSize, completionThreshold), nil
}

func (mgr *SlabBufferManager) markBufferComplete(buffer *SlabBuffer, gid bufferGroupID) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if _, exists := mgr.incompleteBuffers[gid]; exists {
		var found bool
		for i := range mgr.incompleteBuffers[gid] {
			if mgr.incompleteBuffers[gid][i] == buffer {
				mgr.incompleteBuffers[gid] = append(mgr.incompleteBuffers[gid][:i], mgr.incompleteBuffers[gid][i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: %v", errBufferNotFound, buffer.filename)
		}
		mgr.completeBuffers[gid] = append(mgr.completeBuffers[gid], buffer)
	}
	return nil
}

func bufferFilename(contractSetID uint, minShards, totalShards uint8) string {
	identifier := frand.Entropy256()
	return fmt.Sprintf("%v-%v-%v-%v", contractSetID, minShards, totalShards, hex.EncodeToString(identifier[:]))
}

func bufferedSlabSize(minShards uint8) int {
	return int(rhpv2.SectorSize) * int(minShards)
}

func createSlabBuffer(tx *gorm.DB, contractSetID uint, dir string, minShards, totalShards uint8) (*SlabBuffer, error) {
	ec := object.GenerateEncryptionKey()
	key, err := ec.MarshalBinary()
	if err != nil {
		return nil, err
	}
	// Create a new buffer and slab.
	fileName := bufferFilename(contractSetID, minShards, totalShards)
	file, err := os.Create(filepath.Join(dir, fileName))
	if err != nil {
		return nil, err
	}
	createdSlab := dbBufferedSlab{
		DBSlab: dbSlab{
			DBContractSetID: contractSetID,
			Key:             key,
			MinShards:       minShards,
			TotalShards:     totalShards,
		},
		Filename: fileName,
	}
	err = tx.Create(&createdSlab).
		Error
	return &SlabBuffer{
		dbID:     createdSlab.ID,
		filename: fileName,
		slabKey:  ec,
		maxSize:  int64(bufferedSlabSize(minShards)),
		file:     file,
	}, err
}
