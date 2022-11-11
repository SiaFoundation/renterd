package slab

import (
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/types"
)

type Metric interface {
	isMetric()
}

type MetricsRecorder interface {
	RecordMetric(m Metric)
}

// A MetricSectorUpload contains metrics resulting from a sector upload.
type MetricSectorUpload struct {
	HostKey    consensus.PublicKey
	Contract   types.FileContractID
	Timestamp  time.Time
	Elapsed    time.Duration
	Err        error
	Cost       types.Currency
	Collateral types.Currency
}

// A MetricSectorDownload contains metrics resulting from a sector download.
type MetricSectorDownload struct {
	HostKey    consensus.PublicKey
	Contract   types.FileContractID
	Timestamp  time.Time
	Elapsed    time.Duration
	Err        error
	Downloaded uint64
	Cost       types.Currency
}

// A MetricSectorDeletion contains metrics resulting from a sector deletion.
type MetricSectorDeletion struct {
	HostKey   consensus.PublicKey
	Contract  types.FileContractID
	Timestamp time.Time
	Elapsed   time.Duration
	Err       error
	Cost      types.Currency
	NumRoots  uint64
}

func (MetricSectorUpload) isMetric()   {}
func (MetricSectorDownload) isMetric() {}
func (MetricSectorDeletion) isMetric() {}
