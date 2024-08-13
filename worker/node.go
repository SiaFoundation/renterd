package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/worker/s3"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
)

func NewNode(cfg config.Worker, s3Opts s3.Opts, b Bus, seed types.PrivateKey, l *zap.Logger) (http.Handler, http.Handler, func(context.Context, string, string) error, func(context.Context) error, error) {
	workerKey := blake2b.Sum256(append([]byte("worker"), seed...))
	w, err := New(workerKey, cfg.ID, b, cfg.ContractLockTimeout, cfg.BusFlushInterval, cfg.DownloadOverdriveTimeout, cfg.UploadOverdriveTimeout, cfg.DownloadMaxOverdrive, cfg.UploadMaxOverdrive, cfg.DownloadMaxMemory, cfg.UploadMaxMemory, cfg.AllowPrivateIPs, l)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	s3Handler, err := s3.New(b, w, l.Named("s3").Sugar(), s3Opts)
	if err != nil {
		err = errors.Join(err, w.Shutdown(context.Background()))
		return nil, nil, nil, nil, fmt.Errorf("failed to create s3 handler: %w", err)
	}
	return w.Handler(), s3Handler, w.Setup, w.Shutdown, nil
}
