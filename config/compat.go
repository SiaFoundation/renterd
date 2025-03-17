package config

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"gopkg.in/yaml.v3"
)

type (
	v111Config struct {
		Seed            string        `yaml:"seed,omitempty"`
		Directory       string        `yaml:"directory,omitempty"`
		AutoOpenWebUI   bool          `yaml:"autoOpenWebUI,omitempty"`
		Network         string        `yaml:"network,omitempty"`
		ShutdownTimeout time.Duration `yaml:"shutdownTimeout,omitempty"`
		HTTP            struct {
			Address  string `yaml:"address,omitempty"`
			Password string `yaml:"password,omitempty"`
		} `yaml:"http,omitempty"`
		Log       v111Log       `yaml:"log,omitempty"`
		Autopilot v111Autopilot `yaml:"autopilot,omitempty"`
		Bus       v111Bus       `yaml:"bus,omitempty"`
		Worker    v111Worker    `yaml:"worker,omitempty"`
		S3        v111S3        `yaml:"s3,omitempty"`
		Database  v111Database  `yaml:"database,omitempty"`
	}

	v111Autopilot struct {
		Enabled                        bool          `yaml:"enabled,omitempty"`
		ID                             string        `yaml:"id,omitempty"`
		Heartbeat                      time.Duration `yaml:"heartbeat,omitempty"`
		MigrationHealthCutoff          float64       `yaml:"migrationHealthCutoff,omitempty"`
		RevisionBroadcastInterval      time.Duration `yaml:"revisionBroadcastInterval,omitempty"`
		RevisionSubmissionBuffer       uint64        `yaml:"revisionSubmissionBuffer,omitempty"`
		ScannerInterval                time.Duration `yaml:"scannerInterval,omitempty"`
		ScannerBatchSize               uint64        `yaml:"scannerBatchSize,omitempty"`
		ScannerNumThreads              uint64        `yaml:"scannerNumThreads,omitempty"`
		MigratorParallelSlabsPerWorker uint64        `yaml:"migratorParallelSlabsPerWorker,omitempty"`
	}

	v111Bus struct {
		AnnouncementMaxAgeHours       uint64        `yaml:"announcementMaxAgeHours,omitempty"`
		Bootstrap                     bool          `yaml:"bootstrap,omitempty"`
		GatewayAddr                   string        `yaml:"gatewayAddr,omitempty"`
		RemoteAddr                    string        `yaml:"remoteAddr,omitempty"`
		RemotePassword                string        `yaml:"remotePassword,omitempty"`
		UsedUTXOExpiry                time.Duration `yaml:"usedUtxoExpiry,omitempty"`
		SlabBufferCompletionThreshold int64         `yaml:"slabBufferCompleionThreshold,omitempty"`
		PersistInterval               time.Duration `yaml:"persistInterval,omitempty"` // deprecated
	}

	v111Worker struct {
		Enabled bool   `yaml:"enabled,omitempty"`
		ID      string `yaml:"id,omitempty"`
		Remotes []struct {
			Address  string `yaml:"address,omitempty"`
			Password string `yaml:"password,omitempty"`
		} `yaml:"remotes,omitempty"`
		AccountsRefillInterval        time.Duration `yaml:"accountsRefillInterval,omitempty"`
		AllowPrivateIPs               bool          `yaml:"allowPrivateIPs,omitempty"`
		BusFlushInterval              time.Duration `yaml:"busFlushInterval,omitempty"`
		ContractLockTimeout           time.Duration `yaml:"contractLockTimeout,omitempty"`
		DownloadOverdriveTimeout      time.Duration `yaml:"downloadOverdriveTimeout,omitempty"`
		UploadOverdriveTimeout        time.Duration `yaml:"uploadOverdriveTimeout,omitempty"`
		DownloadMaxOverdrive          uint64        `yaml:"downloadMaxOverdrive,omitempty"`
		DownloadMaxMemory             uint64        `yaml:"downloadMaxMemory,omitempty"`
		UploadMaxMemory               uint64        `yaml:"uploadMaxMemory,omitempty"`
		UploadMaxOverdrive            uint64        `yaml:"uploadMaxOverdrive,omitempty"`
		AllowUnauthenticatedDownloads bool          `yaml:"allowUnauthenticatedDownloads,omitempty"`
		ExternalAddress               string        `yaml:"externalAddress,omitempty"`
	}

	v111S3 struct {
		Address           string            `yaml:"address,omitempty"`
		DisableAuth       bool              `yaml:"disableAuth,omitempty"`
		Enabled           bool              `yaml:"enabled,omitempty"`
		KeypairsV4        map[string]string `yaml:"keypairsV4,omitempty"` // deprecated
		HostBucketEnabled bool              `yaml:"hostBucketEnabled,omitempty"`
		HostBucketBases   []string          `yaml:"hostBucketBases,omitempty"`
	}

	v111Database struct {
		Log struct {
			Enabled                   bool          `yaml:"enabled,omitempty"`
			Level                     string        `yaml:"level,omitempty"`
			IgnoreRecordNotFoundError bool          `yaml:"ignoreRecordNotFoundError,omitempty"`
			SlowThreshold             time.Duration `yaml:"slowThreshold,omitempty"`
		} `yaml:"log,omitempty"` // deprecated
		MySQL struct {
			URI             string `yaml:"uri,omitempty"`
			User            string `yaml:"user,omitempty"`
			Password        string `yaml:"password,omitempty"`
			Database        string `yaml:"database,omitempty"`
			MetricsDatabase string `yaml:"metricsDatabase,omitempty"`
		} `yaml:"mysql,omitempty"`
	}

	v111Log struct {
		Path   string `yaml:"path,omitempty"`
		Level  string `yaml:"level,omitempty"`
		StdOut struct {
			Level      string `yaml:"level,omitempty"`
			Enabled    bool   `yaml:"enabled,omitempty"`
			Format     string `yaml:"format,omitempty"`
			EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
		} `yaml:"stdout,omitempty"`
		File struct {
			Enabled bool   `yaml:"enabled,omitempty"`
			Level   string `yaml:"level,omitempty"`
			Format  string `yaml:"format,omitempty"`
			Path    string `yaml:"path,omitempty"`
		} `yaml:"file,omitempty"`
		Database v111DatabaseLog `yaml:"database,omitempty"`
	}

	v111DatabaseLog struct {
		Enabled                   bool          `yaml:"enabled,omitempty"`
		Level                     string        `yaml:"level,omitempty"`
		IgnoreRecordNotFoundError bool          `yaml:"ignoreRecordNotFoundError,omitempty"`
		SlowThreshold             time.Duration `yaml:"slowThreshold,omitempty"`
	}
)

func updateConfigV111(fp string, r io.Reader, cfg *Config) error {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	old := v111Config{
		Directory:     ".",
		Seed:          os.Getenv("RENTERD_SEED"),
		AutoOpenWebUI: true,
		Network:       "mainnet",
		HTTP: struct {
			Address  string `yaml:"address,omitempty"`
			Password string `yaml:"password,omitempty"`
		}{
			Address:  "localhost:9980",
			Password: os.Getenv("RENTERD_API_PASSWORD"),
		},
		ShutdownTimeout: 5 * time.Minute,
		Database: v111Database{
			MySQL: struct {
				URI             string `yaml:"uri,omitempty"`
				User            string `yaml:"user,omitempty"`
				Password        string `yaml:"password,omitempty"`
				Database        string `yaml:"database,omitempty"`
				MetricsDatabase string `yaml:"metricsDatabase,omitempty"`
			}{
				User:            "renterd",
				Database:        "renterd",
				MetricsDatabase: "renterd_metrics",
			},
		},
		Log: v111Log{
			Path:  "",
			Level: "",
			File: struct {
				Enabled bool   `yaml:"enabled,omitempty"`
				Level   string `yaml:"level,omitempty"`
				Format  string `yaml:"format,omitempty"`
				Path    string `yaml:"path,omitempty"`
			}{
				Enabled: true,
				Format:  "json",
				Path:    os.Getenv("RENTERD_LOG_FILE"),
			},
			StdOut: struct {
				Level      string `yaml:"level,omitempty"`
				Enabled    bool   `yaml:"enabled,omitempty"`
				Format     string `yaml:"format,omitempty"`
				EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
			}{
				Enabled:    true,
				Format:     "human",
				EnableANSI: runtime.GOOS != "windows",
			},
			Database: v111DatabaseLog{
				Enabled:                   true,
				IgnoreRecordNotFoundError: true,
				SlowThreshold:             100 * time.Millisecond,
			},
		},
		Bus: v111Bus{
			AnnouncementMaxAgeHours:       24 * 7 * 52, // 1 year
			Bootstrap:                     true,
			GatewayAddr:                   ":9981",
			UsedUTXOExpiry:                24 * time.Hour,
			SlabBufferCompletionThreshold: 1 << 12,
		},
		Worker: v111Worker{
			Enabled: true,

			ID:                     "",
			AccountsRefillInterval: 10 * time.Second,
			ContractLockTimeout:    30 * time.Second,
			BusFlushInterval:       5 * time.Second,

			DownloadMaxOverdrive:     5,
			DownloadOverdriveTimeout: 3 * time.Second,

			DownloadMaxMemory:      1 << 30, // 1 GiB
			UploadMaxMemory:        1 << 30, // 1 GiB
			UploadMaxOverdrive:     5,
			UploadOverdriveTimeout: 3 * time.Second,
		},
		Autopilot: v111Autopilot{
			Enabled: true,

			ID:                             "autopilot",
			RevisionSubmissionBuffer:       150,
			Heartbeat:                      30 * time.Minute,
			MigrationHealthCutoff:          0.75,
			RevisionBroadcastInterval:      7 * 24 * time.Hour,
			ScannerBatchSize:               100,
			ScannerInterval:                4 * time.Hour,
			ScannerNumThreads:              10,
			MigratorParallelSlabsPerWorker: 1,
		},
		S3: v111S3{
			Address:     "localhost:8080",
			Enabled:     true,
			DisableAuth: false,
			KeypairsV4:  nil,
		},
	}
	if err := dec.Decode(&old); err != nil {
		return fmt.Errorf("failed to decode config file: %w", err)
	}

	cfg.Seed = old.Seed
	cfg.Directory = old.Directory
	cfg.AutoOpenWebUI = old.AutoOpenWebUI
	cfg.Network = old.Network
	cfg.ShutdownTimeout = old.ShutdownTimeout

	cfg.Log.Level = old.Log.Level
	cfg.Log.StdOut = old.Log.StdOut
	cfg.Log.File = old.Log.File
	cfg.Log.Database = old.Database.Log // moved

	cfg.HTTP = old.HTTP

	cfg.Autopilot.Enabled = old.Autopilot.Enabled
	cfg.Autopilot.Heartbeat = old.Autopilot.Heartbeat
	cfg.Autopilot.MigratorHealthCutoff = old.Autopilot.MigrationHealthCutoff        // renamed
	cfg.Autopilot.MigratorNumThreads = old.Autopilot.MigratorParallelSlabsPerWorker // renamed
	cfg.Autopilot.RevisionBroadcastInterval = old.Autopilot.RevisionBroadcastInterval
	cfg.Autopilot.RevisionSubmissionBuffer = old.Autopilot.RevisionSubmissionBuffer
	cfg.Autopilot.ScannerInterval = old.Autopilot.ScannerInterval
	cfg.Autopilot.ScannerBatchSize = old.Autopilot.ScannerBatchSize
	cfg.Autopilot.ScannerNumThreads = old.Autopilot.ScannerNumThreads

	cfg.Bus.AllowPrivateIPs = old.Worker.AllowPrivateIPs // moved
	cfg.Bus.AnnouncementMaxAgeHours = old.Bus.AnnouncementMaxAgeHours
	cfg.Bus.Bootstrap = old.Bus.Bootstrap
	cfg.Bus.GatewayAddr = old.Bus.GatewayAddr
	cfg.Bus.RemoteAddr = old.Bus.RemoteAddr
	cfg.Bus.RemotePassword = old.Bus.RemotePassword
	cfg.Bus.UsedUTXOExpiry = old.Bus.UsedUTXOExpiry
	cfg.Bus.SlabBufferCompletionThreshold = old.Bus.SlabBufferCompletionThreshold

	cfg.Worker.Enabled = old.Worker.Enabled
	cfg.Worker.ID = old.Worker.ID
	cfg.Worker.AccountsRefillInterval = old.Worker.AccountsRefillInterval
	cfg.Worker.BusFlushInterval = old.Worker.BusFlushInterval
	cfg.Worker.DownloadOverdriveTimeout = old.Worker.DownloadOverdriveTimeout
	cfg.Worker.UploadOverdriveTimeout = old.Worker.UploadOverdriveTimeout
	cfg.Worker.DownloadMaxOverdrive = old.Worker.DownloadMaxOverdrive
	cfg.Worker.DownloadMaxMemory = old.Worker.DownloadMaxMemory
	cfg.Worker.UploadMaxMemory = old.Worker.UploadMaxMemory
	cfg.Worker.UploadMaxOverdrive = old.Worker.UploadMaxOverdrive
	cfg.Worker.AllowUnauthenticatedDownloads = old.Worker.AllowUnauthenticatedDownloads

	cfg.S3.Address = old.S3.Address
	cfg.S3.DisableAuth = old.S3.DisableAuth
	cfg.S3.Enabled = old.S3.Enabled
	cfg.S3.HostBucketEnabled = old.S3.HostBucketEnabled
	cfg.S3.HostBucketBases = old.S3.HostBucketBases

	cfg.Database.MySQL = old.Database.MySQL

	tmpFilePath := fp + ".tmp"
	f, err := os.Create(tmpFilePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	enc := yaml.NewEncoder(f)
	enc.SetIndent(2)
	if err := enc.Encode(cfg); err != nil {
		return fmt.Errorf("failed to encode config file: %w", err)
	} else if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	} else if err := os.Rename(tmpFilePath, fp); err != nil {
		return fmt.Errorf("failed to rename file: %w", err)
	}
	return nil
}
