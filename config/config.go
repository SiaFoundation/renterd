package config

import (
	"time"
)

type (
	// Config contains the configuration for a renterd node
	Config struct {
		Seed      string `yaml:"seed"`
		Directory string `yaml:"directory"`

		ShutdownTimeout time.Duration `yaml:"shutdownTimeout"`

		Log Log `yaml:"log"`

		HTTP      HTTP      `yaml:"http"`
		Bus       Bus       `yaml:"bus"`
		Worker    Worker    `yaml:"worker"`
		S3        S3        `yaml:"s3"`
		Autopilot Autopilot `yaml:"autopilot"`

		Database Database `yaml:"database"`
		Tracing  Tracing  `yaml:"tracing"`
	}

	// HTTP contains the configuration for the HTTP server.
	HTTP struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	DatabaseLog struct {
		IgnoreRecordNotFoundError bool          `yaml:"ignoreRecordNotFoundError"`
		SlowThreshold             time.Duration `yaml:"slowThreshold"`
	}

	Database struct {
		Log DatabaseLog `yaml:"log"`
		// optional fields depending on backend
		MySQL MySQL `yaml:"mysql"`
	}

	// Tracing contains the configuration for tracing.
	Tracing struct {
		Enabled    bool   `yaml:"enabled"`
		InstanceID string `yaml:"instanceID"`
	}

	// Bus contains the configuration for a bus.
	Bus struct {
		AnnouncementMaxAgeHours       uint64        `yaml:"announcementMaxAgeHours"`
		Bootstrap                     bool          `yaml:"bootstrap"`
		GatewayAddr                   string        `yaml:"gatewayAddr"`
		RemoteAddr                    string        `yaml:"remoteAddr"`
		RemotePassword                string        `yaml:"remotePassword"`
		PersistInterval               time.Duration `yaml:"persistInterval"`
		UsedUTXOExpiry                time.Duration `yaml:"usedUTXOExpiry"`
		SlabBufferCompletionThreshold int64         `yaml:"slabBufferCompleionThreshold"`
	}

	// Log contains the configuration for the logger.
	Log struct {
		Path  string `yaml:"path"`
		Level string `yaml:"level"`
	}

	// MySQL contains the configuration for an optional MySQL database.
	MySQL struct {
		URI      string `yaml:"URI"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
	}

	RemoteWorker struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	S3 struct {
		Address           string            `yaml:"address"`
		DisableAuth       bool              `yaml:"disableAuth"`
		Enabled           bool              `yaml:"enabled"`
		KeypairsV4        map[string]string `yaml:"keypairsV4"`
		HostBucketEnabled bool              `yaml:"hostBucketEnabled"`
	}

	// Worker contains the configuration for a worker.
	Worker struct {
		Enabled                       bool           `yaml:"enabled"`
		ID                            string         `yaml:"ID"`
		Remotes                       []RemoteWorker `yaml:"remotes"`
		AllowPrivateIPs               bool           `yaml:"allowPrivateIPs"`
		BusFlushInterval              time.Duration  `yaml:"busFlushInterval"`
		ContractLockTimeout           time.Duration  `yaml:"contractLockTimeout"`
		DownloadOverdriveTimeout      time.Duration  `yaml:"downloadOverdriveTimeout"`
		UploadOverdriveTimeout        time.Duration  `yaml:"uploadOverdriveTimeout"`
		DownloadMaxOverdrive          uint64         `yaml:"downloadMaxOverdrive"`
		UploadMaxOverdrive            uint64         `yaml:"uploadMaxOverdrive"`
		AllowUnauthenticatedDownloads bool           `yaml:"allowUnauthenticatedDownloads"`
	}

	// Autopilot contains the configuration for an autopilot.
	Autopilot struct {
		Enabled                        bool          `yaml:"enabled"`
		AccountsRefillInterval         time.Duration `yaml:"accountsRefillInterval"`
		Heartbeat                      time.Duration `yaml:"heartbeat"`
		MigrationHealthCutoff          float64       `yaml:"migrationHealthCutoff"`
		RevisionBroadcastInterval      time.Duration `yaml:"revisionBroadcastInterval"`
		RevisionSubmissionBuffer       uint64        `yaml:"revisionSubmissionBuffer"`
		ScannerInterval                time.Duration `yaml:"scannerInterval"`
		ScannerBatchSize               uint64        `yaml:"scannerBatchSize"`
		ScannerMinRecentFailures       uint64        `yaml:"scannerMinRecentFailures"`
		ScannerNumThreads              uint64        `yaml:"scannerNumThreads"`
		MigratorParallelSlabsPerWorker uint64        `yaml:"migratorParallelSlabsPerWorker"`
	}
)
