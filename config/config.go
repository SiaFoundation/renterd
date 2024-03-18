package config

import (
	"time"
)

type (
	// Config contains the configuration for a renterd node
	Config struct {
		Seed      string `yaml:"seed,omitempty"`
		Directory string `yaml:"directory,omitempty"`

		ShutdownTimeout time.Duration `yaml:"shutdownTimeout,omitempty"`

		Log Log `yaml:"log,omitempty"`

		HTTP      HTTP      `yaml:"http,omitempty"`
		Bus       Bus       `yaml:"bus,omitempty"`
		Worker    Worker    `yaml:"worker,omitempty"`
		S3        S3        `yaml:"s3,omitempty"`
		Autopilot Autopilot `yaml:"autopilot,omitempty"`

		Database Database `yaml:"database,omitempty"`
	}

	// HTTP contains the configuration for the HTTP server.
	HTTP struct {
		Address  string `yaml:"address,omitempty"`
		Password string `yaml:"password,omitempty"`
	}

	DatabaseLog struct {
		Enabled                   bool          `yaml:"enabled,omitempty"`
		Level                     string        `yaml:"level,omitempty"`
		IgnoreRecordNotFoundError bool          `yaml:"ignoreRecordNotFoundError,omitempty"`
		SlowThreshold             time.Duration `yaml:"slowThreshold,omitempty"`
	}

	Database struct {
		Log DatabaseLog `yaml:"log,omitempty"` // deprecated. included for compatibility.
		// optional fields depending on backend
		MySQL MySQL `yaml:"mysql,omitempty"`
	}

	// Bus contains the configuration for a bus.
	Bus struct {
		AnnouncementMaxAgeHours       uint64        `yaml:"announcementMaxAgeHours,omitempty"`
		Bootstrap                     bool          `yaml:"bootstrap,omitempty"`
		GatewayAddr                   string        `yaml:"gatewayAddr,omitempty"`
		RemoteAddr                    string        `yaml:"remoteAddr,omitempty"`
		RemotePassword                string        `yaml:"remotePassword,omitempty"`
		PersistInterval               time.Duration `yaml:"persistInterval,omitempty"`
		UsedUTXOExpiry                time.Duration `yaml:"usedUtxoExpiry,omitempty"`
		SlabBufferCompletionThreshold int64         `yaml:"slabBufferCompleionThreshold,omitempty"`
	}

	// LogFile configures the file output of the logger.
	LogFile struct {
		Enabled bool   `yaml:"enabled,omitempty"`
		Level   string `yaml:"level,omitempty"` // override the file log level
		Format  string `yaml:"format,omitempty"`
		// Path is the path of the log file.
		Path string `yaml:"path,omitempty"`
	}

	// StdOut configures the standard output of the logger.
	StdOut struct {
		Level      string `yaml:"level,omitempty"` // override the stdout log level
		Enabled    bool   `yaml:"enabled,omitempty"`
		Format     string `yaml:"format,omitempty"`
		EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
	}

	Log struct {
		Path     string      `yaml:"path,omitempty"`  // deprecated. included for compatibility.
		Level    string      `yaml:"level,omitempty"` // global log level
		StdOut   StdOut      `yaml:"stdout,omitempty"`
		File     LogFile     `yaml:"file,omitempty"`
		Database DatabaseLog `yaml:"database,omitempty"`
	}

	// MySQL contains the configuration for an optional MySQL database.
	MySQL struct {
		URI             string `yaml:"uri,omitempty"`
		User            string `yaml:"user,omitempty"`
		Password        string `yaml:"password,omitempty"`
		Database        string `yaml:"database,omitempty"`
		MetricsDatabase string `yaml:"metricsDatabase,omitempty"`
	}

	RemoteWorker struct {
		Address  string `yaml:"address,omitempty"`
		Password string `yaml:"password,omitempty"`
	}

	S3 struct {
		Address           string            `yaml:"address,omitempty"`
		DisableAuth       bool              `yaml:"disableAuth,omitempty"`
		Enabled           bool              `yaml:"enabled,omitempty"`
		KeypairsV4        map[string]string `yaml:"keypairsV4,omitempty"`
		HostBucketEnabled bool              `yaml:"hostBucketEnabled,omitempty"`
	}

	// Worker contains the configuration for a worker.
	Worker struct {
		Enabled                       bool           `yaml:"enabled,omitempty"`
		ID                            string         `yaml:"id,omitempty"`
		Remotes                       []RemoteWorker `yaml:"remotes,omitempty"`
		AllowPrivateIPs               bool           `yaml:"allowPrivateIPs,omitempty"`
		BusFlushInterval              time.Duration  `yaml:"busFlushInterval,omitempty"`
		ContractLockTimeout           time.Duration  `yaml:"contractLockTimeout,omitempty"`
		DownloadOverdriveTimeout      time.Duration  `yaml:"downloadOverdriveTimeout,omitempty"`
		UploadOverdriveTimeout        time.Duration  `yaml:"uploadOverdriveTimeout,omitempty"`
		DownloadMaxOverdrive          uint64         `yaml:"downloadMaxOverdrive,omitempty"`
		DownloadMaxMemory             uint64         `yaml:"downloadMaxMemory,omitempty"`
		UploadMaxMemory               uint64         `yaml:"uploadMaxMemory,omitempty"`
		UploadMaxOverdrive            uint64         `yaml:"uploadMaxOverdrive,omitempty"`
		AllowUnauthenticatedDownloads bool           `yaml:"allowUnauthenticatedDownloads,omitempty"`
	}

	// Autopilot contains the configuration for an autopilot.
	Autopilot struct {
		Enabled                        bool          `yaml:"enabled,omitempty"`
		AccountsRefillInterval         time.Duration `yaml:"accountsRefillInterval,omitempty"`
		Heartbeat                      time.Duration `yaml:"heartbeat,omitempty"`
		MigrationHealthCutoff          float64       `yaml:"migrationHealthCutoff,omitempty"`
		RevisionBroadcastInterval      time.Duration `yaml:"revisionBroadcastInterval,omitempty"`
		RevisionSubmissionBuffer       uint64        `yaml:"revisionSubmissionBuffer,omitempty"`
		ScannerInterval                time.Duration `yaml:"scannerInterval,omitempty"`
		ScannerBatchSize               uint64        `yaml:"scannerBatchSize,omitempty"`
		ScannerNumThreads              uint64        `yaml:"scannerNumThreads,omitempty"`
		MigratorParallelSlabsPerWorker uint64        `yaml:"migratorParallelSlabsPerWorker,omitempty"`
	}
)
