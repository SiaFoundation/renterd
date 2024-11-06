package config

import (
	"os"
	"time"
)

type (
	// Config contains the configuration for a renterd node
	Config struct {
		Seed          string `yaml:"seed,omitempty"`
		Directory     string `yaml:"directory,omitempty"`
		AutoOpenWebUI bool   `yaml:"autoOpenWebUI,omitempty"`
		Network       string `yaml:"network,omitempty"`

		ShutdownTimeout time.Duration `yaml:"shutdownTimeout,omitempty"`

		Log Log `yaml:"log,omitempty"`

		HTTP HTTP `yaml:"http,omitempty"`

		Autopilot Autopilot `yaml:"autopilot,omitempty"`
		Bus       Bus       `yaml:"bus,omitempty"`
		Worker    Worker    `yaml:"worker,omitempty"`
		S3        S3        `yaml:"s3,omitempty"`

		Database Database     `yaml:"database,omitempty"`
		Explorer ExplorerData `yaml:"explorer,omitempty"`
	}

	// ExplorerData contains the configuration for using an external explorer.
	ExplorerData struct {
		Disable bool   `yaml:"disable,omitempty"`
		URL     string `yaml:"url,omitempty"`
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
		// optional fields depending on backend
		MySQL MySQL `yaml:"mysql,omitempty"`
	}

	// Bus contains the configuration for a bus.
	Bus struct {
		AllowPrivateIPs               bool          `yaml:"allowPrivateIPs,omitempty"`
		AnnouncementMaxAgeHours       uint64        `yaml:"announcementMaxAgeHours,omitempty"`
		Bootstrap                     bool          `yaml:"bootstrap,omitempty"`
		GatewayAddr                   string        `yaml:"gatewayAddr,omitempty"`
		RemoteAddr                    string        `yaml:"remoteAddr,omitempty"`
		RemotePassword                string        `yaml:"remotePassword,omitempty"`
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
		Level    string      `yaml:"level,omitempty"` // global log level
		StdOut   StdOut      `yaml:"stdout,omitempty"`
		File     LogFile     `yaml:"file,omitempty"`
		Database DatabaseLog `yaml:"database,omitempty"`
	}

	// SQLite contains the configuration for a SQLite database.
	SQLite struct {
		Database        string `yaml:"database,omitempty"`
		MetricsDatabase string `yaml:"metricsDatabase,omitempty"`
	}

	// MySQL contains the configuration for a MySQL database.
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
		Address           string   `yaml:"address,omitempty"`
		DisableAuth       bool     `yaml:"disableAuth,omitempty"`
		Enabled           bool     `yaml:"enabled,omitempty"`
		HostBucketEnabled bool     `yaml:"hostBucketEnabled,omitempty"`
		HostBucketBases   []string `yaml:"hostBucketBases,omitempty"`
	}

	// Worker contains the configuration for a worker.
	Worker struct {
		Enabled                       bool           `yaml:"enabled,omitempty"`
		ID                            string         `yaml:"id,omitempty"`
		Remotes                       []RemoteWorker `yaml:"remotes,omitempty"`
		AccountsRefillInterval        time.Duration  `yaml:"accountsRefillInterval,omitempty"`
		BusFlushInterval              time.Duration  `yaml:"busFlushInterval,omitempty"`
		ContractLockTimeout           time.Duration  `yaml:"contractLockTimeout,omitempty"`
		DownloadOverdriveTimeout      time.Duration  `yaml:"downloadOverdriveTimeout,omitempty"`
		UploadOverdriveTimeout        time.Duration  `yaml:"uploadOverdriveTimeout,omitempty"`
		DownloadMaxOverdrive          uint64         `yaml:"downloadMaxOverdrive,omitempty"`
		DownloadMaxMemory             uint64         `yaml:"downloadMaxMemory,omitempty"`
		UploadMaxMemory               uint64         `yaml:"uploadMaxMemory,omitempty"`
		UploadMaxOverdrive            uint64         `yaml:"uploadMaxOverdrive,omitempty"`
		AllowUnauthenticatedDownloads bool           `yaml:"allowUnauthenticatedDownloads,omitempty"`
		ExternalAddress               string         `yaml:"externalAddress,omitempty"`
	}

	// Autopilot contains the configuration for an autopilot.
	Autopilot struct {
		Enabled                        bool          `yaml:"enabled,omitempty"`
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

func MySQLConfigFromEnv() MySQL {
	return MySQL{
		URI:             os.Getenv("RENTERD_DB_URI"),
		User:            os.Getenv("RENTERD_DB_USER"),
		Password:        os.Getenv("RENTERD_DB_PASSWORD"),
		Database:        os.Getenv("RENTERD_DB_NAME"),
		MetricsDatabase: os.Getenv("RENTERD_DB_METRICS_NAME"),
	}
}
