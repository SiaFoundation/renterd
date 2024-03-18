package test

import (
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

var (
	// AutopilotConfig is the autopilot used for testing unless a different
	// one is explicitly set.
	AutopilotConfig = api.AutopilotConfig{
		Contracts: api.ContractsConfig{
			Allowance:   types.Siacoins(1).Mul64(1e3),
			Amount:      3,
			Period:      144,
			RenewWindow: 72,

			Download: rhpv2.SectorSize * 500,
			Upload:   rhpv2.SectorSize * 500,
			Storage:  rhpv2.SectorSize * 5e3,

			Set:   ContractSet,
			Prune: false,
		},
		Hosts: api.HostsConfig{
			MaxDowntimeHours:      10,
			MinRecentScanFailures: 10,
			AllowRedundantIPs:     true, // allow for integration tests by default
		},
	}

	ContractSet         = "testset"
	ContractSetSettings = api.ContractSetSetting{
		Default: ContractSet,
	}

	GougingSettings = api.GougingSettings{
		MaxRPCPrice:      types.Siacoins(1).Div64(1000),        // 1mS per RPC
		MaxContractPrice: types.Siacoins(10),                   // 10 SC per contract
		MaxDownloadPrice: types.Siacoins(1).Mul64(1000),        // 1000 SC per 1 TiB
		MaxUploadPrice:   types.Siacoins(1).Mul64(1000),        // 1000 SC per 1 TiB
		MaxStoragePrice:  types.Siacoins(1000).Div64(144 * 30), // 1000 SC per month

		HostBlockHeightLeeway: 240, // amount of leeway given to host block height

		MinPriceTableValidity:         10 * time.Second,  // minimum value for price table validity
		MinAccountExpiry:              time.Hour,         // minimum value for account expiry
		MinMaxEphemeralAccountBalance: types.Siacoins(1), // 1SC
	}

	RedundancySettings = api.RedundancySettings{
		MinShards:   2,
		TotalShards: 3,
	}

	S3AccessKeyID     = "TESTINGYNHUWCPKOPSYQ"
	S3SecretAccessKey = "Rh30BNyj+qNI4ftYRteoZbHJ3X4Ln71QtZkRXzJ9"
	S3Credentials     = credentials.NewStaticV4(S3AccessKeyID, S3SecretAccessKey, "")
)
