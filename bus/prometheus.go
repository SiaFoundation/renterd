package bus

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/prometheus"
	"go.sia.tech/renterd/wallet"
)

type AlertsResp []alerts.Alert

// PrometheusMetric returns Prometheus samples for the hosts alerts.
func (a AlertsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, alert := range a {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_alert",
			Labels: map[string]any{
				"id":        alert.ID,
				"severity":  alert.Severity.String(),
				"message":   alert.Message,
				"timestamp": alert.Timestamp,
			},
			Value: 1,
		})
	}
	return
}

type BucketResp []api.Bucket

func (b BucketResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, bucket := range b {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_bucket",
			Labels: map[string]any{
				"name": bucket.Name,
				"publicReadAccess": func() float64 {
					if bucket.Policy.PublicReadAccess {
						return 1
					}
					return 0
				}(),
				"createdAt": bucket.CreatedAt.String(),
			},
			Value: 1,
		})
	}
	return
}

type ConsensusStateResp api.ConsensusState

func (c ConsensusStateResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_consensus_state_synced",
		Value: func() float64 {
			if c.Synced {
				return 1
			}
			return 0
		}(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_consensus_state_chain_index_height",
		Value: float64(c.BlockHeight),
	})
	return
}

type ContractsResp []api.ContractMetadata

func (c ContractsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, contract := range c {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_contract",
			Labels: map[string]any{
				"hostIP":        contract.HostIP,
				"state":         contract.State,
				"hostKey":       contract.HostKey.String(),
				"siamuxAddr":    contract.SiamuxAddr,
				"contractPrice": contract.ContractPrice.Siacoins(),
			},
			Value: contract.TotalCost.Siacoins(),
		})
	}
	return
}

type ContractsPrunableResp api.ContractsPrunableDataResponse

func (c ContractsPrunableResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_prunable_contracts_total",
		Value: float64(c.TotalPrunable),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_prunable_contracts_size",
		Value: float64(c.TotalSize),
	})
	return
}

type HostsResp []hostdb.Host

func (h HostsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, host := range h {
		priceTableLabels := map[string]any{
			"netAddress": host.NetAddress,
			"uid":        host.PriceTable.UID.String(),
			"expiry":     host.PriceTable.Expiry.Local().Format("2006-01-02T15:04:05Z07:00"),
		}
		settingsLabels := map[string]any{
			"netAddress": host.NetAddress,
			"version":    host.Settings.Version,
			"siamuxPort": host.Settings.SiaMuxPort,
		}
		netAddressLabel := map[string]any{
			"netAddress": host.NetAddress,
		}

		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_scanned",
			Labels: netAddressLabel,
			Value:  1,
		})

		// price table
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_validity",
			Labels: priceTableLabels,
			Value:  float64(host.PriceTable.Validity.Milliseconds()),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_hostblockheight",
			Labels: priceTableLabels,
			Value:  float64(host.PriceTable.HostBlockHeight),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_updatepricetablecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.UpdatePriceTableCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_accountbalancecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.AccountBalanceCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_fundaccountcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.FundAccountCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_latestrevisioncost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.LatestRevisionCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_subscriptionmemorycost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.SubscriptionMemoryCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_subscriptionnotificationcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.SubscriptionNotificationCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_initbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.InitBaseCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_memorytimecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.MemoryTimeCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_downloadbandwidthcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.DownloadBandwidthCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_uploadbandwidthcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.UploadBandwidthCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_dropsectorsbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.DropSectorsBaseCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_dropsectorsunitcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.DropSectorsUnitCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_hassectorbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.HasSectorBaseCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_readbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.ReadBaseCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_readlengthcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.ReadLengthCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_renewcontractcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.RenewContractCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_revisionbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.RevisionBaseCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_swapsectorcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.SwapSectorBaseCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_writebasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.WriteBaseCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_writelengthcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.WriteLengthCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_writestorecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.WriteStoreCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_txnfeeminrecommended",
			Labels: priceTableLabels,
			Value:  host.PriceTable.TxnFeeMinRecommended.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_txnfeemaxrecommended",
			Labels: priceTableLabels,
			Value:  host.PriceTable.TxnFeeMaxRecommended.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_contractprice",
			Labels: priceTableLabels,
			Value:  host.PriceTable.ContractPrice.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_collateralcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.CollateralCost.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_maxcollateral",
			Labels: priceTableLabels,
			Value:  host.PriceTable.MaxCollateral.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_maxduration",
			Labels: priceTableLabels,
			Value:  float64(host.PriceTable.MaxDuration),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_pricetable_windowsize",
			Labels: priceTableLabels,
			Value:  float64(host.PriceTable.WindowSize),
		})

		// settings
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_acceptingcontracts",
			Labels: settingsLabels,
			Value: func() float64 {
				if host.Settings.AcceptingContracts {
					return 1
				}
				return 0
			}(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_baserpcprice",
			Labels: settingsLabels,
			Value:  host.Settings.BaseRPCPrice.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_collateral",
			Labels: settingsLabels,
			Value:  host.Settings.Collateral.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_contractprice",
			Labels: settingsLabels,
			Value:  host.Settings.ContractPrice.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_downloadbandwidthprice",
			Labels: settingsLabels,
			Value:  host.Settings.DownloadBandwidthPrice.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_ephemeralaccountexpiry",
			Labels: settingsLabels,
			Value:  float64(host.Settings.EphemeralAccountExpiry.Milliseconds()),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_maxcollateral",
			Labels: settingsLabels,
			Value:  host.Settings.MaxCollateral.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_maxdownloadbatchsize",
			Labels: settingsLabels,
			Value:  float64(host.Settings.MaxDownloadBatchSize),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_maxduration",
			Labels: settingsLabels,
			Value:  float64(host.Settings.MaxDuration),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_maxephemeralaccountbalance",
			Labels: settingsLabels,
			Value:  host.Settings.MaxEphemeralAccountBalance.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_maxrevisebatchsize",
			Labels: settingsLabels,
			Value:  float64(host.Settings.MaxReviseBatchSize),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_remainingstorage",
			Labels: settingsLabels,
			Value:  float64(host.Settings.RemainingStorage),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_revisionnumber",
			Labels: settingsLabels,
			Value:  float64(host.Settings.RevisionNumber),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_sectoraccessprice",
			Labels: settingsLabels,
			Value:  host.Settings.SectorAccessPrice.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_sectorsize",
			Labels: settingsLabels,
			Value:  float64(host.Settings.SectorSize),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_storageprice",
			Labels: settingsLabels,
			Value:  host.Settings.StoragePrice.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_totalstorage",
			Labels: settingsLabels,
			Value:  float64(host.Settings.TotalStorage),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_uploadbandwidthprice",
			Labels: settingsLabels,
			Value:  host.Settings.UploadBandwidthPrice.Siacoins(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_settings_windowsize",
			Labels: settingsLabels,
			Value:  float64(host.Settings.WindowSize),
		})

		// interactions
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_interactions_totalscans",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.TotalScans),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_interactions_lastscansuccess",
			Labels: netAddressLabel,
			Value: func() float64 {
				if host.Interactions.LastScanSuccess {
					return 1
				}
				return 0
			}(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_interactions_lostsectors",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.LostSectors),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_interactions_secondtolastscansuccess",
			Labels: netAddressLabel,
			Value: func() float64 {
				if host.Interactions.SecondToLastScanSuccess {
					return 1
				}
				return 0
			}(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_interactions_uptime",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.Uptime.Milliseconds()),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_interactions_downtime",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.Downtime.Milliseconds()),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_interactions_successfulinteractions",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.SuccessfulInteractions),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_host_interactions_failedinteractions",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.FailedInteractions),
		})
	}
	return
}

type AllowListResp []types.PublicKey

func (a AllowListResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, host := range a {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_allowed_host",
			Labels: map[string]any{
				"publicKey": host,
			},
			Value: 1,
		})
	}
	return
}

type BlocklistResp []string

func (b BlocklistResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, host := range b {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_blocked_host",
			Labels: map[string]any{
				"publicKey": host,
			},
			Value: 1,
		})
	}
	return
}

type HostsScanningResp []hostdb.HostAddress

func (b HostsScanningResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, host := range b {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_hosts_scanning",
			Labels: map[string]any{
				"netAddress": host.NetAddress,
				"publicKey":  host.PublicKey.String(),
			},
			Value: 1,
		})
	}
	return
}

type GougingParamsResp api.GougingParams

func (gp GougingParamsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = FormatSettingsMetricName(gp, "gouging")
	return
}

type UploadParamsResp api.UploadParams

func (up UploadParamsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = FormatSettingsMetricName(GougingParamsResp(up.GougingParams), "upload")
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_upload_currentheight",
		Labels: map[string]any{
			"contractset": up.ContractSet,
			"uploadpacking": func() float64 {
				if up.UploadPacking {
					return 1
				}
				return 0
			}(),
		},
		Value: float64(up.CurrentHeight),
	})
	return
}

func FormatSettingsMetricName(gp GougingParamsResp, name string) (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name: fmt.Sprintf("renterd_%s_consensusstate_synced", name),
		Value: func() float64 {
			if gp.ConsensusState.Synced {
				return 1
			}
			return 0
		}(),
	})
	if name == "gouging" { //upload setting already has renterd_upload_currentheight
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_gouging_currentheight",
			Labels: map[string]any{
				"lastBlockTime": gp.ConsensusState.LastBlockTime.String(),
			},
			Value: float64(gp.ConsensusState.BlockHeight),
		})
	}
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_minmaxcollateral", name),
		Value: gp.GougingSettings.MinMaxCollateral.Siacoins(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_maxrpcprice", name),
		Value: gp.GougingSettings.MaxRPCPrice.Siacoins(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_maxcontractprice", name),
		Value: gp.GougingSettings.MaxContractPrice.Siacoins(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_maxdownloadprice", name),
		Value: gp.GougingSettings.MaxDownloadPrice.Siacoins(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_maxuploadprice", name),
		Value: gp.GougingSettings.MaxUploadPrice.Siacoins(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_maxstorageprice", name),
		Value: gp.GougingSettings.MaxStoragePrice.Siacoins(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_hostblockheightleeway", name),
		Value: float64(gp.GougingSettings.HostBlockHeightLeeway),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_minpricetablevalidity", name),
		Value: float64(gp.GougingSettings.MinPriceTableValidity.Milliseconds()),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_minaccountexpiry", name),
		Value: float64(gp.GougingSettings.MinAccountExpiry.Milliseconds()),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_minmaxephemeralaccountbalance", name),
		Value: gp.GougingSettings.MinMaxEphemeralAccountBalance.Siacoins(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_settings_migrationsurchargemultiplier", name),
		Value: float64(gp.GougingSettings.MigrationSurchargeMultiplier),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_redundancy_settings_minshards", name),
		Value: float64(gp.RedundancySettings.MinShards),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_redundancy_settings_totalshards", name),
		Value: float64(gp.RedundancySettings.TotalShards),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_transactionfee", name),
		Value: gp.TransactionFee.Siacoins(),
	})
	return
}

type SlabBuffersResp []api.SlabBuffer

func (sb SlabBuffersResp) PrometheusMetric() (metrics []prometheus.Metric) {
	totalSize := int64(0)
	for _, buffer := range sb {
		totalSize += buffer.Size
	}
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_slabbuffers_totalsize",
		Value: float64(totalSize),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_slabbuffers_totalslabs",
		Value: float64(len(sb)),
	})
	return
}

type SearchObjectsResp []api.ObjectMetadata

func (so SearchObjectsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	unavailableObjs := 0
	avgHealth := float64(0.0)
	for _, obj := range so {
		if obj.Health == 0 {
			unavailableObjs += 1
		}
		avgHealth += obj.Health
	}
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_objects_avghealth",
		Value: float64(avgHealth),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_objects_unavailable",
		Value: float64(unavailableObjs),
	})
	return
}

// No specific prometheus response from:
// /setting/contractset - already available in /params/upload
// /setting/gouging - already available in /params/gouging
// /setting/redundancy - already available in /params/gouging
// /setting/uploadpacking - already available in /params/upload
//
// Prometheus specific:
// /setting/s3authentication
type SettingsResp map[string]interface{}

func (s SettingsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for k := range s["v4Keypairs"].(map[string]interface{}) {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_settings_s3v4keypair",
			Labels: map[string]any{
				"publicKey": k,
			},
			Value: 1,
		})
	}
	return
}

type StateResp api.BusStateResponse

func (sr StateResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_state",
		Labels: map[string]any{
			"network":   sr.Network,
			"version":   sr.Version,
			"commit":    sr.Commit,
			"starttime": sr.StartTime.String(),
			"os":        sr.OS,
			"buildtime": sr.BuildTime.String(),
		},
		Value: 1,
	})
	return
}

type ObjectsStatsResp api.ObjectsStatsResponse

func (os ObjectsStatsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_stats_numobjects",
		Value: float64(os.NumObjects),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_stats_numunfinishedobjects",
		Value: float64(os.NumUnfinishedObjects),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_stats_minhealth",
		Value: os.MinHealth,
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_stats_totalobjectsize",
		Value: float64(os.TotalObjectsSize),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_stats_totalunfinishedobjectssize",
		Value: float64(os.TotalUnfinishedObjectsSize),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_stats_totalsectorssize",
		Value: float64(os.TotalSectorsSize),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_stats_totaluploadedsize",
		Value: float64(os.TotalUploadedSize),
	})
	return
}

type SyncerAddrResp string

func (sar SyncerAddrResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_syncer_address",
		Labels: map[string]any{
			"address": sar,
		},
		Value: 1,
	})
	return
}

type SyncerPeersResp []string

func (srp SyncerPeersResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, p := range srp {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_syncer_peer",
			Labels: map[string]any{
				"address": p,
			},
			Value: 1,
		})
	}
	return
}

type TxPoolResp string

func (t TxPoolResp) ToFloat64() (float64, error) {
	// Convert string to float64
	return strconv.ParseFloat(string(t), 64)
}

func (tpr TxPoolResp) PrometheusMetric() (metrics []prometheus.Metric) {
	floatValue, err := tpr.ToFloat64()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_tpool_fee",
		Value: floatValue,
	})
	return
}

type TxPoolTxResp []types.Transaction

func (tpr TxPoolTxResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_txpool_numtxns",
		Value: float64(len(tpr)),
	})
	return
}

type WalletResp api.WalletResponse

func (w WalletResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_wallet_scanheight",
		Value: float64(w.ScanHeight),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_wallet_spendable",
		Value: w.Spendable.Siacoins(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_wallet_confirmed",
		Value: w.Confirmed.Siacoins(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_wallet_unconfirmed",
		Value: w.Unconfirmed.Siacoins(),
	})
	return
}

type WalletOutputsResp []wallet.SiacoinElement

func (utxos WalletOutputsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_wallet_numoutputs",
		Value: float64(len(utxos)),
	})
	return
}

type WalletPendingResp []types.Transaction

func (w WalletPendingResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, txn := range w {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_wallet_transacaction_pending_inflow",
			Labels: map[string]any{
				"txid": txn.ID().String(),
			},
			Value: float64(len(txn.SiacoinInputs)),
		})
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_wallet_transaction_pending_outflow",
			Labels: map[string]any{
				"txid": txn.ID().String(),
			},
			Value: float64(len(txn.SiacoinOutputs)),
		})
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_wallet_transaction_pending_minerfee",
			Labels: map[string]any{
				"txid": txn.ID().String(),
			},
			Value: txn.MinerFees[0].Siacoins(),
		})
	}
	return
}

type WalletTransactionsResp []wallet.Transaction

func (w WalletTransactionsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, txn := range w {
		labels := map[string]any{
			"height":    strconv.FormatUint(txn.Index.Height, 10),
			"timestamp": txn.Timestamp.Format(time.RFC3339),
			"txid":      strings.Split(txn.ID.String(), ":")[1],
		}
		var value float64
		if txn.Inflow.Cmp(txn.Outflow) > 0 { // inflow > outflow = positive value
			value = txn.Inflow.Sub(txn.Outflow).Siacoins()
		} else { // inflow < outflow = negative value
			value = txn.Outflow.Sub(txn.Inflow).Siacoins() * -1
		}
		metrics = append(metrics, prometheus.Metric{
			Name:   "renterd_wallet_transaction",
			Labels: labels,
			Value:  value,
		})
		if len(txn.Raw.MinerFees) != 0 {
			metrics = append(metrics, prometheus.Metric{
				Name:   "renterd_wallet_transaction_minerfee",
				Labels: labels,
				Value:  txn.Raw.MinerFees[0].Siacoins(),
			})
		}
	}
	return
}
