package api

import (
	"fmt"
	"strconv"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/internal/prometheus"
)

// See https://prometheus.io/docs/practices/naming/ for naming conventions
// on metrics and labels.

func boolToFloat(x bool) float64 {
	if x {
		return 1
	}
	return 0
}

func (c ConsensusState) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "renterd_consensus_state_synced",
			Value: boolToFloat(c.Synced),
		},
		{
			Name:  "renterd_consensus_state_chain_index_height",
			Value: float64(c.BlockHeight),
		},
	}
}

func (asr AutopilotStateResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	labels := map[string]any{
		"version":    asr.Version,
		"commit":     asr.Commit,
		"os":         asr.OS,
		"build_time": asr.BuildTime.String(),
	}
	return []prometheus.Metric{
		{
			Name:   "renterd_autopilot_state_uptimems",
			Labels: labels,
			Value:  float64(asr.UptimeMS),
		},
		{
			Name:   "renterd_autopilot_state_configured",
			Labels: labels,
			Value:  boolToFloat(asr.Configured),
		},
		{
			Name:   "renterd_autopilot_state_migrating",
			Labels: labels,
			Value:  boolToFloat(asr.Migrating),
		},
		{
			Name:   "renterd_autopilot_state_migratinglaststart",
			Labels: labels,
			Value:  float64(time.Time(asr.MigratingLastStart).Unix()),
		},
		{
			Name:   "renterd_autopilot_state_pruning",
			Labels: labels,
			Value:  boolToFloat(asr.Pruning),
		},
		{
			Name:   "renterd_autopilot_state_pruninglaststart",
			Labels: labels,
			Value:  float64(time.Time(asr.PruningLastStart).Unix()),
		},
		{
			Name:   "renterd_autopilot_state_scanning",
			Labels: labels,
			Value:  boolToFloat(asr.Scanning),
		},
		{
			Name:   "renterd_autopilot_state_scanninglaststart",
			Labels: labels,
			Value:  float64(time.Time(asr.ScanningLastStart).Unix()),
		},
	}
}

func (b Bucket) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name: "renterd_bucket",
			Labels: map[string]any{
				"name":             b.Name,
				"publicReadAccess": boolToFloat(b.Policy.PublicReadAccess),
				"createdAt":        b.CreatedAt.String(),
			},
			Value: 1,
		}}
}

func (host Host) PrometheusMetric() (metrics []prometheus.Metric) {
	priceTableLabels := map[string]any{
		"net_address": host.NetAddress,
		"uid":         host.PriceTable.UID.String(),
		"expiry":      host.PriceTable.Expiry.Local().Format(time.RFC3339),
	}
	settingsLabels := map[string]any{
		"net_address": host.NetAddress,
		"version":     host.Settings.Version,
		"siamux_port": host.Settings.SiaMuxPort,
	}
	netAddressLabel := map[string]any{
		"net_address": host.NetAddress,
	}

	return []prometheus.Metric{
		{
			Name:   "renterd_host_scanned",
			Labels: netAddressLabel,
			Value:  1,
		},

		// price table
		{
			Name:   "renterd_host_pricetable_validity",
			Labels: priceTableLabels,
			Value:  float64(host.PriceTable.Validity.Milliseconds()),
		},
		{
			Name:   "renterd_host_pricetable_hostblockheight",
			Labels: priceTableLabels,
			Value:  float64(host.PriceTable.HostBlockHeight),
		},
		{
			Name:   "renterd_host_pricetable_updatepricetablecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.UpdatePriceTableCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_accountbalancecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.AccountBalanceCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_fundaccountcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.FundAccountCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_latestrevisioncost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.LatestRevisionCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_subscriptionmemorycost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.SubscriptionMemoryCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_subscriptionnotificationcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.SubscriptionNotificationCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_initbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.InitBaseCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_memorytimecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.MemoryTimeCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_downloadbandwidthcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.DownloadBandwidthCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_uploadbandwidthcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.UploadBandwidthCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_dropsectorsbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.DropSectorsBaseCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_dropsectorsunitcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.DropSectorsUnitCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_hassectorbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.HasSectorBaseCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_readbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.ReadBaseCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_readlengthcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.ReadLengthCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_renewcontractcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.RenewContractCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_revisionbasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.RevisionBaseCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_swapsectorcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.SwapSectorBaseCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_writebasecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.WriteBaseCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_writelengthcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.WriteLengthCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_writestorecost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.WriteStoreCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_txnfeeminrecommended",
			Labels: priceTableLabels,
			Value:  host.PriceTable.TxnFeeMinRecommended.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_txnfeemaxrecommended",
			Labels: priceTableLabels,
			Value:  host.PriceTable.TxnFeeMaxRecommended.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_contractprice",
			Labels: priceTableLabels,
			Value:  host.PriceTable.ContractPrice.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_collateralcost",
			Labels: priceTableLabels,
			Value:  host.PriceTable.CollateralCost.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_maxcollateral",
			Labels: priceTableLabels,
			Value:  host.PriceTable.MaxCollateral.Siacoins(),
		},
		{
			Name:   "renterd_host_pricetable_maxduration",
			Labels: priceTableLabels,
			Value:  float64(host.PriceTable.MaxDuration),
		},
		{
			Name:   "renterd_host_pricetable_windowsize",
			Labels: priceTableLabels,
			Value:  float64(host.PriceTable.WindowSize),
		},

		// settings
		{
			Name:   "renterd_host_settings_acceptingcontracts",
			Labels: settingsLabels,
			Value:  boolToFloat(host.Settings.AcceptingContracts),
		},
		{
			Name:   "renterd_host_settings_baserpcprice",
			Labels: settingsLabels,
			Value:  host.Settings.BaseRPCPrice.Siacoins(),
		},
		{
			Name:   "renterd_host_settings_collateral",
			Labels: settingsLabels,
			Value:  host.Settings.Collateral.Siacoins(),
		},
		{
			Name:   "renterd_host_settings_contractprice",
			Labels: settingsLabels,
			Value:  host.Settings.ContractPrice.Siacoins(),
		},
		{
			Name:   "renterd_host_settings_downloadbandwidthprice",
			Labels: settingsLabels,
			Value:  host.Settings.DownloadBandwidthPrice.Siacoins(),
		},
		{
			Name:   "renterd_host_settings_ephemeralaccountexpiry",
			Labels: settingsLabels,
			Value:  float64(host.Settings.EphemeralAccountExpiry.Milliseconds()),
		},
		{
			Name:   "renterd_host_settings_maxcollateral",
			Labels: settingsLabels,
			Value:  host.Settings.MaxCollateral.Siacoins(),
		},
		{
			Name:   "renterd_host_settings_maxdownloadbatchsize",
			Labels: settingsLabels,
			Value:  float64(host.Settings.MaxDownloadBatchSize),
		},
		{
			Name:   "renterd_host_settings_maxduration",
			Labels: settingsLabels,
			Value:  float64(host.Settings.MaxDuration),
		},
		{
			Name:   "renterd_host_settings_maxephemeralaccountbalance",
			Labels: settingsLabels,
			Value:  host.Settings.MaxEphemeralAccountBalance.Siacoins(),
		},
		{
			Name:   "renterd_host_settings_maxrevisebatchsize",
			Labels: settingsLabels,
			Value:  float64(host.Settings.MaxReviseBatchSize),
		},
		{
			Name:   "renterd_host_settings_remainingstorage",
			Labels: settingsLabels,
			Value:  float64(host.Settings.RemainingStorage),
		},
		{
			Name:   "renterd_host_settings_revisionnumber",
			Labels: settingsLabels,
			Value:  float64(host.Settings.RevisionNumber),
		},
		{
			Name:   "renterd_host_settings_sectoraccessprice",
			Labels: settingsLabels,
			Value:  host.Settings.SectorAccessPrice.Siacoins(),
		},
		{
			Name:   "renterd_host_settings_sectorsize",
			Labels: settingsLabels,
			Value:  float64(host.Settings.SectorSize),
		},
		{
			Name:   "renterd_host_settings_storageprice",
			Labels: settingsLabels,
			Value:  host.Settings.StoragePrice.Siacoins(),
		},
		{
			Name:   "renterd_host_settings_totalstorage",
			Labels: settingsLabels,
			Value:  float64(host.Settings.TotalStorage),
		},
		{
			Name:   "renterd_host_settings_uploadbandwidthprice",
			Labels: settingsLabels,
			Value:  host.Settings.UploadBandwidthPrice.Siacoins(),
		},
		{
			Name:   "renterd_host_settings_windowsize",
			Labels: settingsLabels,
			Value:  float64(host.Settings.WindowSize),
		},

		// interactions
		{
			Name:   "renterd_host_interactions_totalscans",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.TotalScans),
		},
		{
			Name:   "renterd_host_interactions_lastscansuccess",
			Labels: netAddressLabel,
			Value:  boolToFloat(host.Interactions.LastScanSuccess),
		},
		{
			Name:   "renterd_host_interactions_lostsectors",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.LostSectors),
		},
		{
			Name:   "renterd_host_interactions_secondtolastscansuccess",
			Labels: netAddressLabel,
			Value:  boolToFloat(host.Interactions.SecondToLastScanSuccess),
		},
		{
			Name:   "renterd_host_interactions_uptime",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.Uptime.Milliseconds()),
		},
		{
			Name:   "renterd_host_interactions_downtime",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.Downtime.Milliseconds()),
		},
		{
			Name:   "renterd_host_interactions_successfulinteractions",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.SuccessfulInteractions),
		},
		{
			Name:   "renterd_host_interactions_failedinteractions",
			Labels: netAddressLabel,
			Value:  float64(host.Interactions.FailedInteractions),
		}}
}

func (host HostAddress) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name: "renterd_hosts_scanning",
			Labels: map[string]any{
				"net_address": host.NetAddress,
				"host_key":    host.PublicKey.String(),
			},
			Value: 1,
		}}
}

func (c ContractMetadata) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name: "renterd_contract",
			Labels: map[string]any{
				"host_ip":        c.HostIP,
				"state":          c.State,
				"host_key":       c.HostKey.String(),
				"siamux_addr":    c.SiamuxAddr,
				"contract_price": c.ContractPrice.Siacoins(),
			},
			Value: c.TotalCost.Siacoins(),
		}}
}

func (c ContractsPrunableDataResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "renterd_prunable_contracts_total",
			Value: float64(c.TotalPrunable),
		},
		{
			Name:  "renterd_prunable_contracts_size",
			Value: float64(c.TotalSize),
		}}
}

func formatSettingsMetricName(gp GougingParams, name string) (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  fmt.Sprintf("renterd_%s_consensusstate_synced", name),
		Value: boolToFloat(gp.ConsensusState.Synced),
	})
	if name == "gouging" { // upload setting already has renterd_upload_currentheight
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_gouging_currentheight",
			Labels: map[string]any{
				"last_block_time": gp.ConsensusState.LastBlockTime.String(),
			},
			Value: float64(gp.ConsensusState.BlockHeight),
		})
	}
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

func (gp GougingParams) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = formatSettingsMetricName(gp, "gouging")
	return
}

func (up UploadParams) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = formatSettingsMetricName(up.GougingParams, "upload")
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_upload_currentheight",
		Labels: map[string]any{
			"contract_set":   up.ContractSet,
			"upload_packing": boolToFloat(up.UploadPacking),
		},
		Value: float64(up.CurrentHeight),
	})
	return
}

// SlabBuffersResp represents multiple `SlabBuffer`s.  Its prometheus encoding
// is a summary of the count and average size of slab buffers.
type SlabBuffersResp []SlabBuffer

func (sb SlabBuffersResp) PrometheusMetric() (metrics []prometheus.Metric) {
	totalSize := int64(0)
	for _, buffer := range sb {
		totalSize += buffer.Size
	}
	return []prometheus.Metric{
		{
			Name:  "renterd_slabbuffers_totalsize",
			Value: float64(totalSize),
		},
		{
			Name:  "renterd_slabbuffers_totalslabs",
			Value: float64(len(sb)),
		}}
}

// SlabBuffersResp represents multiple `ObjectMetadata`s.  Its prometheus encoding
// is a summary of the count and average health of slab buffers.
type SearchObjectsResp []ObjectMetadata

func (so SearchObjectsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	unavailableObjs := 0
	avgHealth := float64(0.0)
	for _, obj := range so {
		if obj.Health == 0 {
			unavailableObjs += 1
		}
		avgHealth += obj.Health
	}
	return []prometheus.Metric{
		{
			Name:  "renterd_objects_avghealth",
			Value: float64(avgHealth),
		},
		{
			Name:  "renterd_objects_unavailable",
			Value: float64(unavailableObjs),
		}}
}

// SettingsResp represents the settings response fields not available from
// other endpoints.
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
	return
}

func (sr BusStateResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name: "renterd_state",
			Labels: map[string]any{
				"network":    sr.Network,
				"version":    sr.Version,
				"commit":     sr.Commit,
				"start_time": sr.StartTime.String(),
				"os":         sr.OS,
				"build_time": sr.BuildTime.String(),
			},
			Value: 1,
		}}
}

func (os ObjectsStatsResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "renterd_stats_numobjects",
			Value: float64(os.NumObjects),
		},
		{
			Name:  "renterd_stats_numunfinishedobjects",
			Value: float64(os.NumUnfinishedObjects),
		},
		{
			Name:  "renterd_stats_minhealth",
			Value: os.MinHealth,
		},
		{
			Name:  "renterd_stats_totalobjectsize",
			Value: float64(os.TotalObjectsSize),
		},
		{
			Name:  "renterd_stats_totalunfinishedobjectssize",
			Value: float64(os.TotalUnfinishedObjectsSize),
		},
		{
			Name:  "renterd_stats_totalsectorssize",
			Value: float64(os.TotalSectorsSize),
		},
		{
			Name:  "renterd_stats_totaluploadedsize",
			Value: float64(os.TotalUploadedSize),
		}}
}

func (w WalletResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "renterd_wallet_scanheight",
			Value: float64(w.ScanHeight),
		},
		{
			Name:  "renterd_wallet_spendable",
			Value: w.Spendable.Siacoins(),
		},
		{
			Name:  "renterd_wallet_confirmed",
			Value: w.Confirmed.Siacoins(),
		},
		{
			Name:  "renterd_wallet_unconfirmed",
			Value: w.Unconfirmed.Siacoins(),
		},
		{
			Name:  "renterd_wallet_immature",
			Value: w.Immature.Siacoins(),
		}}
}

// WalletOutputsResp represents multiple `SiacoinElement`s.  Its prometheus
// encoding is a summary of the count and total value of wallet outputs.
type WalletOutputsResp []SiacoinElement

func (utxos WalletOutputsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "renterd_wallet_numoutputs",
			Value: float64(len(utxos)),
		},
		{
			Name: "renterd_wallet_value",
			Value: func() float64 {
				var sum types.Currency
				for _, utxo := range utxos {
					sum = sum.Add(utxo.Value)
				}
				return sum.Siacoins()
			}(),
		},
	}
}

func (txn Transaction) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name: "renterd_wallet_transacaction_pending_inflow",
			Labels: map[string]any{
				"txid": txn.ID.String(),
			},
			Value: txn.Inflow.Siacoins(),
		},
		{
			Name: "renterd_wallet_transaction_pending_outflow",
			Labels: map[string]any{
				"txid": txn.ID.String(),
			},
			Value: txn.Outflow.Siacoins(),
		},

		{
			Name: "renterd_wallet_transaction_pending_minerfee",
			Labels: map[string]any{
				"txid": txn.ID.String(),
			},
			Value: func() float64 {
				if len(txn.Raw.MinerFees) > 0 {
					return txn.Raw.MinerFees[0].Siacoins()
				}
				return 0
			}(),
		}}
}

func (m MemoryResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "renterd_worker_memory_download_available",
			Value: float64(m.Download.Available),
		},
		{
			Name:  "renterd_worker_memory_download_total",
			Value: float64(m.Download.Total),
		},
		{
			Name:  "renterd_worker_memory_upload_available",
			Value: float64(m.Upload.Available),
		},
		{
			Name:  "renterd_worker_memory_upload_total",
			Value: float64(m.Upload.Total),
		}}
}

func (m DownloadStatsResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "renterd_worker_stats_avgdownloadspeedmbps",
			Value: m.AvgDownloadSpeedMBPS,
		},
		{
			Name:  "renterd_worker_stats_avgoverdrivepct_download",
			Value: m.AvgOverdrivePct,
		},
		{
			Name:  "renterd_worker_stats_healthydownloaders",
			Value: float64(m.HealthyDownloaders),
		},
		{
			Name:  "renterd_worker_stats_numdownloaders",
			Value: float64(m.NumDownloaders),
		}}
}

func (m UploadStatsResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "renterd_worker_stats_avgslabuploadspeedmbps",
			Value: m.AvgSlabUploadSpeedMBPS,
		},
		{
			Name:  "renterd_worker_stats_avgoverdrivepct_upload",
			Value: m.AvgOverdrivePct,
		},
		{
			Name:  "renterd_worker_stats_healthyuploaders",
			Value: float64(m.HealthyUploaders),
		},
		{
			Name:  "renterd_worker_stats_numuploaders",
			Value: float64(m.NumUploaders),
		}}
}

// AllowListResp represents multiple `typex.PublicKey`s.  Its prometheus
// encoding is a list of those keys.
type AllowListResp []types.PublicKey

func (a AllowListResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, host := range a {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_allowed_host",
			Labels: map[string]any{
				"host_key": host,
			},
			Value: 1,
		})
	}
	return
}

// BlockListResp represents multiple blocked host net addresses.  Its
// prometheus encoding is a list of those addresses.
type BlockListResp []string

func (b BlockListResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, host := range b {
		metrics = append(metrics, prometheus.Metric{
			Name: "renterd_blocked_host",
			Labels: map[string]any{
				"address": host,
			},
			Value: 1,
		})
	}
	return
}

// SyncerAddrResp represents the address of the renterd syncer.  Its prometheus
// encoding includings a label of the same value.
type SyncerAddrResp string

func (sar SyncerAddrResp) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name: "renterd_syncer_address",
			Labels: map[string]any{
				"address": string(sar),
			},
			Value: 1,
		}}
}

// SyncerPeersResp represents the addresses of the syncers peers.  Its prometheus
// encoding is a list of those addresses.
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

// TxPoolFeeResp represents the average transaction fee.  Its prometheus
// encoding is a float encoded version of the fee.
// We can't just use a type alias or the custom Currency marshaling methods
// won't be used.
type TxPoolFeeResp struct{ types.Currency }

func (t TxPoolFeeResp) ToFloat64() (float64, error) {
	// Convert string to float64
	return strconv.ParseFloat(t.String(), 64)
}

func (t TxPoolFeeResp) PrometheusMetric() (metrics []prometheus.Metric) {
	floatValue, err := t.ToFloat64()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	return []prometheus.Metric{
		{
			Name:  "renterd_tpool_fee",
			Value: floatValue,
		}}
}

// TxPoolTxResp represents a multiple `types.Transaction`s.  Its prometheus
// encoding represents the number of transactions in the slice.
type TxPoolTxResp []types.Transaction

func (tpr TxPoolTxResp) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "renterd_txpool_numtxns",
			Value: float64(len(tpr)),
		}}
}
