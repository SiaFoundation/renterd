package stores

type (
	// dbContractMetric tracks information about a contract's funds.  It is
	// supposed to be reported by a worker every time a contract is revised.
	dbContractMetric struct {
		Model

		FCID fileContractID
		Host publicKey

		RemainingCollateralLo uint64
		RemainingCollateralHi uint64
		RemainingFundsLo      uint64
		RemainingFundsHi      uint64
		RevisionNumber        uint64

		UploadSpendingLo      uint64
		UploadSpendingHi      uint64
		DownloadSpendingLo    uint64
		DownloadSpendingHi    uint64
		FundAccountSpendingLo uint64
		FundAccountSpendingHi uint64
		DeleteSpendingLo      uint64
		DeleteSpendingHi      uint64
		ListSpendingLo        uint64
		ListSpendingHi        uint64
	}

	// dbContractSetMetric tracks information about a specific contract set.
	// Such as the number of contracts it contains. Intended to be reported by
	// the bus every time the set is updated.
	dbContractSetMetric struct {
		Model

		Name      string
		Contracts int
	}

	// dbContractSetChurnMetric contains information about contracts being added
	// to / removed from a contract set. Expected to be reported by the entity
	// updating the set. e.g. the autopilot.
	dbContractSetChurnMetric struct {
		Model

		Name      string
		FCID      fileContractID
		Direction string // "added" or "removed"
		Reason    string
	}

	// dbPerformanceMetric is a generic metric used to track the performance of
	// an action. Such an action could be a ReadSector operation. Expected to be
	// reported by workers.
	dbPerformanceMetric struct {
		Model

		Action   string
		Host     publicKey
		Reporter string
		Duration float64
	}
)

func (dbContractMetric) TableName() string         { return "contracts" }
func (dbContractSetMetric) TableName() string      { return "contract_sets" }
func (dbContractSetChurnMetric) TableName() string { return "contract_sets_churn" }
func (dbPerformanceMetric) TableName() string      { return "performance" }
