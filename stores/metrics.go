package stores

type (
	// dbContractMetric tracks information about a contract's funds.  It is
	// supposed to be reported by a worker every time a contract is revised.
	dbContractMetric struct {
		Model

		FCID fileContractID `gorm:"index;NOT NULL"`
		Host publicKey      `gorm:"index;NOT NULL"`

		RemainingCollateralLo uint64 `gorm:"index;NOT NULL"`
		RemainingCollateralHi uint64 `gorm:"index;NOT NULL"`
		RemainingFundsLo      uint64 `gorm:"index;NOT NULL"`
		RemainingFundsHi      uint64 `gorm:"index;NOT NULL"`
		RevisionNumber        uint64 `gorm:"index;NOT NULL"`

		UploadSpendingLo      uint64 `gorm:"index;NOT NULL"`
		UploadSpendingHi      uint64 `gorm:"index;NOT NULL"`
		DownloadSpendingLo    uint64 `gorm:"index;NOT NULL"`
		DownloadSpendingHi    uint64 `gorm:"index;NOT NULL"`
		FundAccountSpendingLo uint64 `gorm:"index;NOT NULL"`
		FundAccountSpendingHi uint64 `gorm:"index;NOT NULL"`
		DeleteSpendingLo      uint64 `gorm:"index;NOT NULL"`
		DeleteSpendingHi      uint64 `gorm:"index;NOT NULL"`
		ListSpendingLo        uint64 `gorm:"index;NOT NULL"`
		ListSpendingHi        uint64 `gorm:"index;NOT NULL"`
	}

	// dbContractSetMetric tracks information about a specific contract set.
	// Such as the number of contracts it contains. Intended to be reported by
	// the bus every time the set is updated.
	dbContractSetMetric struct {
		Model

		Name      string `gorm:"index;NOT NULL"`
		Contracts int    `gorm:"index;NOT NULL"`
	}

	// dbContractSetChurnMetric contains information about contracts being added
	// to / removed from a contract set. Expected to be reported by the entity
	// updating the set. e.g. the autopilot.
	dbContractSetChurnMetric struct {
		Model

		Name      string         `gorm:"index;NOT NULL"`
		FCID      fileContractID `gorm:"index;NOT NULL"`
		Direction string         `gorm:"index;NOT NULL"` // "added" or "removed"
		Reason    string         `gorm:"index;NOT NULL"`
	}

	// dbPerformanceMetric is a generic metric used to track the performance of
	// an action. Such an action could be a ReadSector operation. Expected to be
	// reported by workers.
	dbPerformanceMetric struct {
		Model

		Action   string    `gorm:"index;NOT NULL"`
		Host     publicKey `gorm:"index;NOT NULL"`
		Reporter string    `gorm:"index;NOT NULL"`
		Duration float64   `gorm:"index;NOT NULL"`
	}
)

func (dbContractMetric) TableName() string         { return "contracts" }
func (dbContractSetMetric) TableName() string      { return "contract_sets" }
func (dbContractSetChurnMetric) TableName() string { return "contract_sets_churn" }
func (dbPerformanceMetric) TableName() string      { return "performance" }
