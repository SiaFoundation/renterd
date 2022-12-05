package testing

import (
	"math/big"

	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// TODO: This is quite the hack but until we have a better solution
func init() {
	modules.SectorSize = modules.SectorSizeTesting

	types.TaxHardforkHeight = types.BlockHeight(10)

	// 'testing' settings are for automatic testing, and create much faster
	// environments than a human can interact with.
	types.ASICHardforkHeight = 5
	types.ASICHardforkTotalTarget = types.Target{255, 255}
	types.ASICHardforkTotalTime = 10e3

	types.FoundationHardforkHeight = 50
	types.FoundationSubsidyFrequency = 5

	initialFoundationUnlockConditions, _ := types.GenerateDeterministicMultisig(2, 3, types.InitialFoundationTestingSalt)
	initialFoundationFailsafeUnlockConditions, _ := types.GenerateDeterministicMultisig(3, 5, types.InitialFoundationFailsafeTestingSalt)
	types.InitialFoundationUnlockHash = initialFoundationUnlockConditions.UnlockHash()
	types.InitialFoundationFailsafeUnlockHash = initialFoundationFailsafeUnlockConditions.UnlockHash()

	types.BlockFrequency = 1 // As fast as possible
	types.MaturityDelay = 3
	types.GenesisTimestamp = types.CurrentTimestamp() - 1e6
	types.RootTarget = types.Target{128} // Takes an expected 2 hashes; very fast for testing but still probes 'bad hash' code.

	// A restrictive difficulty clamp prevents the difficulty from climbing
	// during testing, as the resolution on the difficulty adjustment is
	// only 1 second and testing mining should be happening substantially
	// faster than that.
	types.TargetWindow = 200
	types.MaxTargetAdjustmentUp = big.NewRat(10001, 10000)
	types.MaxTargetAdjustmentDown = big.NewRat(9999, 10000)
	types.FutureThreshold = 3        // 3 seconds
	types.ExtremeFutureThreshold = 6 // 6 seconds

	types.MinimumCoinbase = 299990 // Minimum coinbase is hit after 10 blocks to make testing minimum-coinbase code easier.

	// Do not let the difficulty change rapidly - blocks will be getting
	// mined far faster than the difficulty can adjust to.
	types.OakHardforkBlock = 20
	types.OakHardforkFixBlock = 23
	types.OakDecayNum = 9999
	types.OakDecayDenom = 10e3
	types.OakMaxBlockShift = 3
	types.OakMaxRise = big.NewRat(10001, 10e3)
	types.OakMaxDrop = big.NewRat(10e3, 10001)

	// Populate the void address with 1 billion siacoins in the genesis block.
	types.GenesisSiacoinAllocation = []types.SiacoinOutput{
		{
			Value:      types.NewCurrency64(1000000000).Mul(types.SiacoinPrecision),
			UnlockHash: types.UnlockHash{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}

	types.GenesisSiafundAllocation = []types.SiafundOutput{
		{
			Value:      types.NewCurrency64(2000),
			UnlockHash: types.UnlockHash{214, 166, 197, 164, 29, 201, 53, 236, 106, 239, 10, 158, 127, 131, 20, 138, 63, 221, 230, 16, 98, 247, 32, 77, 210, 68, 116, 12, 241, 89, 27, 223},
		},
		{
			Value:      types.NewCurrency64(7000),
			UnlockHash: types.UnlockHash{209, 246, 228, 60, 248, 78, 242, 110, 9, 8, 227, 248, 225, 216, 163, 52, 142, 93, 47, 176, 103, 41, 137, 80, 212, 8, 132, 58, 241, 189, 2, 17},
		},
		{
			Value:      types.NewCurrency64(1000),
			UnlockHash: types.UnlockConditions{}.UnlockHash(),
		},
	}

	// Create the genesis block.
	types.GenesisBlock = types.Block{
		Timestamp: types.GenesisTimestamp,
		Transactions: []types.Transaction{
			{SiafundOutputs: types.GenesisSiafundAllocation},
		},
	}
	// Calculate the genesis ID.
	types.GenesisID = types.GenesisBlock.ID()
}
