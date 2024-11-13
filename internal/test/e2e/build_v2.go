//go:build v2

package e2e

// configuration for post-v2-hardfork tests
const (
	HardforkV2AllowHeight   = 2
	HardforkV2RequireHeight = 30000 // TODO: change this to 3 once all the code is updated to use v2 above the allow height
)
