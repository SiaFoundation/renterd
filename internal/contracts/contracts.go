package contracts

import "go.sia.tech/core/types"

type V2BroadcastElement struct {
	Basis                 types.ChainIndex
	V2FileContractElement types.V2FileContractElement
}
