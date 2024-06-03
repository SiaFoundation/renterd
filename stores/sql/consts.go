package sql

import (
	"strings"

	"go.sia.tech/renterd/api"
)

type ContractStateEnum uint8

const (
	contractStateInvalid ContractStateEnum = iota
	contractStatePending
	contractStateActive
	contractStateComplete
	contractStateFailed
)

func ContractStateFromString(state string) ContractStateEnum {
	switch strings.ToLower(state) {
	case api.ContractStateInvalid:
		return contractStateInvalid
	case api.ContractStatePending:
		return contractStatePending
	case api.ContractStateActive:
		return contractStateActive
	case api.ContractStateComplete:
		return contractStateComplete
	case api.ContractStateFailed:
		return contractStateFailed
	default:
		return contractStateInvalid
	}
}

func (s *ContractStateEnum) LoadString(state string) error {
	switch strings.ToLower(state) {
	case api.ContractStateInvalid:
		*s = contractStateInvalid
	case api.ContractStatePending:
		*s = contractStatePending
	case api.ContractStateActive:
		*s = contractStateActive
	case api.ContractStateComplete:
		*s = contractStateComplete
	case api.ContractStateFailed:
		*s = contractStateFailed
	default:
		*s = contractStateInvalid
	}
	return nil
}

func (s ContractStateEnum) String() string {
	switch s {
	case contractStateInvalid:
		return api.ContractStateInvalid
	case contractStatePending:
		return api.ContractStatePending
	case contractStateActive:
		return api.ContractStateActive
	case contractStateComplete:
		return api.ContractStateComplete
	case contractStateFailed:
		return api.ContractStateFailed
	default:
		return api.ContractStateUnknown
	}
}
