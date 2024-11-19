package sql

import (
	"errors"
	"strings"

	"go.sia.tech/renterd/api"
)

var (
	ErrInvalidContractState     = errors.New("invalid contract state")
	ErrInvalidContractUsability = errors.New("invalid contract usability")
)

type ContractState uint8

const (
	contractStateInvalid ContractState = iota
	contractStatePending
	contractStateActive
	contractStateComplete
	contractStateFailed
)

func ContractStateFromString(state string) ContractState {
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

func (s *ContractState) LoadString(state string) error {
	switch strings.ToLower(state) {
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
		return ErrInvalidContractState
	}
	return nil
}

func (s ContractState) String() string {
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

type ContractUsability uint8

const (
	contractUsabilityInvalid ContractUsability = iota
	contractUsabilityBad
	contractUsabilityGood
)

func (s *ContractUsability) LoadString(usability string) error {
	switch strings.ToLower(usability) {
	case api.ContractUsabilityBad:
		*s = contractUsabilityBad
	case api.ContractUsabilityGood:
		*s = contractUsabilityGood
	default:
		*s = contractUsabilityInvalid
		return ErrInvalidContractUsability
	}
	return nil
}

func (s ContractUsability) String() string {
	switch s {
	case contractUsabilityBad:
		return api.ContractUsabilityBad
	case contractUsabilityGood:
		return api.ContractUsabilityGood
	default:
		return "invalid"
	}
}
