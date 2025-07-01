package sql

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

type Scanner interface {
	Scan(dest ...any) error
}

type ContractRow struct {
	FCID    FileContractID
	HostID  *int64
	HostKey PublicKey
	V2      bool

	// state fields
	ArchivalReason NullableString
	ProofHeight    uint64
	RenewedFrom    FileContractID
	RenewedTo      FileContractID
	RevisionHeight uint64
	RevisionNumber uint64
	Size           uint64
	StartHeight    uint64
	State          ContractState
	Usability      ContractUsability
	WindowStart    uint64
	WindowEnd      uint64

	// cost fields
	ContractPrice      Currency
	InitialRenterFunds Currency

	// spending fields
	DeleteSpending      Currency
	FundAccountSpending Currency
	SectorRootsSpending Currency
	UploadSpending      Currency
}

func (r *ContractRow) Scan(s Scanner) error {
	return s.Scan(
		&r.FCID, &r.HostID, &r.HostKey,
		&r.ArchivalReason, &r.ProofHeight, &r.RenewedFrom, &r.RenewedTo, &r.RevisionHeight, &r.RevisionNumber, &r.Size, &r.StartHeight, &r.State, &r.Usability, &r.WindowStart, &r.WindowEnd,
		&r.ContractPrice, &r.InitialRenterFunds,
		&r.DeleteSpending, &r.FundAccountSpending, &r.SectorRootsSpending, &r.UploadSpending,
	)
}

func (r *ContractRow) ContractMetadata() api.ContractMetadata {
	spending := api.ContractSpending{
		Uploads:     types.Currency(r.UploadSpending),
		FundAccount: types.Currency(r.FundAccountSpending),
		Deletions:   types.Currency(r.DeleteSpending),
		SectorRoots: types.Currency(r.SectorRootsSpending),
	}

	return api.ContractMetadata{
		ID:      types.FileContractID(r.FCID),
		HostKey: types.PublicKey(r.HostKey),

		ContractPrice:      types.Currency(r.ContractPrice),
		InitialRenterFunds: types.Currency(r.InitialRenterFunds),

		ArchivalReason: string(r.ArchivalReason),
		ProofHeight:    r.ProofHeight,
		RenewedFrom:    types.FileContractID(r.RenewedFrom),
		RenewedTo:      types.FileContractID(r.RenewedTo),
		RevisionHeight: r.RevisionHeight,
		RevisionNumber: r.RevisionNumber,
		Size:           r.Size,
		Spending:       spending,
		StartHeight:    r.StartHeight,
		State:          r.State.String(),
		Usability:      r.Usability.String(),
		WindowStart:    r.WindowStart,
		WindowEnd:      r.WindowEnd,
	}
}
