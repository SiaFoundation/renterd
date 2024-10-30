package sql

import (
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

type Scanner interface {
	Scan(dest ...any) error
}

type ContractRow struct {
	FCID    FileContractID
	HostID  *int64
	HostKey PublicKey

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

	// decorated fields
	ContractSet string
	NetAddress  string
	SiamuxPort  string
}

func (r *ContractRow) Scan(s Scanner) error {
	return s.Scan(
		&r.FCID, &r.HostID, &r.HostKey,
		&r.ArchivalReason, &r.ProofHeight, &r.RenewedFrom, &r.RenewedTo, &r.RevisionHeight, &r.RevisionNumber, &r.Size, &r.StartHeight, &r.State, &r.WindowStart, &r.WindowEnd,
		&r.ContractPrice, &r.InitialRenterFunds,
		&r.DeleteSpending, &r.FundAccountSpending, &r.SectorRootsSpending, &r.UploadSpending,
		&r.ContractSet, &r.NetAddress, &r.SiamuxPort,
	)
}

func (r *ContractRow) ContractMetadata() api.ContractMetadata {
	var sets []string
	if r.ContractSet != "" {
		sets = append(sets, r.ContractSet)
	}

	var siamuxAddr string
	if r.NetAddress != "" && r.SiamuxPort != "" {
		siamuxAddr = rhpv2.HostSettings{
			NetAddress: r.NetAddress,
			SiaMuxPort: r.SiamuxPort,
		}.SiamuxAddr()
	}

	spending := api.ContractSpending{
		Uploads:     types.Currency(r.UploadSpending),
		FundAccount: types.Currency(r.FundAccountSpending),
		Deletions:   types.Currency(r.DeleteSpending),
		SectorRoots: types.Currency(r.SectorRootsSpending),
	}

	return api.ContractMetadata{
		ID:      types.FileContractID(r.FCID),
		HostIP:  r.NetAddress,
		HostKey: types.PublicKey(r.HostKey),

		ContractPrice:      types.Currency(r.ContractPrice),
		InitialRenterFunds: types.Currency(r.InitialRenterFunds),

		ArchivalReason: string(r.ArchivalReason),
		ContractSets:   sets,
		ProofHeight:    r.ProofHeight,
		RenewedFrom:    types.FileContractID(r.RenewedFrom),
		RenewedTo:      types.FileContractID(r.RenewedTo),
		RevisionHeight: r.RevisionHeight,
		RevisionNumber: r.RevisionNumber,
		SiamuxAddr:     siamuxAddr,
		Size:           r.Size,
		Spending:       spending,
		StartHeight:    r.StartHeight,
		State:          r.State.String(),
		WindowStart:    r.WindowStart,
		WindowEnd:      r.WindowEnd,
	}
}
