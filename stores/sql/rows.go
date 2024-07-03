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
	FCID        FileContractID
	RenewedFrom FileContractID

	ContractPrice  Currency
	State          ContractState
	TotalCost      Currency
	ProofHeight    uint64
	RevisionHeight uint64
	RevisionNumber uint64
	Size           uint64
	StartHeight    uint64
	WindowStart    uint64
	WindowEnd      uint64

	// spending fields
	UploadSpending      Currency
	DownloadSpending    Currency
	FundAccountSpending Currency
	DeleteSpending      Currency
	ListSpending        Currency

	ContractSet string
	NetAddress  string
	PublicKey   PublicKey
	SiamuxPort  string
}

func (r *ContractRow) Scan(s Scanner) error {
	return s.Scan(&r.FCID, &r.RenewedFrom, &r.ContractPrice, &r.State, &r.TotalCost, &r.ProofHeight,
		&r.RevisionHeight, &r.RevisionNumber, &r.Size, &r.StartHeight, &r.WindowStart, &r.WindowEnd,
		&r.UploadSpending, &r.DownloadSpending, &r.FundAccountSpending, &r.DeleteSpending, &r.ListSpending,
		&r.ContractSet, &r.NetAddress, &r.PublicKey, &r.SiamuxPort)
}

func (r *ContractRow) ContractMetadata() api.ContractMetadata {
	var sets []string
	if r.ContractSet != "" {
		sets = append(sets, r.ContractSet)
	}
	return api.ContractMetadata{
		ContractPrice: types.Currency(r.ContractPrice),
		ID:            types.FileContractID(r.FCID),
		HostIP:        r.NetAddress,
		HostKey:       types.PublicKey(r.PublicKey),
		SiamuxAddr: rhpv2.HostSettings{
			NetAddress: r.NetAddress,
			SiaMuxPort: r.SiamuxPort,
		}.SiamuxAddr(),

		RenewedFrom: types.FileContractID(r.RenewedFrom),
		TotalCost:   types.Currency(r.TotalCost),
		Spending: api.ContractSpending{
			Uploads:     types.Currency(r.UploadSpending),
			Downloads:   types.Currency(r.DownloadSpending),
			FundAccount: types.Currency(r.FundAccountSpending),
			Deletions:   types.Currency(r.DeleteSpending),
			SectorRoots: types.Currency(r.ListSpending),
		},
		ProofHeight:    r.ProofHeight,
		RevisionHeight: r.RevisionHeight,
		RevisionNumber: r.RevisionNumber,
		ContractSets:   sets,
		Size:           r.Size,
		StartHeight:    r.StartHeight,
		State:          r.State.String(),
		WindowStart:    r.WindowStart,
		WindowEnd:      r.WindowEnd,
	}
}
