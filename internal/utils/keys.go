package utils

import (
	"fmt"

	"go.sia.tech/core/types"
	"golang.org/x/crypto/blake2b"
)

type (
	MasterKey   [32]byte
	AccountsKey types.PrivateKey
)

// DeriveAccountsKey derives an accounts key from a masterkey which is used
// to derive individual account keys from.
func (key *MasterKey) DeriveAccountsKey(workerID string) AccountsKey {
	keyPath := fmt.Sprintf("accounts/%s", workerID)
	return AccountsKey(key.deriveSubKey(keyPath))
}

// DeriveContractKey derives a contract key from a masterkey which is used to
// form, renew and revise contracts.
func (key *MasterKey) DeriveContractKey(hostKey types.PublicKey) types.PrivateKey {
	seed := blake2b.Sum256(append(key.deriveSubKey("renterkey"), hostKey[:]...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// deriveSubKey can be used to derive a sub-masterkey from the worker's
// masterkey to use for a specific purpose. Such as deriving more keys for
// ephemeral accounts.
func (key *MasterKey) deriveSubKey(purpose string) types.PrivateKey {
	seed := blake2b.Sum256(append(key[:], []byte(purpose)...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// DeriveAccountKey derives an account plus key for a given host and worker.
// Each worker has its own account for a given host. That makes concurrency
// around keeping track of an accounts balance and refilling it a lot easier in
// a multi-worker setup.
func (key *AccountsKey) DeriveAccountKey(hk types.PublicKey) types.PrivateKey {
	index := byte(0) // not used yet but can be used to derive more than 1 account per host

	// Append the host for which to create it and the index to the
	// corresponding sub-key.
	subKey := *key
	data := make([]byte, 0, len(subKey)+len(hk)+1)
	data = append(data, subKey[:]...)
	data = append(data, hk[:]...)
	data = append(data, index)

	seed := types.HashBytes(data)
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}
