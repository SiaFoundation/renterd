package rhp

import (
	"bytes"
	"errors"
	"fmt"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

const (
	// EntryTypeArbitrary is a registry value where all data is arbitrary.
	EntryTypeArbitrary = iota + 1

	// EntryTypePubKey is a registry value where the first 20 bytes of data
	// corresponds to the hash of a host's public key.
	EntryTypePubKey
)

const (
	// MaxValueDataSize is the maximum size of a Value's Data
	// field.
	MaxValueDataSize = 113
)

// A RegistryKey uniquely identifies a value in the host's registry.
type RegistryKey struct {
	PublicKey consensus.PublicKey
	Tweak     consensus.Hash256
}

// A RegistryValue is a value associated with a key and a tweak in a host's
// registry.
type RegistryValue struct {
	Data      []byte
	Revision  uint64
	Type      uint8
	Signature consensus.Signature
}

// A RegistryEntry contains the data stored by a host for each registry value.
type RegistryEntry struct {
	RegistryKey
	RegistryValue
}

// Hash returns the hash of the Value used for signing
// the entry.
func (re *RegistryEntry) Hash() consensus.Hash256 {
	if re.Type != EntryTypePubKey {
		return consensus.Hash256(crypto.HashAll(re.Tweak, re.Data, re.Revision))
	}
	return consensus.Hash256(crypto.HashAll(re.Tweak, re.Data, re.Revision, re.Type))
}

// Work returns the work of a Value.
func (re *RegistryEntry) Work() consensus.Hash256 {
	data := re.Data
	if re.Type == EntryTypePubKey {
		data = re.Data[20:]
	}
	return consensus.Hash256(crypto.HashAll(re.Tweak, data, re.Revision))
}

// RegistryHostID returns the ID hash of the host for primary registry entries.
func RegistryHostID(pk consensus.PublicKey) consensus.Hash256 {
	return consensus.Hash256(crypto.HashObject(types.SiaPublicKey{
		Algorithm: types.SignatureEd25519,
		Key:       pk[:],
	}))
}

// ValidateRegistryEntry validates the fields of a registry entry.
func ValidateRegistryEntry(re RegistryEntry) (err error) {
	switch re.Type {
	case EntryTypeArbitrary:
		break // no extra validation required
	case EntryTypePubKey:
		// pub key entries have the first 20 bytes of the host's pub key hash
		// prefixed to the data.
		if len(re.Data) < 20 {
			return errors.New("expected host public key hash")
		}
	default:
		return fmt.Errorf("invalid registry value type: %d", re.Type)
	}
	if !re.PublicKey.VerifyHash(re.Hash(), re.Signature) {
		return errors.New("invalid signature")
	}
	return nil
}

// ValidateRegistryUpdate validates a registry update against the current entry.
// An updated registry entry must have a greater revision number, more work, or
// be replacing a non-primary registry entry.
func ValidateRegistryUpdate(old, update RegistryEntry, hostID crypto.Hash) error {
	// if the new revision is greater than the current revision, the update is
	// valid.
	if update.Revision > old.Revision {
		return nil
	} else if update.Revision < old.Revision {
		return errors.New("update revision must be greater than current revision")
	}

	// if the revision number is the same, but the work is greater, the update
	// is valid.
	work, oldWork := update.Work(), old.Work()
	if w := bytes.Compare(work[:], oldWork[:]); w > 0 {
		return nil
	} else if w < 0 {
		return errors.New("update must have greater work or greater revision number than current entry")
	}

	// if the update entry is an arbitrary value entry, the update is invalid.
	if update.Type == EntryTypeArbitrary {
		return errors.New("update must be a primary entry or have a greater revision number")
	}

	// if the updated entry is not a primary entry, it is invalid.
	if !bytes.Equal(update.Data[:20], hostID[:20]) {
		return errors.New("update must be a primary entry or have a greater revision number")
	}

	// if the update and current entry are both primary, the update is invalid
	if old.Type == EntryTypePubKey && bytes.Equal(old.Data[:20], hostID[:20]) {
		return errors.New("update revision must be greater than current revision")
	}

	return nil
}
