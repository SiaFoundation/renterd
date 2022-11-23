package autopilot

import (
	"go.sia.tech/renterd/internal/consensus"
	"golang.org/x/crypto/blake2b"
)

// TODO: deriving the renter key from the host key using the master key only
// works if we persist a hash of the renter's master key in the database and
// compare it on startup, otherwise there's no way of knowing the derived key is
// usuable
//
// TODO: instead of deriving a renter key use a randomly generated salt so we're
// not limited to one key per host
func (ap *Autopilot) deriveRenterKey(hostKey consensus.PublicKey) consensus.PrivateKey {
	seed := blake2b.Sum256(append(ap.masterKey[:], hostKey[:]...))
	pk := consensus.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}
