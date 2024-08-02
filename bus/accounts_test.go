package bus

import (
	"context"
	"testing"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestAccountLocking(t *testing.T) {
	accounts := newAccounts(nil, zap.NewNop().Sugar())

	var accountID rhpv3.Account
	frand.Read(accountID[:])
	var hk types.PublicKey
	frand.Read(hk[:])

	// Lock account non-exclusively a few times.
	var lockIDs []uint64
	for i := 0; i < 10; i++ {
		acc, lockID := accounts.LockAccount(context.Background(), accountID, hk, false, 30*time.Second)
		if lockID == 0 {
			t.Fatal("invalid lock id")
		}
		if acc.ID != accountID {
			t.Fatal("wrong id")
		}
		lockIDs = append(lockIDs, lockID)
	}

	// Unlock them again.
	for _, lockID := range lockIDs {
		err := accounts.UnlockAccount(accountID, lockID)
		if err != nil {
			t.Fatal("failed to unlock", err)
		}
	}

	// Acquire exclusive lock.
	_, exclusiveLockID := accounts.LockAccount(context.Background(), accountID, hk, true, 30*time.Second)

	// Try acquiring a non-exclusive one.
	var sharedLockID uint64
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, sharedLockID = accounts.LockAccount(context.Background(), accountID, hk, true, 30*time.Second)
	}()

	// Wait some time to confirm it's not possible.
	select {
	case <-done:
		t.Fatal("lock was acquired even though exclusive one was held")
	case <-time.After(100 * time.Millisecond):
	}

	// Unlock exclusive one.
	if err := accounts.UnlockAccount(accountID, exclusiveLockID); err != nil {
		t.Fatal(err)
	}
	// Doing so again should fail.
	if err := accounts.UnlockAccount(accountID, exclusiveLockID); err == nil {
		t.Fatal("should fail")
	}

	// Other lock should be acquired now.
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("other lock wasn't acquired")
	case <-done:
	}

	// Unlock the other lock too.
	if err := accounts.UnlockAccount(accountID, sharedLockID); err != nil {
		t.Fatal(err)
	}

	// Locks should be empty since they clean up after themselves.
	acc := accounts.account(accountID, hk)
	if len(acc.locks) != 0 {
		t.Fatal("should not have any locks", len(acc.locks))
	}
}
