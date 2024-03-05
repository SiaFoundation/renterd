package test

import (
	"strings"
	"time"
)

type (
	TT interface {
		TestingCommon

		AssertContains(err error, target string)
		AssertIs(err, target error)
		FailAll(vs ...interface{})
		OK(err error)
		OKAll(vs ...interface{})

		// Retry will call 'fn' 'tries' times, waiting 'durationBetweenAttempts'
		// between each attempt, returning 'nil' the first time that 'fn'
		// returns nil. If 'nil' is never returned, then the final error
		// returned by 'fn' is returned.
		Retry(tries int, durationBetweenAttempts time.Duration, fn func() error)
	}

	// TestingCommon is an interface that describes the common methods of
	// testing.T and testing.B ensuring this testutil can be used in both
	// contexts.
	TestingCommon interface {
		Log(args ...any)
		Logf(format string, args ...any)
		Error(args ...any)
		Errorf(format string, args ...any)
		Fatal(args ...any)
		Fatalf(format string, args ...any)
		Skip(args ...any)
		Skipf(format string, args ...any)
		SkipNow()
		Skipped() bool
		Helper()
		Cleanup(f func())
		TempDir() string
		Setenv(key, value string)
	}

	impl struct {
		TestingCommon
	}
)

func NewTT(tc TestingCommon) TT {
	return &impl{TestingCommon: tc}
}

func (t impl) AssertContains(err error, target string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), target) {
		t.Fatalf("err: %v != target: %v", err, target)
	}
}

func (t impl) AssertIs(err, target error) {
	t.Helper()
	t.AssertContains(err, target.Error())
}

func (t impl) FailAll(vs ...interface{}) {
	t.Helper()
	for _, v := range vs {
		if err, ok := v.(error); ok && err == nil {
			t.Fatal("should've failed")
		}
	}
}

func (t impl) OK(err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (t impl) OKAll(vs ...interface{}) {
	t.Helper()
	for _, v := range vs {
		if err, ok := v.(error); ok && err != nil {
			t.Fatal(err)
		}
	}
}

func (t impl) Retry(tries int, durationBetweenAttempts time.Duration, fn func() error) {
	t.Helper()
	t.OK(Retry(tries, durationBetweenAttempts, fn))
}

func Retry(tries int, durationBetweenAttempts time.Duration, fn func() error) error {
	var err error
	for i := 0; i < tries; i++ {
		err = fn()
		if err == nil {
			break
		}
		time.Sleep(durationBetweenAttempts)
	}
	return err
}
