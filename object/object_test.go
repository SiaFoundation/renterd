package object

import (
	"bytes"
	"math"
	"testing"

	"lukechampine.com/frand"
)

func TestEncryptionOverflow(t *testing.T) {
	// Create a random key.
	key := GenerateEncryptionKey()
	data := frand.Bytes(3 * 64)
	sr := key.Encrypt(bytes.NewReader(data))

	// Check that the streamreader is initialized correctly.
	if sr.counter != 0 {
		t.Fatalf("expected counter to be 0, got %v", sr.counter)
	}
	if sr.nonce != 0 {
		t.Fatalf("expected nonce to be 0, got %v", sr.nonce)
	}

	// Read 64 bytes.
	b := make([]byte, 64)
	n, err := sr.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(b) {
		t.Fatalf("expected to read 10 bytes, got %v", n)
	}

	// Assert counter was incremented correctly.
	if sr.counter != 64 {
		t.Fatalf("expected counter to be 10, got %v", sr.counter)
	}
	if sr.nonce != 0 {
		t.Fatalf("expected nonce to be 0, got %v", sr.nonce)
	}

	// Assert data matches.
	buf := bytes.NewBuffer(nil)
	written, err := key.Decrypt(buf, 0).Write(b)
	if err != nil {
		t.Fatal(err)
	}
	if written != len(b) {
		t.Fatal("unexpected")
	}
	if !bytes.Equal(buf.Bytes(), data[:64]) {
		t.Fatal("mismatch", buf.Bytes(), data[:10])
	}

	// Read 128 bytes. 64 before the overflow, 64 after.
	b = make([]byte, 128)
	sr.counter = math.MaxUint32*64 - 64
	sr.c.SetCounter(math.MaxUint32 - 1)
	n, err = sr.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(b) {
		t.Fatalf("expected to read 10 bytes, got %v", n)
	}

	// Check that counter and nonce did overflow correctly.
	if sr.counter != 64 {
		t.Fatalf("expected counter to be 10, got %v", sr.counter)
	}
	if sr.nonce != 1 {
		t.Fatalf("expected nonce to be 0, got %v", sr.nonce)
	}

	// Assert data matches.
	buf = bytes.NewBuffer(nil)
	written, err = key.Decrypt(buf, math.MaxUint32*64-64).Write(b)
	if err != nil {
		t.Fatal(err)
	}
	if written != len(b) {
		t.Fatal("unexpected")
	}
	if !bytes.Equal(buf.Bytes(), data[64:]) {
		t.Fatal("mismatch", buf.Bytes(), data[64:])
	}
}
