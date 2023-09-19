package object

import (
	"bytes"
	"io"
	"math"
	"testing"

	"lukechampine.com/frand"
)

func TestEncryptionOffset(t *testing.T) {
	key := GenerateEncryptionKey()

	encrypt := func(offset uint64, plainText []byte) []byte {
		t.Helper()
		sr, err := key.Encrypt(bytes.NewReader(plainText), offset)
		if err != nil {
			t.Fatal(err)
		}
		ct, err := io.ReadAll(sr)
		if err != nil {
			t.Fatal(err)
		}
		return ct
	}
	decrypt := func(offset uint64, cipherText []byte) []byte {
		pt := bytes.NewBuffer(nil)
		_, err := key.Decrypt(pt, offset).Write(cipherText)
		if err != nil {
			t.Fatal(err)
		}
		return pt.Bytes()
	}

	data := frand.Bytes(640)
	offset := uint64(64)
	if !bytes.Equal(data, decrypt(offset, encrypt(offset, data))) {
		t.Fatal("mismatch")
	} else if bytes.Equal(data, decrypt(offset, encrypt(128, data))) {
		t.Fatal("expected mismatch")
	}
}

func TestEncryptionOverflow(t *testing.T) {
	// Create a random key.
	key := GenerateEncryptionKey()
	data := frand.Bytes(3 * 64)
	sr, err := key.Encrypt(bytes.NewReader(data), 0)
	if err != nil {
		t.Fatal(err)
	}

	// Check that the streamreader is initialized correctly.
	rs := sr.S.(*rekeyStream)
	if rs.counter != 0 {
		t.Fatalf("expected counter to be 0, got %v", rs.counter)
	}
	if rs.nonce != 0 {
		t.Fatalf("expected nonce to be 0, got %v", rs.nonce)
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
	if rs.counter != 64 {
		t.Fatalf("expected counter to be 10, got %v", rs.counter)
	}
	if rs.nonce != 0 {
		t.Fatalf("expected nonce to be 0, got %v", rs.nonce)
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

	// Move the counter 64 bytes before an overflow and read 128 bytes.
	b = make([]byte, 128)
	rs.counter = math.MaxUint32*64 - 64
	rs.c.SetCounter(math.MaxUint32 - 1)
	n, err = sr.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(b) {
		t.Fatalf("expected to read 10 bytes, got %v", n)
	}

	// Check that counter and nonce did overflow correctly.
	if rs.counter != 64 {
		t.Fatalf("expected counter to be 10, got %v", rs.counter)
	}
	if rs.nonce != 1 {
		t.Fatalf("expected nonce to be 0, got %v", rs.nonce)
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
		t.Fatal("mismatch")
	}
}
