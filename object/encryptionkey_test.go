package object

import (
	"encoding/json"
	"testing"
)

func TestEncryptionKeyJSON(t *testing.T) {
	b, err := json.Marshal(NoOpKey)
	if err != nil {
		t.Fatal(err)
	} else if string(b) != `"key:0000000000000000000000000000000000000000000000000000000000000000"` {
		t.Fatal("unexpected JSON:", string(b))
	}
}
