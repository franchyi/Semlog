package jsonutil

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestCanonicalHashEquivalentObjects(t *testing.T) {
	a := json.RawMessage(`{"b":2,"a":1}`)
	b := json.RawMessage(`{"a":1,"b":2}`)
	ha := hex.EncodeToString(CanonicalHash(a))
	hb := hex.EncodeToString(CanonicalHash(b))
	if ha != hb {
		t.Fatalf("hash mismatch %s %s", ha, hb)
	}
}
