package engine

import (
	"encoding/json"
	"testing"
)

func TestDiffSimple(t *testing.T) {
	before := json.RawMessage(`{"email":"old@x.com","phone":"555","stats":{"views":10}}`)
	after := json.RawMessage(`{"email":"new@x.com","phone":"555","stats":{"views":15}}`)
	patch := Diff(before, after)
	if string(patch) != `{"email":"new@x.com","stats":{"views":15}}` && string(patch) != `{"stats":{"views":15},"email":"new@x.com"}` {
		t.Fatalf("unexpected patch: %s", patch)
	}
}
