package applier

import (
	"encoding/json"
	"testing"
)

func TestInventoryInvariants(t *testing.T) {
	if !CheckInvariants("inventory:sku1", json.RawMessage(`{"stock":10,"reserved":4}`)) {
		t.Fatal("expected valid")
	}
	if CheckInvariants("inventory:sku1", json.RawMessage(`{"stock":4,"reserved":10}`)) {
		t.Fatal("expected invalid")
	}
}

func TestTaskInvariants(t *testing.T) {
	if !CheckInvariants("task:k1", json.RawMessage(`{"state":"READY"}`)) {
		t.Fatal("ready should be valid")
	}
	if CheckInvariants("task:k1", json.RawMessage(`{"state":"CLAIMED","owner":""}`)) {
		t.Fatal("claimed without owner should be invalid")
	}
}
