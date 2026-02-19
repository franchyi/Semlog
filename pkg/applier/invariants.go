package applier

import (
	"encoding/json"
	"strings"
)

func CheckInvariants(key string, value json.RawMessage) bool {
	if strings.HasPrefix(key, "inventory:") {
		var v struct {
			Stock    float64 `json:"stock"`
			Reserved float64 `json:"reserved"`
		}
		if err := json.Unmarshal(value, &v); err != nil {
			return false
		}
		return v.Stock >= 0 && v.Reserved >= 0 && v.Reserved <= v.Stock
	}
	if strings.HasPrefix(key, "task:") {
		var v struct {
			State string `json:"state"`
			Owner string `json:"owner"`
		}
		if err := json.Unmarshal(value, &v); err != nil {
			return false
		}
		switch v.State {
		case "READY", "DONE":
			return true
		case "CLAIMED":
			return v.Owner != ""
		default:
			return false
		}
	}
	return true
}
