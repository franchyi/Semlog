package workload

import (
	"encoding/json"
	"math/rand"
)

type WorkloadD struct{}

func (w *WorkloadD) InitValue(key string) json.RawMessage {
	return json.RawMessage(`{"stock":100,"reserved":0}`)
}

func (w *WorkloadD) NextOp(region string, key string, rng *rand.Rand) WriteRequest {
	if rng.Float64() < 0.7 {
		n := 1 + rng.Intn(3)
		args, _ := json.Marshal(map[string]any{"n": n})
		return WriteRequest{Key: key, OpType: "RESERVE", Args: args, WriteSet: []string{"/reserved"}}
	}
	n := 1 + rng.Intn(2)
	args, _ := json.Marshal(map[string]any{"n": n})
	return WriteRequest{Key: key, OpType: "CANCEL", Args: args, WriteSet: []string{"/reserved"}}
}

func (w *WorkloadD) CheckInvariants(value json.RawMessage) bool { return len(value) > 0 }
