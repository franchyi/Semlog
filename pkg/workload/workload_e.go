package workload

import (
	"encoding/json"
	"math/rand"
)

type WorkloadE struct{}

func (w *WorkloadE) InitValue(key string) json.RawMessage {
	return json.RawMessage(`{"value":0,"derived":0}`)
}

func (w *WorkloadE) NextOp(region string, key string, rng *rand.Rand) WriteRequest {
	path := "/value"
	if rng.Float64() < 0.5 {
		path = "/derived"
	}
	args, _ := json.Marshal(map[string]any{"path": path, "delta": 1})
	return WriteRequest{Key: key, OpType: "INC", Args: args, WriteSet: []string{path}}
}

func (w *WorkloadE) CheckInvariants(value json.RawMessage) bool { return len(value) > 0 }
