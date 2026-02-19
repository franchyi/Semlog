package workload

import (
	"encoding/json"
	"math/rand"
)

type WorkloadC struct{}

func (w *WorkloadC) InitValue(key string) json.RawMessage {
	return json.RawMessage(`{"state":"READY","owner":"","attempt":0}`)
}

func (w *WorkloadC) NextOp(region string, key string, rng *rand.Rand) WriteRequest {
	worker := "wA"
	if region == "B" {
		worker = "wB"
	}
	if rng.Float64() < 0.5 {
		args := json.RawMessage(`{"worker_id":"` + worker + `"}`)
		return WriteRequest{Key: key, OpType: "CLAIM", Args: args, WriteSet: []string{"/state", "/owner", "/attempt"}}
	}
	args := json.RawMessage(`{"worker_id":"` + worker + `"}`)
	return WriteRequest{Key: key, OpType: "COMPLETE", Args: args, WriteSet: []string{"/state"}}
}

func (w *WorkloadC) CheckInvariants(value json.RawMessage) bool { return len(value) > 0 }
