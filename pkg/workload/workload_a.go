package workload

import (
	"encoding/json"
	"math/rand"
)

type WorkloadA struct{}

func (w *WorkloadA) InitValue(key string) json.RawMessage {
	return json.RawMessage(`{"f0":0,"f1":0,"f2":0,"f3":0,"f4":0,"f5":0,"f6":0,"f7":0,"stats":{"views":0,"likes":0},"ver":0}`)
}

func (w *WorkloadA) NextOp(region string, key string, rng *rand.Rand) WriteRequest {
	b := NewWorkloadB(0.7)
	return b.NextOp(region, key, rng)
}

func (w *WorkloadA) CheckInvariants(value json.RawMessage) bool { return len(value) > 0 }
