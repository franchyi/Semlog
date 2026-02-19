package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/yourorg/redpanda-mm/pkg/workload"
)

func main() {
	var (
		workloadID        = flag.String("workload", "B", "Workload: A|B|C|D|E")
		duration          = flag.Duration("duration", 30*time.Second, "Test duration")
		opsPerSec         = flag.Int("ops-per-sec", 100, "Per region ops/sec")
		keyspaceSize      = flag.Int("keyspace-size", 1000, "Number of keys")
		zipfTheta         = flag.Float64("zipf-theta", 0.8, "Zipf skew")
		sharedHotFraction = flag.Float64("shared-hot-fraction", 0.05, "Shared hot keys fraction")
		disjointFieldProb = flag.Float64("disjoint-field-prob", 0.8, "PATCH disjoint probability")
		ingestAURL        = flag.String("ingest-a-url", "http://localhost:8081", "Ingest A URL")
		ingestBURL        = flag.String("ingest-b-url", "http://localhost:8082", "Ingest B URL")
		applierAURL       = flag.String("applier-a-url", "http://localhost:8091", "Applier A URL")
		applierBURL       = flag.String("applier-b-url", "http://localhost:8092", "Applier B URL")
	)
	flag.Parse()

	model := selectWorkload(*workloadID, *disjointFieldProb)
	cfg := workload.WorkloadConfig{
		KeyspaceSize:      *keyspaceSize,
		ZipfTheta:         *zipfTheta,
		SharedHotFraction: *sharedHotFraction,
		DisjointFieldProb: *disjointFieldProb,
		OpsPerSecond:      *opsPerSec,
		DurationSec:       int(duration.Seconds()),
	}

	h := &workload.Harness{
		Config:      cfg,
		Model:       model,
		IngestAURL:  *ingestAURL,
		IngestBURL:  *ingestBURL,
		ApplierAURL: *applierAURL,
		ApplierBURL: *applierBURL,
	}

	result, err := h.Run(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	b, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(b))
}

func selectWorkload(id string, disjointFieldProb float64) workload.WorkloadModel {
	switch id {
	case "A":
		return &workload.WorkloadA{}
	case "C":
		return &workload.WorkloadC{}
	case "D":
		return &workload.WorkloadD{}
	case "E":
		return &workload.WorkloadE{}
	case "B":
		fallthrough
	default:
		return workload.NewWorkloadB(disjointFieldProb)
	}
}
