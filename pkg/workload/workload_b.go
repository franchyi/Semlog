package workload

import (
	"encoding/json"
	"fmt"
	"math/rand"
)

type WorkloadB struct {
	DisjointFieldProb float64
}

func NewWorkloadB(disjointFieldProb float64) *WorkloadB {
	if disjointFieldProb < 0 {
		disjointFieldProb = 0
	}
	if disjointFieldProb > 1 {
		disjointFieldProb = 1
	}
	return &WorkloadB{DisjointFieldProb: disjointFieldProb}
}

func (w *WorkloadB) InitValue(key string) json.RawMessage {
	return json.RawMessage(`{"email":"user@example.com","phone":"555-0100","addr":{"city":"SF","zip":"94105"},"prefs":{"theme":"light","lang":"en"},"stats":{"views":0,"likes":0}}`)
}

func (w *WorkloadB) NextOp(region string, key string, rng *rand.Rand) WriteRequest {
	if rng.Float64() < 0.30 {
		path := "/stats/views"
		if rng.Float64() < 0.5 {
			path = "/stats/likes"
		}
		args := map[string]any{"path": path, "delta": 1}
		b, _ := json.Marshal(args)
		return WriteRequest{Key: key, OpType: "INC", Args: b, WriteSet: []string{path}}
	}

	path, value := w.pickPatch(region, rng)
	args := map[string]any{"patches": []map[string]any{{"path": path, "value": value}}}
	b, _ := json.Marshal(args)
	return WriteRequest{Key: key, OpType: "PATCH", Args: b, WriteSet: []string{path}}
}

func (w *WorkloadB) pickPatch(region string, rng *rand.Rand) (string, any) {
	groupA := []string{"/email", "/prefs/theme", "/addr/city"}
	groupB := []string{"/phone", "/prefs/lang", "/addr/zip"}
	all := append(append([]string{}, groupA...), groupB...)

	if rng.Float64() < w.DisjointFieldProb {
		if region == "A" {
			p := groupA[rng.Intn(len(groupA))]
			return p, randomValueForPath(p, rng)
		}
		p := groupB[rng.Intn(len(groupB))]
		return p, randomValueForPath(p, rng)
	}
	p := all[rng.Intn(len(all))]
	return p, randomValueForPath(p, rng)
}

func randomValueForPath(path string, rng *rand.Rand) any {
	switch path {
	case "/email":
		return fmt.Sprintf("user%d@example.com", rng.Intn(100000))
	case "/phone":
		return fmt.Sprintf("555-%04d", rng.Intn(10000))
	case "/addr/city":
		cities := []string{"SF", "NYC", "SEA", "AUS", "BOS"}
		return cities[rng.Intn(len(cities))]
	case "/addr/zip":
		return fmt.Sprintf("%05d", 10000+rng.Intn(89999))
	case "/prefs/theme":
		themes := []string{"light", "dark", "solarized"}
		return themes[rng.Intn(len(themes))]
	case "/prefs/lang":
		langs := []string{"en", "es", "fr", "de"}
		return langs[rng.Intn(len(langs))]
	default:
		return "x"
	}
}

func (w *WorkloadB) CheckInvariants(value json.RawMessage) bool {
	return len(value) > 0
}
