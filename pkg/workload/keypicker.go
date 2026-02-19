package workload

import (
	"fmt"
	"math/rand"
)

type KeyPicker struct {
	keyspace      int
	sharedHot     int
	zipf          *rand.Zipf
	rng           *rand.Rand
	exclusiveSpan int
}

func NewKeyPicker(keyspace int, zipfTheta float64, sharedHotFraction float64, seed int64) *KeyPicker {
	if keyspace < 1 {
		keyspace = 1
	}
	if sharedHotFraction < 0 {
		sharedHotFraction = 0
	}
	if sharedHotFraction > 1 {
		sharedHotFraction = 1
	}
	r := rand.New(rand.NewSource(seed))
	hot := int(float64(keyspace) * sharedHotFraction)
	if hot > keyspace {
		hot = keyspace
	}
	exclusive := (keyspace - hot) / 2
	var z *rand.Zipf
	if zipfTheta > 0 {
		s := 1.0 + zipfTheta
		if s <= 1.0 {
			s = 1.0001
		}
		z = rand.NewZipf(r, s, 1, uint64(max(1, keyspace-1)))
	}
	return &KeyPicker{
		keyspace:      keyspace,
		sharedHot:     hot,
		zipf:          z,
		rng:           r,
		exclusiveSpan: exclusive,
	}
}

func (k *KeyPicker) Pick(region string) string {
	if k.keyspace == 1 {
		return "key:0"
	}
	useShared := k.sharedHot > 0 && k.rng.Float64() < 0.5
	if useShared {
		idx := k.sampleInRange(0, k.sharedHot)
		return fmt.Sprintf("key:%d", idx)
	}
	start, end := k.regionRange(region)
	if end <= start {
		idx := int(k.zipf.Uint64() % uint64(k.keyspace))
		return fmt.Sprintf("key:%d", idx)
	}
	idx := k.sampleInRange(start, end)
	return fmt.Sprintf("key:%d", idx)
}

func (k *KeyPicker) sampleInRange(start, end int) int {
	if end-start <= 1 {
		return start
	}
	n := end - start
	if k.zipf == nil {
		return start + k.rng.Intn(n)
	}
	for {
		v := int(k.zipf.Uint64() % uint64(n))
		if v >= 0 && v < n {
			return start + v
		}
	}
}

func (k *KeyPicker) regionRange(region string) (int, int) {
	if region == "A" {
		return k.sharedHot, k.sharedHot + k.exclusiveSpan
	}
	if region == "B" {
		return k.sharedHot + k.exclusiveSpan, k.keyspace
	}
	return 0, k.keyspace
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
