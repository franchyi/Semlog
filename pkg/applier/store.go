package applier

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"

	"github.com/yourorg/redpanda-mm/pkg/jsonutil"
)

type Store struct {
	mu     sync.RWMutex
	values map[string]json.RawMessage
}

func NewStore() *Store {
	return &Store{values: map[string]json.RawMessage{}}
}

func (s *Store) Get(key string) (json.RawMessage, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.values[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (s *Store) Set(key string, value json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value == nil {
		delete(s.values, key)
		return
	}
	v := make([]byte, len(value))
	copy(v, value)
	s.values[key] = v
}

func (s *Store) Snapshot() map[string]json.RawMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]json.RawMessage, len(s.values))
	for k, v := range s.values {
		vv := make([]byte, len(v))
		copy(vv, v)
		out[k] = vv
	}
	return out
}

func (s *Store) Hash() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.values))
	for k := range s.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write(jsonutil.CanonicalHash(s.values[k]))
	}
	return hex.EncodeToString(h.Sum(nil))
}
