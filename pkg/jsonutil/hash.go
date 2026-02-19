package jsonutil

import (
	"crypto/sha256"
	"encoding/json"
)

func CanonicalHash(v json.RawMessage) []byte {
	if len(v) == 0 {
		s := sha256.Sum256([]byte("null"))
		return s[:]
	}
	var obj any
	if err := json.Unmarshal(v, &obj); err != nil {
		s := sha256.Sum256(v)
		return s[:]
	}
	b, _ := json.Marshal(obj)
	s := sha256.Sum256(b)
	return s[:]
}
