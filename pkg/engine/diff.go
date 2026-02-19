package engine

import (
	"encoding/json"
	"reflect"
)

func Diff(before, after json.RawMessage) json.RawMessage {
	var bAny any
	var aAny any
	_ = json.Unmarshal(normalize(before), &bAny)
	_ = json.Unmarshal(normalize(after), &aAny)

	patch := diffAny(bAny, aAny)
	if patch == nil {
		return json.RawMessage(`{}`)
	}
	out, err := json.Marshal(patch)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return out
}

func normalize(v json.RawMessage) []byte {
	if len(v) == 0 {
		return []byte("null")
	}
	return v
}

func diffAny(before, after any) any {
	if reflect.DeepEqual(before, after) {
		return nil
	}
	bMap, bObj := before.(map[string]any)
	aMap, aObj := after.(map[string]any)
	if !bObj || !aObj {
		return after
	}

	patch := map[string]any{}
	seen := map[string]struct{}{}
	for k, bv := range bMap {
		seen[k] = struct{}{}
		av, ok := aMap[k]
		if !ok {
			patch[k] = nil
			continue
		}
		sub := diffAny(bv, av)
		if sub != nil {
			patch[k] = sub
		}
	}
	for k, av := range aMap {
		if _, ok := seen[k]; ok {
			continue
		}
		patch[k] = av
	}
	if len(patch) == 0 {
		return nil
	}
	return patch
}
