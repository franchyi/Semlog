package jsonutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func parsePath(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	if path == "*" {
		return []string{"*"}, nil
	}
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("invalid path %q: must start with /", path)
	}
	if path == "/" {
		return []string{""}, nil
	}
	return strings.Split(path[1:], "/"), nil
}

func asObject(doc json.RawMessage) (map[string]any, error) {
	if len(doc) == 0 || string(doc) == "null" {
		return map[string]any{}, nil
	}
	var v any
	if err := json.Unmarshal(doc, &v); err != nil {
		return nil, err
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("document is not an object")
	}
	return obj, nil
}

func toRaw(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func GetPath(doc json.RawMessage, path string) (json.RawMessage, error) {
	segs, err := parsePath(path)
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		if len(doc) == 0 {
			return json.RawMessage("null"), nil
		}
		return doc, nil
	}
	if segs[0] == "*" {
		return nil, errors.New("* is not a readable path")
	}

	var cur any
	if len(doc) == 0 {
		return nil, errors.New("path not found")
	}
	if err := json.Unmarshal(doc, &cur); err != nil {
		return nil, err
	}

	for _, seg := range segs {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, errors.New("path not found")
		}
		next, ok := m[seg]
		if !ok {
			return nil, errors.New("path not found")
		}
		cur = next
	}
	return toRaw(cur)
}

func SetPath(doc json.RawMessage, path string, value json.RawMessage) (json.RawMessage, error) {
	segs, err := parsePath(path)
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		return append(json.RawMessage(nil), value...), nil
	}
	if segs[0] == "*" {
		return nil, errors.New("* is not a writable path")
	}

	obj, err := asObject(doc)
	if err != nil {
		return nil, err
	}

	var val any
	if err := json.Unmarshal(value, &val); err != nil {
		return nil, err
	}

	cur := obj
	for i := 0; i < len(segs)-1; i++ {
		seg := segs[i]
		next, ok := cur[seg]
		if !ok {
			child := map[string]any{}
			cur[seg] = child
			cur = child
			continue
		}
		m, ok := next.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("path segment %q is not object", seg)
		}
		cur = m
	}
	cur[segs[len(segs)-1]] = val
	return toRaw(obj)
}

func PathsOverlap(p1, p2 string) bool {
	if p1 == "*" || p2 == "*" {
		return true
	}
	s1, err1 := parsePath(p1)
	s2, err2 := parsePath(p2)
	if err1 != nil || err2 != nil {
		return false
	}
	return isPrefix(s1, s2) || isPrefix(s2, s1)
}

func isPrefix(a, b []string) bool {
	if len(a) > len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func WriteSetsDisjoint(ws1, ws2 []string) bool {
	for _, p1 := range ws1 {
		for _, p2 := range ws2 {
			if PathsOverlap(p1, p2) {
				return false
			}
		}
	}
	return true
}
