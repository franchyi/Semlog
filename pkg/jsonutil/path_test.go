package jsonutil

import (
	"encoding/json"
	"testing"
)

func TestPathsOverlap(t *testing.T) {
	cases := []struct {
		a, b string
		ok   bool
	}{
		{"/addr", "/addr/city", true},
		{"/addr/city", "/addr/zip", false},
		{"/email", "/phone", false},
		{"/ad", "/addr", false},
		{"*", "/x", true},
	}
	for _, c := range cases {
		if got := PathsOverlap(c.a, c.b); got != c.ok {
			t.Fatalf("overlap(%s,%s)=%v want %v", c.a, c.b, got, c.ok)
		}
	}
}

func TestWriteSetsDisjoint(t *testing.T) {
	if !WriteSetsDisjoint([]string{"/email"}, []string{"/phone"}) {
		t.Fatal("expected disjoint")
	}
	if WriteSetsDisjoint([]string{"/addr"}, []string{"/addr/city"}) {
		t.Fatal("expected overlap")
	}
}

func TestGetSetPath(t *testing.T) {
	doc := json.RawMessage(`{"addr":{"city":"NY"}}`)
	out, err := SetPath(doc, "/addr/zip", json.RawMessage(`"10001"`))
	if err != nil {
		t.Fatal(err)
	}
	v, err := GetPath(out, "/addr/zip")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `"10001"` {
		t.Fatalf("got %s", string(v))
	}
}
