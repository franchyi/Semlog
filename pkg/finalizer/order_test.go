package finalizer

import (
	"testing"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
)

func TestCompareOps(t *testing.T) {
	a := &pb.OpRef{Region: "A", OpId: "a", Hlc: &pb.HLC{PhysicalMs: 1, Logical: 0}}
	b := &pb.OpRef{Region: "B", OpId: "b", Hlc: &pb.HLC{PhysicalMs: 2, Logical: 0}}
	if CompareOps(a, b) >= 0 {
		t.Fatal("expected a < b")
	}
	if CompareOps(b, a) <= 0 {
		t.Fatal("expected b > a")
	}
}
