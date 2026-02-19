package applier

import (
	"encoding/json"
	"testing"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
)

func TestClassifyRules(t *testing.T) {
	incA := &pb.AcceptedRecord{OpType: pb.OpType_OP_INC, WriteSet: []string{"/stats/views"}, ArgsJson: json.RawMessage(`{"path":"/stats/views","delta":1}`)}
	incB := &pb.AcceptedRecord{OpType: pb.OpType_OP_INC, WriteSet: []string{"/stats/views"}, ArgsJson: json.RawMessage(`{"path":"/stats/views","delta":2}`)}
	c, _ := Classify(incA, incB, nil)
	if c != ClassificationMergeable {
		t.Fatal("expected INC same path to be mergeable")
	}

	disA := &pb.AcceptedRecord{WriteSet: []string{"/email"}}
	disB := &pb.AcceptedRecord{WriteSet: []string{"/phone"}}
	c, _ = Classify(disA, disB, nil)
	if c != ClassificationMergeable {
		t.Fatal("expected disjoint to be mergeable")
	}

	put := &pb.AcceptedRecord{WriteSet: []string{"*"}}
	c, _ = Classify(put, disB, nil)
	if c != ClassificationConflicting {
		t.Fatal("expected * to conflict")
	}

	pia := &pb.AcceptedRecord{OpType: pb.OpType_OP_PUT_IF_ABSENT, WriteSet: []string{"*"}}
	c, _ = Classify(pia, disB, json.RawMessage(`{"x":1}`))
	if c != ClassificationConflicting {
		t.Fatal("star must still conflict before put_if_absent rule")
	}

	pia.WriteSet = []string{"/x"}
	c, _ = Classify(pia, disB, json.RawMessage(`{"x":1}`))
	if c != ClassificationMergeable {
		t.Fatal("put_if_absent on existing key should be mergeable")
	}
}

func TestClassifyNaiveModeAlwaysConflicting(t *testing.T) {
	disA := &pb.AcceptedRecord{WriteSet: []string{"/email"}}
	disB := &pb.AcceptedRecord{WriteSet: []string{"/phone"}}
	c, _ := ClassifyWithMode(ClassifyModeNaive, disA, disB, nil)
	if c != ClassificationConflicting {
		t.Fatal("expected naive mode to always conflict")
	}
}
