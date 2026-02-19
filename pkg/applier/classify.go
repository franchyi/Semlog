package applier

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
	"github.com/yourorg/redpanda-mm/pkg/jsonutil"
)

type Classification int

const (
	ClassificationUnknown Classification = iota
	ClassificationMergeable
	ClassificationConflicting
)

const (
	ClassifyModeStructural = "structural"
	ClassifyModeNaive      = "naive"
)

func NormalizeClassifyMode(mode string) string {
	switch mode {
	case ClassifyModeNaive:
		return ClassifyModeNaive
	default:
		return ClassifyModeStructural
	}
}

func Classify(op1, op2 *pb.AcceptedRecord, stableState json.RawMessage) (Classification, pb.ConflictReason) {
	return ClassifyWithMode(ClassifyModeStructural, op1, op2, stableState)
}

func ClassifyWithMode(mode string, op1, op2 *pb.AcceptedRecord, stableState json.RawMessage) (Classification, pb.ConflictReason) {
	if NormalizeClassifyMode(mode) == ClassifyModeNaive {
		return ClassificationConflicting, pb.ConflictReason_REASON_SAME_FIELD
	}

	if containsStar(op1.WriteSet) || containsStar(op2.WriteSet) {
		return ClassificationConflicting, pb.ConflictReason_REASON_OVERLAP_WRITESET
	}

	if op1.OpType == pb.OpType_OP_INC && op2.OpType == pb.OpType_OP_INC {
		p1 := incPath(op1.ArgsJson)
		p2 := incPath(op2.ArgsJson)
		if p1 != "" && p1 == p2 {
			return ClassificationMergeable, pb.ConflictReason_REASON_UNKNOWN
		}
	}

	if jsonutil.WriteSetsDisjoint(op1.WriteSet, op2.WriteSet) {
		return ClassificationMergeable, pb.ConflictReason_REASON_UNKNOWN
	}

	keyExists := len(stableState) > 0 && string(stableState) != "null"
	if keyExists && (op1.OpType == pb.OpType_OP_PUT_IF_ABSENT || op2.OpType == pb.OpType_OP_PUT_IF_ABSENT) {
		return ClassificationMergeable, pb.ConflictReason_REASON_UNKNOWN
	}

	return ClassificationConflicting, pb.ConflictReason_REASON_SAME_FIELD
}

func containsStar(ws []string) bool {
	for _, p := range ws {
		if p == "*" {
			return true
		}
	}
	return false
}

func incPath(args json.RawMessage) string {
	var a struct {
		Path string `json:"path"`
	}
	_ = json.Unmarshal(args, &a)
	return a.Path
}

func ConflictID(key string, opIDs []string, baseStateHash []byte) string {
	sorted := append([]string(nil), opIDs...)
	sort.Strings(sorted)
	h := sha256.New()
	h.Write([]byte(key))
	h.Write(baseStateHash)
	for _, id := range sorted {
		h.Write([]byte(id))
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
