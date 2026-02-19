package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
	"github.com/yourorg/redpanda-mm/pkg/jsonutil"
)

type Outcome struct {
	Type      pb.OutcomeType
	Reason    string
	Transform json.RawMessage
}

func Apply(op *pb.AcceptedRecord, state json.RawMessage) (json.RawMessage, Outcome) {
	if op == nil {
		return state, Outcome{Type: pb.OutcomeType_OUTCOME_FAIL, Reason: "nil op"}
	}

	switch op.OpType {
	case pb.OpType_OP_PUT:
		var args struct {
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(op.ArgsJson, &args); err != nil {
			return state, fail("invalid PUT args")
		}
		return cloneRaw(args.Value), ok()

	case pb.OpType_OP_PATCH:
		var args struct {
			Patches []struct {
				Path  string          `json:"path"`
				Value json.RawMessage `json:"value"`
			} `json:"patches"`
		}
		if err := json.Unmarshal(op.ArgsJson, &args); err != nil {
			return state, fail("invalid PATCH args")
		}
		next := cloneRaw(state)
		for _, p := range args.Patches {
			if p.Path == "" {
				return state, fail("invalid path")
			}
			var err error
			next, err = jsonutil.SetPath(next, p.Path, p.Value)
			if err != nil {
				return state, fail("invalid path")
			}
		}
		return next, ok()

	case pb.OpType_OP_INC:
		var args struct {
			Path  string  `json:"path"`
			Delta float64 `json:"delta"`
		}
		if err := json.Unmarshal(op.ArgsJson, &args); err != nil {
			return state, fail("invalid INC args")
		}
		curRaw, err := jsonutil.GetPath(state, args.Path)
		if err != nil {
			return state, fail("not numeric")
		}
		var cur any
		if err := json.Unmarshal(curRaw, &cur); err != nil {
			return state, fail("not numeric")
		}
		n, okNum := asFloat(cur)
		if !okNum {
			return state, fail("not numeric")
		}
		nextValue, _ := json.Marshal(n + args.Delta)
		next, err := jsonutil.SetPath(state, args.Path, nextValue)
		if err != nil {
			return state, fail("invalid path")
		}
		return next, ok()

	case pb.OpType_OP_PUT_IF_ABSENT:
		if exists(state) {
			return state, Outcome{Type: pb.OutcomeType_OUTCOME_NOOP, Reason: "key already exists"}
		}
		var args struct {
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(op.ArgsJson, &args); err != nil {
			return state, fail("invalid PUT_IF_ABSENT args")
		}
		return cloneRaw(args.Value), ok()

	case pb.OpType_OP_CAS:
		var args struct {
			Path     string `json:"path"`
			Expected any    `json:"expected"`
			NewValue any    `json:"new_value"`
		}
		if err := json.Unmarshal(op.ArgsJson, &args); err != nil {
			return state, fail("invalid CAS args")
		}
		curRaw, err := jsonutil.GetPath(state, args.Path)
		if err != nil {
			return state, fail("CAS mismatch")
		}
		var cur any
		if err := json.Unmarshal(curRaw, &cur); err != nil {
			return state, fail("CAS mismatch")
		}
		if !reflect.DeepEqual(cur, args.Expected) {
			return state, fail("CAS mismatch")
		}
		nv, _ := json.Marshal(args.NewValue)
		next, err := jsonutil.SetPath(state, args.Path, nv)
		if err != nil {
			return state, fail("invalid path")
		}
		return next, ok()

	case pb.OpType_OP_RESERVE:
		stock, reserved, err := stockReserved(state)
		if err != nil {
			return state, fail("invalid inventory state")
		}
		var args struct {
			N float64 `json:"n"`
		}
		if err := json.Unmarshal(op.ArgsJson, &args); err != nil {
			return state, fail("invalid RESERVE args")
		}
		if stock-reserved < args.N {
			return state, fail("insufficient stock")
		}
		next, err := jsonutil.SetPath(state, "/reserved", mustJSON(reserved+args.N))
		if err != nil {
			return state, fail("invalid inventory state")
		}
		return next, ok()

	case pb.OpType_OP_CANCEL:
		_, reserved, err := stockReserved(state)
		if err != nil {
			return state, fail("invalid inventory state")
		}
		var args struct {
			N float64 `json:"n"`
		}
		if err := json.Unmarshal(op.ArgsJson, &args); err != nil {
			return state, fail("invalid CANCEL args")
		}
		if reserved < args.N {
			return state, fail("insufficient reserved")
		}
		next, err := jsonutil.SetPath(state, "/reserved", mustJSON(reserved-args.N))
		if err != nil {
			return state, fail("invalid inventory state")
		}
		return next, ok()

	case pb.OpType_OP_CLAIM:
		var args struct {
			WorkerID string `json:"worker_id"`
		}
		if err := json.Unmarshal(op.ArgsJson, &args); err != nil {
			return state, fail("invalid CLAIM args")
		}
		if args.WorkerID == "" {
			return state, fail("worker_id required")
		}
		st, _ := getString(state, "/state")
		if st != "READY" {
			return state, fail("not READY")
		}
		next := cloneRaw(state)
		var err error
		next, err = jsonutil.SetPath(next, "/state", mustJSON("CLAIMED"))
		if err != nil {
			return state, fail("invalid task state")
		}
		next, _ = jsonutil.SetPath(next, "/owner", mustJSON(args.WorkerID))
		attemptRaw, err := jsonutil.GetPath(next, "/attempt")
		attempt := 0.0
		if err == nil {
			var v any
			if json.Unmarshal(attemptRaw, &v) == nil {
				if n, okNum := asFloat(v); okNum {
					attempt = n
				}
			}
		}
		next, _ = jsonutil.SetPath(next, "/attempt", mustJSON(attempt+1))
		return next, ok()

	case pb.OpType_OP_COMPLETE:
		var args struct {
			WorkerID string `json:"worker_id"`
		}
		if err := json.Unmarshal(op.ArgsJson, &args); err != nil {
			return state, fail("invalid COMPLETE args")
		}
		st, _ := getString(state, "/state")
		owner, _ := getString(state, "/owner")
		if st != "CLAIMED" || owner != args.WorkerID {
			return state, fail("not CLAIMED by this worker")
		}
		next, err := jsonutil.SetPath(state, "/state", mustJSON("DONE"))
		if err != nil {
			return state, fail("invalid task state")
		}
		return next, ok()
	}

	return state, Outcome{Type: pb.OutcomeType_OUTCOME_FAIL, Reason: fmt.Sprintf("unsupported op %s", op.OpType.String())}
}

func ok() Outcome {
	return Outcome{Type: pb.OutcomeType_OUTCOME_COMMIT_PATCH}
}

func fail(reason string) Outcome {
	return Outcome{Type: pb.OutcomeType_OUTCOME_FAIL, Reason: reason}
}

func exists(state json.RawMessage) bool {
	if len(state) == 0 {
		return false
	}
	return string(state) != "null"
}

func cloneRaw(in json.RawMessage) json.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func getString(state json.RawMessage, path string) (string, error) {
	raw, err := jsonutil.GetPath(state, path)
	if err != nil {
		return "", err
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", err
	}
	return s, nil
}

func stockReserved(state json.RawMessage) (stock float64, reserved float64, err error) {
	sRaw, err := jsonutil.GetPath(state, "/stock")
	if err != nil {
		return 0, 0, err
	}
	rRaw, err := jsonutil.GetPath(state, "/reserved")
	if err != nil {
		return 0, 0, err
	}
	var sv, rv any
	if err := json.Unmarshal(sRaw, &sv); err != nil {
		return 0, 0, err
	}
	if err := json.Unmarshal(rRaw, &rv); err != nil {
		return 0, 0, err
	}
	s, ok := asFloat(sv)
	if !ok {
		return 0, 0, errors.New("stock not numeric")
	}
	r, ok := asFloat(rv)
	if !ok {
		return 0, 0, errors.New("reserved not numeric")
	}
	return s, r, nil
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
