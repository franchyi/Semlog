package proto

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"
)

func ParseOpType(v string) (OpType, error) {
	norm := strings.ToUpper(strings.TrimSpace(v))
	if norm == "" {
		return OpType_OP_UNKNOWN, fmt.Errorf("unknown op_type %q", v)
	}
	if n, ok := OpType_value[norm]; ok {
		return OpType(n), nil
	}
	if !strings.HasPrefix(norm, "OP_") {
		norm = "OP_" + norm
	}
	if n, ok := OpType_value[norm]; ok {
		return OpType(n), nil
	}
	return OpType_OP_UNKNOWN, fmt.Errorf("unknown op_type %q", v)
}

func MarshalJSON(msg gproto.Message) ([]byte, error) {
	return protojson.MarshalOptions{UseProtoNames: true}.Marshal(msg)
}

func UnmarshalJSON(data []byte, msg gproto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, msg)
}
