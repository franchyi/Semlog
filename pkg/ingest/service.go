package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	pb "github.com/yourorg/redpanda-mm/gen/proto"
	"github.com/yourorg/redpanda-mm/pkg/hlc"
	"github.com/yourorg/redpanda-mm/pkg/kafka"
)

type Service struct {
	region   string
	topic    string
	producer *kafka.Producer
	clock    *hlc.HLC
}

type WriteRequest struct {
	Key       string          `json:"key"`
	OpType    string          `json:"op_type"`
	Args      json.RawMessage `json:"args"`
	WriteSet  []string        `json:"write_set"`
	ClientID  string          `json:"client_id"`
	SessionID string          `json:"session_id"`
}

type WriteResponse struct {
	OpID      string `json:"op_id"`
	Region    string `json:"region"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	HLC       pb.HLC `json:"hlc"`
}

func NewService(region string, producer *kafka.Producer) (*Service, error) {
	region = strings.ToUpper(strings.TrimSpace(region))
	if region != "A" && region != "B" {
		return nil, errors.New("region must be A or B")
	}
	return &Service{
		region:   region,
		topic:    "ingest." + region,
		producer: producer,
		clock:    &hlc.HLC{},
	}, nil
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/write", s.handleWrite)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func (s *Service) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	opType, err := pb.ParseOpType(req.OpType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.WriteSet) == 0 {
		req.WriteSet = defaultWriteSet(opType, req.Args)
	}

	ts := s.clock.Now()
	rec := &pb.AcceptedRecord{
		OpId:       uuid.NewString(),
		Region:     s.region,
		Key:        req.Key,
		OpType:     opType,
		ArgsJson:   append([]byte(nil), req.Args...),
		WriteSet:   append([]string(nil), req.WriteSet...),
		Hlc:        &pb.HLC{PhysicalMs: ts.PhysicalMS, Logical: ts.Logical},
		IngestTsMs: time.Now().UnixMilli(),
		ClientId:   req.ClientID,
		SessionId:  req.SessionID,
	}

	payload, err := pb.MarshalJSON(rec)
	if err != nil {
		http.Error(w, "failed to serialize record", http.StatusInternalServerError)
		return
	}
	partition, offset, err := s.producer.Write(context.Background(), s.topic, rec.Key, payload)
	if err != nil {
		http.Error(w, "failed to produce record: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := WriteResponse{
		OpID:      rec.OpId,
		Region:    s.region,
		Topic:     s.topic,
		Partition: partition,
		Offset:    offset,
		HLC:       *rec.Hlc,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func defaultWriteSet(opType pb.OpType, args json.RawMessage) []string {
	switch opType {
	case pb.OpType_OP_PUT:
		return []string{"*"}
	case pb.OpType_OP_INC:
		var a struct {
			Path string `json:"path"`
		}
		_ = json.Unmarshal(args, &a)
		if a.Path != "" {
			return []string{a.Path}
		}
	case pb.OpType_OP_PATCH:
		var a struct {
			Patches []struct {
				Path string `json:"path"`
			} `json:"patches"`
		}
		_ = json.Unmarshal(args, &a)
		out := make([]string, 0, len(a.Patches))
		for _, p := range a.Patches {
			if p.Path != "" {
				out = append(out, p.Path)
			}
		}
		if len(out) > 0 {
			return out
		}
	case pb.OpType_OP_CAS:
		var a struct {
			Path string `json:"path"`
		}
		_ = json.Unmarshal(args, &a)
		if a.Path != "" {
			return []string{a.Path}
		}
	}
	return []string{"*"}
}
