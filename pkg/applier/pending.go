package applier

import (
	"sync"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
)

type PendingEntry struct {
	Op        *pb.AcceptedRecord
	Topic     string
	Partition int
	Offset    int64
}

type PendingWindow struct {
	mu    sync.RWMutex
	byKey map[string][]PendingEntry
}

func NewPendingWindow() *PendingWindow {
	return &PendingWindow{byKey: map[string][]PendingEntry{}}
}

func (p *PendingWindow) Add(key string, entry PendingEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.byKey[key] = append(p.byKey[key], entry)
}

func (p *PendingWindow) GetOtherRegion(key, region string) []PendingEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	entries := p.byKey[key]
	out := make([]PendingEntry, 0, len(entries))
	for _, e := range entries {
		if e.Op != nil && e.Op.Region != region {
			out = append(out, e)
		}
	}
	return out
}

func (p *PendingWindow) RemoveOps(key string, opIDs []string) {
	if len(opIDs) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	set := make(map[string]struct{}, len(opIDs))
	for _, id := range opIDs {
		set[id] = struct{}{}
	}
	entries := p.byKey[key]
	out := entries[:0]
	for _, e := range entries {
		if e.Op == nil {
			continue
		}
		if _, ok := set[e.Op.OpId]; ok {
			continue
		}
		out = append(out, e)
	}
	if len(out) == 0 {
		delete(p.byKey, key)
		return
	}
	p.byKey[key] = out
}
