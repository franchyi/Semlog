package hlc

import (
	"sync"
	"time"
)

type Timestamp struct {
	PhysicalMS int64
	Logical    uint32
}

type HLC struct {
	PhysicalMS int64
	Logical    uint32
	mu         sync.Mutex
}

func (h *HLC) Now() Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now().UnixMilli()
	if now > h.PhysicalMS {
		h.PhysicalMS = now
		h.Logical = 0
	} else {
		h.Logical++
	}
	return Timestamp{PhysicalMS: h.PhysicalMS, Logical: h.Logical}
}

func (h *HLC) Update(remote Timestamp) Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now().UnixMilli()
	if now > h.PhysicalMS && now > remote.PhysicalMS {
		h.PhysicalMS = now
		h.Logical = 0
	} else if remote.PhysicalMS > h.PhysicalMS {
		h.PhysicalMS = remote.PhysicalMS
		h.Logical = remote.Logical + 1
	} else if h.PhysicalMS == remote.PhysicalMS {
		if remote.Logical > h.Logical {
			h.Logical = remote.Logical + 1
		} else {
			h.Logical++
		}
	} else {
		h.Logical++
	}
	return Timestamp{PhysicalMS: h.PhysicalMS, Logical: h.Logical}
}

func Compare(a, b Timestamp) int {
	if a.PhysicalMS < b.PhysicalMS {
		return -1
	}
	if a.PhysicalMS > b.PhysicalMS {
		return 1
	}
	if a.Logical < b.Logical {
		return -1
	}
	if a.Logical > b.Logical {
		return 1
	}
	return 0
}
