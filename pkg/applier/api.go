package applier

import (
	"encoding/json"
	"net/http"
)

type OpStatus struct {
	Status  string `json:"status"`
	Outcome string `json:"outcome,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

func (a *Applier) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/stable", a.handleStable)
	mux.HandleFunc("/status", a.handleStatus)
	mux.HandleFunc("/hash", a.handleHash)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func (a *Applier) handleStable(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	v, ok := a.store.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(v)
}

func (a *Applier) handleStatus(w http.ResponseWriter, r *http.Request) {
	opID := r.URL.Query().Get("op_id")
	if opID == "" {
		http.Error(w, "op_id is required", http.StatusBadRequest)
		return
	}
	a.statusMu.RLock()
	st, ok := a.status[opID]
	a.statusMu.RUnlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

func (a *Applier) handleHash(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"hash": a.store.Hash()})
}
