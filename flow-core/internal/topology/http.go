package topology

import (
	"log"
	"net/http"
	"strconv"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

func HandleConsistency(w http.ResponseWriter, r *http.Request) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	report, err := CheckConsistency(dbpkg.Pool)
	if err != nil {
		log.Printf("ERROR topology consistency check: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Topology consistency check failed")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, report)
}

func HandleBackfill(w http.ResponseWriter, r *http.Request) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dryRun := true
	if raw := r.URL.Query().Get("dryRun"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "Invalid dryRun value")
			return
		}
		dryRun = parsed
	}
	result, err := RepairBackfill(dbpkg.Pool, dryRun)
	if err != nil {
		log.Printf("ERROR topology backfill repair: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Topology backfill repair failed")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, result)
}
