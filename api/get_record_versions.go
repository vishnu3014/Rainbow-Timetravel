package api

import (
	"net/http"
	"strconv"
	"github.com/gorilla/mux"
)

func (a *API) GetRecordVersions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := mux.Vars(r)["id"]

        idNumber, err := strconv.ParseInt(id, 10, 32)

	if err != nil || idNumber <0 {
		err := writeError(w, "invalid id; id must be a positive number", http.StatusBadRequest)
		logError(err)
		return
	}

	versionedRecords, err := a.records.GetVersions(ctx, int(idNumber))
	if err != nil {
		logError(err)
		return
	}
	
	err = writeJSON(w, versionedRecords, http.StatusOK)
	logError(err)
	return
}
