package api

import (
	"net/http"
	"strconv"
	"github.com/gorilla/mux"
	"fmt"
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
		err2 := writeError(w, fmt.Sprintf("The versions for the record could not be read from the db."), http.StatusBadRequest)
		logError(err2)
		return
	}
	
	err = writeJSON(w, versionedRecords, http.StatusOK)
	logError(err)
	return
}

func (a *API) GetVersionedRecord(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := mux.Vars(r)["id"]
	versionId := mux.Vars(r)["versionId"]

	idNumber, err := strconv.ParseInt(id, 10, 32)
	versionNumber, err := strconv.ParseInt(versionId, 10, 32)

	if versionNumber < 1 {
		err := writeError(w, fmt.Sprintf("The version needs to be greater than 0, input was  %v", versionNumber), http.StatusBadRequest)
		logError(err)
		return
	}
	
	versionedRecord, err := a.records.GetVersionedRecord(ctx, int(idNumber), int(versionNumber))
	if err != nil {
		err := writeError(w, fmt.Sprintf("The versioned record could be read from the db"), http.StatusBadRequest)
		logError(err)
		return
	}

	err = writeJSON(w, versionedRecord, http.StatusOK)
	logError(err)
	return
}
