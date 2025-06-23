package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/rainbowmga/timetravel/entity"
	"github.com/rainbowmga/timetravel/service"
	"time"
	"context"
)

// POST /records/{id}
// if the record exists, the record is updated.
// if the record doesn't exist, the record is created.
func (a *API) PostRecords(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := mux.Vars(r)["id"]
	idNumber, err := strconv.ParseInt(id, 10, 32)

	if err != nil || idNumber <= 0 {
		err := writeError(w, "invalid id; id must be a positive number", http.StatusBadRequest)
		logError(err)
		return
	}

	var body map[string]*string
	err = json.NewDecoder(r.Body).Decode(&body)

	if err != nil {
		err := writeError(w, "invalid input; could not parse json", http.StatusBadRequest)
		logError(err)
		return
	}

	record, err := a.ProcessInput(ctx, int(idNumber), time.Now().Unix(), body)
	if err != nil {
		errInWriting := writeError(w, ErrInternal.Error(), http.StatusInternalServerError)
		logError(err)
		logError(errInWriting)
		return
	}

	err = writeJSON(w, record, http.StatusOK)
	logError(err)
}

func (a *API) ProcessInput(ctx context.Context, recordId int, updatedTimestamp int64, body map[string]*string) (entity.Record, error) {

	// Check for the existence of the record
	record, err := a.records.GetRecord(ctx, recordId)

	// record exists
	if !errors.Is(err, service.ErrRecordDoesNotExist) {

		record, err = a.records.UpdateRecord(ctx, recordId, body)

	} else { // record does not exist

		recordMap := map[string]string{}
		for key, value := range body {
			if value != nil {
				recordMap[key] = *value
			}
		}

		record = entity.Record{
			ID:  recordId,
			Version: 1,
			UpdatedTimestamp: updatedTimestamp,
			ReportedTimestamp: 0,
			Data: recordMap,
		}
		record, err = a.records.CreateRecord(ctx, record)
	}

	return record, err
}
