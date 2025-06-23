package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/rainbowmga/timetravel/entity"
	"github.com/rainbowmga/timetravel/service"
	"fmt"
)

type RecordPayloadV2 struct {
	UpdatedTimestamp  int                  `json:updatedTimestamp`
	Data              map[string]*string   `json:data`
}

// POST /records/{id}
// if the record exists, the record is updated.
// if the record doesn't exist, the record is created.
func (a *API) PostRecordsAtTimestamp(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := mux.Vars(r)["id"]
	idNumber, err := strconv.ParseInt(id, 10, 32)

	if err != nil || idNumber <= 0 {
		err := writeError(w, "invalid id; id must be a positive number", http.StatusBadRequest)
		logError(err)
		return
	}

	var body map[string]*string
	var recordPayload RecordPayloadV2
	//err = json.NewDecoder(r.Body).Decode(&body)
	err = json.NewDecoder(r.Body).Decode(&recordPayload)

	fmt.Println("*** ", recordPayload.UpdatedTimestamp, " data: ", recordPayload.Data["anupama"])

	if err != nil {
		err := writeError(w, "invalid input; could not parse json", http.StatusBadRequest)
		logError(err)
		return
	}

	// first retrieve the record
	record, err := a.records.GetRecord(
		ctx,
		int(idNumber),
	)

	if !errors.Is(err, service.ErrRecordDoesNotExist) { // record exists
		record, err = a.records.UpdateRecord(ctx, int(idNumber), body)
	} else { // record does not exist

		// exclude the delete updates
		recordMap := map[string]string{}
		for key, value := range recordPayload.Data {
			if value != nil {
				recordMap[key] = *value
			}
		}

		record = entity.Record{
			ID:   int(idNumber),
			Data: recordMap,
			Version: 1,
			UpdatedTimestamp: 1,
			ReportedTimestamp: 1,
		}
		record, err = a.records.CreateRecord(ctx, record)
	}

	if err != nil {
		errInWriting := writeError(w, ErrInternal.Error(), http.StatusInternalServerError)
		logError(err)
		logError(errInWriting)
		return
	}

	err = writeJSON(w, record, http.StatusOK)
	logError(err)
}
