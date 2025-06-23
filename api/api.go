package api

import (
	"github.com/gorilla/mux"
	"github.com/rainbowmga/timetravel/service"
)

type API struct {
	records service.RecordService
}

func NewAPI(records service.RecordService) *API {
	return &API{records}
}

// generates all api routes
func (a *API) CreateRoutes(routes *mux.Router) {
	routes.Path("/records/{id}").HandlerFunc(a.GetRecords).Methods("GET")
	routes.Path("/records/{id}").HandlerFunc(a.PostRecords).Methods("POST")
}

func (a *API) CreateRoutesV2(routes *mux.Router) {
	routes.Path("/records/{id}/versions").HandlerFunc(a.GetRecordVersions).Methods("GET")
	routes.Path("/records/{id}/version/{versionId}").HandlerFunc(a.GetVersionedRecord).Methods("GET")
}
