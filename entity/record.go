package entity

type Record struct {
	ID                     int                 `json:id`
	Version                int                 `json:version`
	UpdatedTimestamp       int64               `json:updatedTimestamp`
	ReportedTimestamp      int64               `json:reportedTimestamp`
	Data                   map[string]string   `json:data`
}

func (d *Record) Copy() Record {
	values := d.Data

	newMap := map[string]string{}
	for key, value := range values {
		newMap[key] = value
	}

	return Record {
		ID: d.ID,
		Version: d.Version,
		UpdatedTimestamp: d.UpdatedTimestamp,
		ReportedTimestamp: d.ReportedTimestamp,
		Data: newMap,
	}
			
}
