package entity

// The V2 version of the record that records the version of the attributes.
type Record struct {
	ID                     int                 `json:id`
	Version                int                 `json:version`
	UpdatedTimestamp       int64               `json:updatedTimestamp`
	ReportedTimestamp      int64               `json:reportedTimestamp`
	Data                   map[string]string   `json:data`
}

// The V1 version of the record.
type RecordV1 struct {
	ID   int
	Data map[string]string
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

// Method to covert the V2 version to the V1 version of the record.
func (d *Record) GetRecordV1() RecordV1 {

	record := RecordV1 {ID: d.ID, Data: d.Data}
	return record
}
