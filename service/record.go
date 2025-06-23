package service

import (
	"context"
	"errors"
	"github.com/rainbowmga/timetravel/entity"
	"database/sql"
	"time"
	"log"
	"encoding/json"
)

var ErrRecordDoesNotExist = errors.New("record with that id does not exist")
var ErrRecordIDInvalid = errors.New("record id must >= 0")
var ErrRecordAlreadyExists = errors.New("record already exists")

// Implements method to get, create, and update record data.
type RecordService interface {

	// GetRecord will retrieve an record.
	GetRecord(ctx context.Context, id int) (entity.Record, error)

	// CreateRecord will insert a new record.
	//
	// If it a record with that id already exists it will fail.
	CreateRecord(ctx context.Context, record entity.Record) error

	// UpdateRecord will change the internal `Map` values of the record if they exist.
	// if the update[key] is null it will delete that key from the record's Map.
	//
	// UpdateRecord will error if id <= 0 or the record does not exist with that id.
	UpdateRecord(ctx context.Context, id int, updates map[string]*string) (entity.Record, error)
	
	// GetVersions will get all the version of a record and it's corresponding created timestamp.
	GetVersions(ctx context.Context, id int) ([]entity.VersionedRecord, error)

	// GetRecord will get a record with a specific version
	GetVersionedRecord(ctx context.Context, id int, version int) (entity.VersionedRecord, error)
}

type DBRecordService struct {
	db *sql.DB
}

func NewDBRecordService(dbConn *sql.DB) DBRecordService {
	return DBRecordService{	db: dbConn }
}

func (s *DBRecordService) GetRecord(ctx context.Context, id int) (entity.Record, error){

	log.Println("Quering the DB to retrieve record with id: ", id)

	query := "select attributes from record_versions where record_id = ? order by actual_update_timestamp desc limit 1"
	
	row := s.db.QueryRow(query, id, id)

	var attributesStr string
	err := row.Scan(&attributesStr)
	if err != nil {
		log.Println("The query failed on execution for id: ", id, " error: ", err)
		return entity.Record{}, ErrRecordDoesNotExist
	}

	jsonData := []byte(attributesStr)
	attributesMap := map[string]string{}
	err = json.Unmarshal(jsonData, &attributesMap)
	if err != nil {
		log.Println("The JSON data failed to unmarshal. Data: ", jsonData)
		return entity.Record{}, ErrRecordDoesNotExist
	}

	log.Println("The query to the DB completed successfully for the record with id: ", id)
	record := entity.Record{ ID: id, Data: attributesMap }
	return record, nil
}

func (s *DBRecordService) CreateRecord(ctx context.Context, record entity.Record) error {
	log.Println("Checking if a record with exists with id: ", record.ID)
	
	query := `select count(*) from records where id = ?`
	row := s.db.QueryRow(query, record.ID)

	count := 0
	err := row.Scan(&count)
	if err != nil {
		return err
	}

	if count != 0 {
		log.Println("Record exists with the ID:", record.ID, " exists in the DB. Please enter a valid ID.")
		return ErrRecordAlreadyExists
	}

	// TODO: Add a transaction here !
	
	// If the record does not exist, add a record to the db.
	stmt := "insert into records (id, created_at) values (?, ?)"
	_, err = s.db.Exec(stmt, record.ID, time.Now().Unix())
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(record.Data)
	if err != nil {
		return err
	}

	stmt = "insert into record_versions(attributes, actual_update_timestamp, record_id, created_at) values (?, ?, ?, ?)"
	_, err = s.db.Exec(stmt, jsonData, time.Now().Unix(), record.ID, time.Now().Unix())
	if err != nil {
		return err
	}

	log.Println("Successfully added a record to the datbase with ID: ", record.ID)
	return nil
}

func (s *DBRecordService) UpdateRecord(ctx context.Context, id int, updates map[string]*string) (entity.Record, error) {
	log.Println("Updating record with id: ", id, " in the database.")

	// TODO: Pass the record from the callee
	record, _ := s.GetRecord(ctx, id)
	for key, value := range updates {
		if value == nil {
			delete(record.Data, key)
		} else {
			record.Data[key] = *value
		}
	}

	jsonData, err := json.Marshal(record.Data)
	if err != nil {
		return entity.Record{}, err
	}

	stmt := "insert into record_versions(attributes, actual_update_timestamp, record_id, created_at) values (?, ?, ?, ?)"
	_, err = s.db.Exec(stmt, jsonData, time.Now().Unix(), id, time.Now().Unix())

	if err != nil {
		return entity.Record{}, err
	}

	log.Println("The update to the record with id: ", id, " is successfully completed.")
	return record.Copy(), nil	
}

// TODO: When the record is not found, return the appropriate error.
func (s *DBRecordService) GetVersions(ctx context.Context, id int) ([]entity.VersionedRecord, error) {

	var versionedRecords []entity.VersionedRecord

	_, err := s.GetRecord(ctx, id)
	if err != nil {
		log.Println("The record with id: ", id, " could not be found.")
		return versionedRecords, err
	}
	
	query := "select id, attributes, actual_update_timestamp, created_at from record_versions where record_id = ? order by actual_update_timestamp asc"
	rows, err := s.db.Query(query, id)
	if err != nil {
		log.Println("There was an error when quering the versions. Error: ", err)
		return versionedRecords, err 
	}

	defer rows.Close()

	version := 1
	for rows.Next() {
		var versionedRecord entity.VersionedRecord
		var attributesStr string
		
		rows.Scan(&versionedRecord.ID, &attributesStr, &versionedRecord.ActualUpdatedTimestamp, &versionedRecord.ReportedTimestamp)

		jsonData := []byte(attributesStr)
		json.Unmarshal(jsonData, &versionedRecord.Data)

		versionedRecord.Version = version
		version = version + 1
		
		versionedRecords = append(versionedRecords, versionedRecord)
	}

	return versionedRecords, nil
}

func (s *DBRecordService) GetVersionedRecord(ctx context.Context, id int, version int) (entity.VersionedRecord, error) {

	var versionedRecord entity.VersionedRecord

	query := "select id, attributes, actual_update_timestamp, created_at from record_versions where record_id = ? order by actual_update_timestamp asc limit 1 offset ?"

	row := s.db.QueryRow(query, id, version-1)
		
	var attributesStr string
	err := row.Scan(&versionedRecord.ID, &attributesStr, &versionedRecord.ActualUpdatedTimestamp, &versionedRecord.ReportedTimestamp)
	if err != nil {
		return versionedRecord, err
	}

	jsonData :=[]byte(attributesStr)
	json.Unmarshal(jsonData, &versionedRecord.Data)

	versionedRecord.Version = version

	return versionedRecord, nil
}


// InMemoryRecordService is an in-memory implementation of RecordService.
type InMemoryRecordService struct {
	data map[int]entity.Record
}

func NewInMemoryRecordService() InMemoryRecordService {
	return InMemoryRecordService{
		data: map[int]entity.Record{},
	}
}

func (s *InMemoryRecordService) GetRecord(ctx context.Context, id int) (entity.Record, error) {
	record := s.data[id]
	if record.ID == 0 {
		return entity.Record{}, ErrRecordDoesNotExist
	}

	record = record.Copy() // copy is necessary so modifations to the record don't change the stored record
	return record, nil
}

func (s *InMemoryRecordService) CreateRecord(ctx context.Context, record entity.Record) error {
	id := record.ID
	if id <= 0 {
		return ErrRecordIDInvalid
	}

	existingRecord := s.data[id]
	if existingRecord.ID != 0 {
		return ErrRecordAlreadyExists
	}

	s.data[id] = record
	return nil
}

func (s *InMemoryRecordService) UpdateRecord(ctx context.Context, id int, updates map[string]*string) (entity.Record, error) {
	entry := s.data[id]
	if entry.ID == 0 {
		return entity.Record{}, ErrRecordDoesNotExist
	}

	for key, value := range updates {
		if value == nil { // deletion update
			delete(entry.Data, key)
		} else {
			entry.Data[key] = *value
		}
	}

	return entry.Copy(), nil
}
