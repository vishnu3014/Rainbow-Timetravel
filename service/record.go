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
}

type DBRecordService struct {
	db *sql.DB
}

func NewDBRecordService(dbConn *sql.DB) DBRecordService {
	return DBRecordService{	db: dbConn }
}

func (s *DBRecordService) GetRecord(ctx context.Context, id int) (entity.Record, error){

	log.Println("Quering the DB to retrieve record with id: ", id)

	query := "select attributes from record_versions where record_id = ? and version = (select max(version) from record_versions where record_id = ?)"
	
	row := s.db.QueryRow(query, id, id)

	var attributesStr string
	err := row.Scan(&attributesStr)
	if err != nil {
		log.Println(" The query failed on execution for id: ", id)
		return entity.Record{}, err
	}

	jsonData := []byte(attributesStr)
	attributesMap := map[string]string{}
	err = json.Unmarshal(jsonData, &attributesMap)
	if err != nil {
		log.Println("The JSON data failed to unmarshal. Data: ", jsonData)
		return entity.Record{}, err
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

	stmt = "insert into record_versions(version, attributes_payload, attributes, attributes_updated_at, record_id, created_at) values (?, ?, ?, ?, ?, ?)"
	_, err = s.db.Exec(stmt, 1, jsonData, jsonData, time.Now().Unix(), record.ID, time.Now().Unix())
	if err != nil {
		return err
	}

	log.Println("Successfully entered the record to the datbase. ID: ", record.ID)
	return nil
}

func (s *DBRecordService) UpdateRecord(ctx context.Context, id int, updates map[string]*string) (entity.Record, error) {
	
	log.Println("Updating record with id: ", id, " in the database.")

	query := "select max(version) from record_versions where record_id = ?"
	row := s.db.QueryRow(query, id)

	maxVersion := 0
	err := row.Scan(&maxVersion)
	if err != nil {
		return entity.Record{}, err
	}

	record, _ := s.GetRecord(ctx, id)
	for key, value := range updates {
		if value == nil {
			delete(record.Data, key)
		} else {
			record.Data[key] = *value
		}
	}

	updatesJson, err := json.Marshal(updates)
	if err != nil {
		return entity.Record{}, err
	}
	
	jsonData, err := json.Marshal(record.Data)
	if err != nil {
		return entity.Record{}, err
	}

	stmt := "insert into record_versions(version, attributes_payload, attributes, attributes_updated_at, record_id, created_at) values (?, ?, ?, ?, ?, ?)"
	_, err = s.db.Exec(stmt, maxVersion + 1, updatesJson, jsonData, time.Now().Unix(), id, time.Now().Unix())

	if err != nil {
		return entity.Record{}, err
	}

	log.Println("The update to the record with id: ", id, " is successfully completed.")
	return record.Copy(), nil	
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
