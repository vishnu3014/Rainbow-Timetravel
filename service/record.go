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
	CreateRecord(ctx context.Context, record entity.Record) (entity.Record, error)

	// UpdateRecord will change the internal `Map` values of the record if they exist.
	// if the update[key] is null it will delete that key from the record's Map.
	//
	// UpdateRecord will error if id <= 0 or the record does not exist with that id.
	UpdateRecord(ctx context.Context, id int, updatedTimestamp int64, updates map[string]*string) (entity.Record, error)
	
	// GetVersions will get all the version of a record and it's corresponding created timestamp.
	GetVersions(ctx context.Context, id int) ([]entity.Record, error)

	// GetRecord will get a record with a specific version
	GetVersionedRecord(ctx context.Context, id int, version int) (entity.Record, error)
}

type DBRecordService struct {
	db *sql.DB
}

func NewDBRecordService(dbConn *sql.DB) DBRecordService {
	return DBRecordService{	db: dbConn }
}

// Gets the latest version of the record.
func (s *DBRecordService) GetRecord(ctx context.Context, id int) (entity.Record, error){

	log.Println("Quering the DB to retrieve record with id: ", id)

	// Get the attributes of the record
	query := "select attributes, actual_update_timestamp, created_at from record_versions where record_id = ? order by actual_update_timestamp desc limit 1"
	
	row := s.db.QueryRow(query, id)
	
	return s.GetRecordDetails(id, row)
}

// Gets the version of record that occurs before a timestamp.
// This version of the record is used as a base to apply updates to the attributes.
// The updates to the attributes are based on the actual updated time not the reported time.
func (s *DBRecordService) GetRecordAt(ctx context.Context, id int, queryTimestamp int64) (entity.Record, error){

	log.Println("Quering the DB to retrieve record with id: ", id)

	// Get the attributes of the record
	query := "select attributes, actual_update_timestamp, created_at from record_versions where record_id = ? and actual_update_timestamp < ? order by actual_update_timestamp desc limit 1"
	
	row := s.db.QueryRow(query, id, queryTimestamp)
	return s.GetRecordDetails(id, row)
}

// This is the helper method that get the details of a version of the record.
func (s *DBRecordService) GetRecordDetails(id int, row *sql.Row) (entity.Record, error){

	var attributesStr string
	var updatedTimestamp int64
	var createdAt int64
	err := row.Scan(&attributesStr, &updatedTimestamp, &createdAt)
	if err != nil {
		log.Println("The query failed on execution for id: ", id, " error: ", err)
		return entity.Record{}, ErrRecordDoesNotExist
	}

	// Infer the version number of the record.
	query := "select count(*) from record_versions where record_id = ? and actual_update_timestamp < ?"

	row = s.db.QueryRow(query, id, updatedTimestamp)

	var version int
	err = row.Scan(&version)
	if err != nil {
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
	record := entity.Record{ ID: id, Data: attributesMap, Version: version+1, UpdatedTimestamp: updatedTimestamp, ReportedTimestamp: createdAt}
	return record, nil

}

// Create a version of the record. The created_at time stores the reported timestamp where as actual_updated_timestamp
// stores the actual timestamp of the update.
func (s *DBRecordService) CreateRecord(ctx context.Context, record entity.Record) (entity.Record, error) {
	log.Println("Checking if a record with exists with id: ", record.ID)
	
	query := `select count(*) from records where id = ?`
	row := s.db.QueryRow(query, record.ID)

	count := 0
	err := row.Scan(&count)
	if err != nil {
		return entity.Record{}, err
	}

	if count != 0 {
		log.Println("Record exists with the ID:", record.ID, " exists in the DB. Please enter a valid ID.")
		return entity.Record{}, ErrRecordAlreadyExists
	}

	// Insert the row into Record and RecordVersion table in a trasaction.
	// To facilitate atomic update in both the Record and Record_Version table wrap the operations in a transaction.
	tx, err := s.db.Begin()
	if err != nil {
		return entity.Record{}, ErrRecordAlreadyExists
	}
	defer tx.Rollback()

	// If the record does not exist, add a record to the db.
	stmt := "insert into records (id, created_at) values (?, ?)"
	_, err = tx.Exec(stmt, record.ID, time.Now().Unix())
	if err != nil {
		return entity.Record{}, err
	}

	jsonData, err := json.Marshal(record.Data)
	if err != nil {
		return entity.Record{}, err
	}

	stmt = "insert into record_versions(attributes, actual_update_timestamp, record_id, created_at) values (?, ?, ?, ?)"

	createdTimestamp := time.Now().Unix()
	_, err = tx.Exec(stmt, jsonData, record.UpdatedTimestamp, record.ID, createdTimestamp)
	if err != nil {
		return entity.Record{}, err
	}

	// Complete the transaction
	tx.Commit()

	recordInDB := entity.Record{
		    ID: record.ID,
		    Version: 1,
		    UpdatedTimestamp: record.UpdatedTimestamp,
		    ReportedTimestamp: createdTimestamp,
		    Data: record.Data,
	}
	
	log.Println("Successfully added a record to the datbase with ID: ", record.ID)
	return recordInDB, nil
}

// Update a record if the record is present.
// The V1 of the api endpoint updates the latest version by creating a new record version at the table.
// The V2 version of the api endpoint creates a new record_version entry. It also applies the update to all
// record_version attributes that occur after the actual time of update.
// This ensures that the update is applied to all versions of the record after actual time of endorsement.
func (s *DBRecordService) UpdateRecord(ctx context.Context, id int, updatedTimestamp int64, updates map[string]*string) (entity.Record, error) {
	log.Println("Updating record with id: ", id, " in the database.")

	// Get the record at the updatedTimestamp.
	// For the v1 endpoints, this value from the callee is time.Now().Unix(): This ensures that all
	// the calls chronologically ascending.
	// For V2 endpoints, the updatedTimestamp represents the actual date of attribute update.
	record := entity.Record{}
	record, err := s.GetRecordAt(ctx, id, updatedTimestamp)
	if err != nil {
		return entity.Record{}, err
	}

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

	tx, err := s.db.Begin()
	if err != nil {
		return entity.Record{}, err
	}
	defer tx.Rollback()

	stmt := "insert into record_versions(attributes, actual_update_timestamp, record_id, created_at) values (?, ?, ?, ?)"
	_, err = tx.Exec(stmt, jsonData, updatedTimestamp, id, time.Now().Unix())

	if err != nil {
		return entity.Record{}, err
	}

	err = s.UpdateAllRecords(tx, id, updatedTimestamp, updates)
	if err != nil {
		return entity.Record{}, err
	}

	// Commit the transaction
	tx.Commit()
	
	log.Println("The update to the record with id: ", id, " is successfully completed.")
	record.UpdatedTimestamp = updatedTimestamp

	query := "select count(*) from record_versions where record_id = ? and actual_update_timestamp < ?"
	row := s.db.QueryRow(query, id, updatedTimestamp)

	var version int
	err = row.Scan(&version)
	
	record.Version = version + 1
	return record.Copy(), nil	
}

// Helper struct for record updates.
type RecordUpdates struct {
	Id       int
	Updates  map[string]string
}

// Apply the update to all the record_version after the actual time of the endorsement.
func (s *DBRecordService) UpdateAllRecords(tx *sql.Tx, id int, updatedTimestamp int64, updates map[string]*string) error {

	// Get the attributes of the record
	query := "select id, attributes from record_versions where record_id = ? and actual_update_timestamp > ?"
	
	rows, err := tx.Query(query, id, updatedTimestamp)
	if err != nil {
		return err
	}

	defer rows.Close()


	var updatesToPerform []RecordUpdates
	// Update all the records with the attribute updates that are made after the updatedTimestamp.
	for rows.Next() {

		var recordVersionId int
		var attributesStr string
		attributes := map[string]string{}

		rows.Scan(&recordVersionId, &attributesStr)

		jsonData := []byte(attributesStr)
		json.Unmarshal(jsonData, &attributes)

		for key, value := range updates {

			if value == nil {
				delete(attributes, key)
			} else {
				attributes[key] = *value
			}
		}

		updatedRecord := RecordUpdates { Id: recordVersionId, Updates: attributes }
		updatesToPerform = append(updatesToPerform, updatedRecord)
	}


	stmt := "update record_versions set attributes = ? where id = ?"
	for _, updatedRecord := range updatesToPerform {

		updatedJsonData, err := json.Marshal(updatedRecord.Updates)
		if err != nil {
			return err
		}

		_, err = tx.Exec(stmt, updatedJsonData, updatedRecord.Id)
		if err != nil {
			return err
		}
		
	}
	
	return nil
}

// Get all the versions of the record.
func (s *DBRecordService) GetVersions(ctx context.Context, id int) ([]entity.Record, error) {

	var records []entity.Record

	_, err := s.GetRecord(ctx, id)
	if err != nil {
		log.Println("The record with id: ", id, " could not be found.")
		return records, err
	}
	
	query := "select attributes, actual_update_timestamp, created_at from record_versions where record_id = ? order by actual_update_timestamp asc"
	rows, err := s.db.Query(query, id)
	if err != nil {
		log.Println("There was an error when quering the versions. Error: ", err)
		return records, err 
	}

	defer rows.Close()

	version := 1
	for rows.Next() {
		var record entity.Record
		var attributesStr string
		
		rows.Scan(&attributesStr, &record.UpdatedTimestamp, &record.ReportedTimestamp)

		jsonData := []byte(attributesStr)
		json.Unmarshal(jsonData, &record.Data)

		record.ID = id
		
		record.Version = version
		version = version + 1
		
		records = append(records, record)
	}

	return records, nil
}

// Get a specific version of the record.
func (s *DBRecordService) GetVersionedRecord(ctx context.Context, id int, version int) (entity.Record, error) {

	var record entity.Record

	query := "select attributes, actual_update_timestamp, created_at from record_versions where record_id = ? order by actual_update_timestamp asc limit 1 offset ?"

	row := s.db.QueryRow(query, id, version-1)
		
	var attributesStr string
	err := row.Scan(&attributesStr, &record.UpdatedTimestamp, &record.ReportedTimestamp)
	if err != nil {
		return record, err
	}

	jsonData :=[]byte(attributesStr)
	json.Unmarshal(jsonData, &record.Data)

	record.ID = id
	
	record.Version = version

	return record, nil
}
