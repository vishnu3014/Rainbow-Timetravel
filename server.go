package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rainbowmga/timetravel/api"
	"github.com/rainbowmga/timetravel/service"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pressly/goose/v3"

	"database/sql"
	"embed"
	"os"
)

// logError logs all non-nil errors
func logError(err error) {
	if err != nil {
		log.Printf("error: %v", err)
	}
}

func main() {
	
	db, err := initDB()
	if err != nil {
		log.Fatalf("The connection to the DB could not be established. Exiting the application..")
		return
	}
	
	router := mux.NewRouter()

	//service := service.NewInMemoryRecordService()
	service := service.NewDBRecordService(db)
	api := api.NewAPI(&service)

	apiRoute := router.PathPrefix("/api/v1").Subrouter()
	apiRoute.Path("/health").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		logError(err)
	})
	api.CreateRoutes(apiRoute)


	apiRouteV2 := router.PathPrefix("/api/v2").Subrouter()
	api.CreateRoutesV2(apiRouteV2)
	

	address := "127.0.0.1:8000"
	srv := &http.Server{
		Handler:      router,
		Addr:         address,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Printf("listening on %s", address)
	log.Fatal(srv.ListenAndServe())

	defer db.Close()
}

func initDB() (*sql.DB, error) {

	db, err := connectToDB()
	if err != nil {
		return nil, err
	}

	if err := performDBMigration(db); err != nil {
		return nil, err
	}

	return db, nil

}

func connectToDB() (*sql.DB, error) {

	dbName := "insurance_data.db"

	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatalf("The database could not be opened. Error: %v", err)
		return nil, err
	}
	log.Println("SQLite: The connection to sqlite has been established")

	if err := db.Ping(); err != nil {
		log.Fatalf("The ping to the database failed. Error: %v", err)
		return nil, err
	}
	log.Println("SQLite: The connection to the sqlite is responsive")

	return db, nil
}

//go:embed migrations/*.sql
var embedMigrations embed.FS

func performDBMigration(db *sql.DB) (error) {

	if err := goose.SetDialect("sqlite3"); err != nil {
		log.Fatal("SQL dialect could not be selected. Error: %v", err)
	}

	log.Println("SQLite: Initializing Goose for SQLite..")
	
	goose.SetLogger(log.New(os.Stdout, "goose: ", log.Lshortfile))
	log.Println("SQLite: Completed setting up the logger for SQLite DB")

	goose.SetBaseFS(embedMigrations)
	log.Println("SQLite: Getting ready to kick off SQL migrations")

	if err := goose.Up(db, "migrations"); err != nil {
		log.Fatalf("SQLite: The migrations failed to run. Error: %v", err)
		return err
	}

	log.Println("SQLite: The SQL migrations have been successfully completed !")
	return nil
}
