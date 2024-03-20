package main

import (
	"log"
	"log/slog"
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/theimes/persistence/intern/relational"
)

func main() {
	sqlConf := relational.SQL{
		DSN:                 "postgres://demo:demo@localhost:5432/demo?sslmode=disable",
		MaxOpenConns:        10,
		MaxIdleConns:        5,
		MaxConnLifetimeMins: 5,
	}
	cfg := relational.Config{
		SQL: sqlConf,
	}

	// open the database
	db, err := relational.OpenDB(cfg)
	if err != nil {
		log.Fatalf("Unable to open SQL connection: %s", err)
	}
	defer db.Close()

	// do something with the database
	err = relational.CreatePostgresTable(db)
	if err != nil {
		slog.Error("Unable to create table:", slog.String("error", err.Error()))
	} else {
		slog.Info("Table created successfully")
	}

	// insert some data
	records := getDataItems(10)
	for _, record := range records {
		err = relational.InsertRecord(db, record)
		if err != nil {
			slog.Error("Unable to insert record:", slog.String("error", err.Error()))
		}
	}

	// read the data
	records, err = relational.ReadRecords(db)
	if err != nil {
		slog.Error("Unable to read records:", slog.String("error", err.Error()))
	} else {
		slog.Info("Records read successfully")
	}
	for _, record := range records {
		slog.Info("Record", slog.String("id", record.ID), slog.Any("erpitem", record.ERPItem), slog.Any("stock", record.Stock))
	}

	// delete the data
	err = relational.DropPostgresTable(db)
	if err != nil {
		slog.Error("Unable to drop table:", slog.String("error", err.Error()))
	} else {
		slog.Info("Table dropped successfully")
	}

}

func getData(id string) relational.Record {
	// create dummy data
	record := relational.Record{
		ID: id,
		ERPItem: map[string]interface{}{
			"item": "item1",
			"qty":  10,
		},
		Stock: map[string]interface{}{
			"location": "loc1",
			"qty":      100,
		},
	}
	return record
}

func getDataItems(count int) []relational.Record {
	records := make([]relational.Record, count)
	for i := 0; i < count; i++ {
		records[i] = getData("id" + strconv.Itoa(i))
	}
	return records
}
