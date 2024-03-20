package relational

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Config struct {
	SQL SQL
}

type SQL struct {
	DSN                 string `envconfig:"SQL_DSN"`
	MaxOpenConns        int64  `envconfig:"SQL_MAX_OPEN_CONNS"`
	MaxIdleConns        int64  `envconfig:"SQL_MAX_IDLE_CONNS"`
	MaxConnLifetimeMins int64  `envconfig:"SQL_MAX_CONN_LIFETIME_MINS"`
}

type Record struct {
	ID      string
	ERPItem map[string]interface{}
	Stock   map[string]interface{}
}

func OpenDB(cfg Config) (*sql.DB, error) {

	db, err := sql.Open("pgx", cfg.SQL.DSN)
	if err != nil {
		log.Fatalf("Unable to open SQL connection: %s", err)
	}

	db.SetConnMaxLifetime(time.Minute * time.Duration(cfg.SQL.MaxConnLifetimeMins))
	db.SetMaxOpenConns(int(cfg.SQL.MaxOpenConns))
	db.SetMaxIdleConns(int(cfg.SQL.MaxIdleConns))

	// ping the database
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func CreatePostgresTable(db *sql.DB) error {
	createStatement := `
	CREATE TABLE public.entities_msgpack (
		id varchar(36) NULL,
		erpitem bytea NULL,
		stock bytea NULL
	)
	`
	createIndex := `CREATE UNIQUE INDEX entities_msgpack_id_idx ON public.entities_msgpack USING btree (id);`

	_, err := db.Exec(createStatement)
	if err != nil {
		return fmt.Errorf("unable to create table: %s", err)
	}

	_, err = db.Exec(createIndex)
	if err != nil {
		return fmt.Errorf("unable to create index: %s", err)
	}

	return nil
}

func DropPostgresTable(db *sql.DB) error {
	dropStatement := `DROP TABLE public.entities_msgpack`
	_, err := db.Exec(dropStatement)
	if err != nil {
		return fmt.Errorf("unable to drop table: %s", err)
	}
	return nil
}

func InsertRecord(db *sql.DB, record Record) error {

	// marshal the maps to bytea
	erpitem, err := msgpack.Marshal(record.ERPItem)
	if err != nil {
		return fmt.Errorf("unable to marshal erpitem: %s", err)
	}
	stock, err := msgpack.Marshal(record.Stock)
	if err != nil {
		return fmt.Errorf("unable to marshal stock: %s", err)
	}

	insertStatement := `INSERT INTO public.entities_msgpack (id, erpitem, stock) VALUES ($1, $2, $3)`
	_, err = db.Exec(insertStatement, record.ID, erpitem, stock)
	if err != nil {
		return fmt.Errorf("unable to insert into table: %s", err)
	}
	return nil
}

func ReadRecords(db *sql.DB) ([]Record, error) {
	selectStatement := `SELECT id, erpitem, stock FROM public.entities_msgpack`
	rows, err := db.Query(selectStatement)
	if err != nil {
		return nil, fmt.Errorf("unable to select from table: %s", err)
	}
	defer rows.Close()

	records := []Record{}
	for rows.Next() {
		var sku string
		var erpitemB []byte
		var stockB []byte
		err = rows.Scan(&sku, &erpitemB, &stockB)
		if err != nil {
			return nil, fmt.Errorf("unable to scan row: %s", err)
		}

		// decode the bytea
		var erpitem map[string]interface{}
		var stock map[string]interface{}
		err = msgpack.Unmarshal(erpitemB, &erpitem)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal erpitem: %s", err)
		}
		err = msgpack.Unmarshal(stockB, &stock)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal stock: %s", err)
		}

		record := Record{
			ID:      sku,
			ERPItem: erpitem,
			Stock:   stock,
		}
		records = append(records, record)
	}

	return records, nil
}

func SelectPostgresTable(db *sql.DB, id string) (Record, error) {
	selectStatement := `SELECT id, erpitem, stock FROM public.entities_msgpack WHERE id = $1`
	var sku string
	var erpitemB []byte
	var stockB []byte
	err := db.QueryRow(selectStatement, id).Scan(&sku, &erpitemB, &stockB)
	if err != nil {
		return Record{}, fmt.Errorf("unable to select from table: %s", err)
	}

	// decode the bytea
	var erpitem map[string]interface{}
	var stock map[string]interface{}
	err = msgpack.Unmarshal(erpitemB, &erpitem)
	if err != nil {
		return Record{}, fmt.Errorf("unable to unmarshal erpitem: %s", err)
	}
	err = msgpack.Unmarshal(stockB, &stock)
	if err != nil {
		return Record{}, fmt.Errorf("unable to unmarshal stock: %s", err)
	}

	record := Record{
		ID:      sku,
		ERPItem: erpitem,
		Stock:   stock,
	}

	return record, nil
}

func CreateOrUpdate(db *sql.DB, ns string, id string, payload map[string]interface{}) (map[string]interface{}, bool, error) {

	encodedPayload, err := msgpack.Marshal(payload)
	if err != nil {
		return nil, false, err
	}

	_, err = db.Exec(
		fmt.Sprintf(`INSERT INTO entities_msgpack (id, %s) VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET %s = $2`, ns, ns),
		id,
		encodedPayload,
	)

	if err != nil {
		return nil, false, err
	}

	return payload, true, nil
}
