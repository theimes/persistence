package nosql

import (
	"fmt"

	"github.com/gocql/gocql"
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

type Cassandra struct {
	session *gocql.Session
}

func OpenDB(cfg Config) (*Cassandra, error) {

	cluster := gocql.NewCluster("127.0.0.1")

	cluster.Keyspace = "demo"

	// create a session
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("unable to create session: %s", err)
	}
	return &Cassandra{
		session: session,
	}, nil
}

func (c *Cassandra) CreateCassandraTable() error {

	return fmt.Errorf("not implemented")
}

func (c *Cassandra) DropTable() error {

	return fmt.Errorf("not implemented")
}

func (c *Cassandra) ReadRecords() ([]Record, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *Cassandra) SelectTable(id string) (Record, error) {

	return Record{}, fmt.Errorf("not implemented")
}

func (c *Cassandra) CreateOrUpdate(ns string, id string, payload map[string]interface{}) (map[string]interface{}, bool, error) {

	return nil, false, fmt.Errorf("not implemented")
}
