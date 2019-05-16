package eventstore

import (
	"database/sql"
	"strings"

	"github.com/pkg/errors"
)

const (
	// CreateSQL provides sql to create the event source table
	CreateSQL = `
	CREATE TABLE IF NOT EXISTS ${TABLE} (
		num           SERIAL PRIMARY KEY,
		aggregate_id VARCHAR(255) NOT NULL,
		type		 VARCHAR(128) NOT NULL,
		data         TEXT,
		version      INT,
		at 			 TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
`
	// CheckIndexSQL provides sql to query db to determine whether the index exists
	CheckIndexSQL = `
	SELECT count(*)
	FROM pg_indexes
	WHERE schemaname = 'public'
	AND tablename  = '${TABLE}'
	AND indexname  = 'idx_${TABLE}';
`

	// CreateIndexSQL provides sql to create the index
	CreateIndexSQL = `
	CREATE UNIQUE INDEX idx_${TABLE}
	ON ${TABLE} (aggregate_id, version);
`
)

func expand(template, tableName string) string {
	return strings.Replace(template, `${TABLE}`, tableName, -1)
}

// CreateIfNotExists creates the specified table and index(es) in the db if they do not already exist
func CreateIfNotExists(db *sql.DB, tableName string) error {
	_, err := db.Exec(expand(CreateSQL, tableName))
	if err != nil {
		return errors.Wrap(err, "unable to create table")
	}

	row, err := db.Query(expand(CheckIndexSQL, tableName))
	if err != nil {
		return errors.Wrap(err, "query failed to determine if index exists")
	}

	row.Next()
	exists := 0
	err = row.Scan(&exists)
	if err != nil {
		return errors.Wrap(err, "unable to read response for whether index exists")
	}

	if exists > 0 {
		return nil
	}

	_, err = db.Exec(expand(CreateIndexSQL, tableName))
	if err != nil {
		return errors.Wrap(err, "unable to create index")
	}

	return err
}
