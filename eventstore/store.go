package eventstore

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/galvital/flux"

	_ "github.com/lib/pq" // postgres driver
)

// EventStore is a postgres based
// implementation of the EventStore interface,
// using the kbus (kafka based) as
// an EventBus implementor.
type EventStore struct {
	*KEventBus
	db        *sql.DB
	tableName string
}

// NewEventStore returns a new es instance.
// argument evTypes is used for type-registering Event types.
func NewEventStore(svcName, connStr, brokerUrl string) *EventStore {
	// currently, we'll hard-code
	// 'events' as the table name
	// (one table with global ordering)
	tblName := "events"
	// create Event store with db
	// connection and type registry
	db := newDbConnection(connStr)
	//CreateIfNotExists(db, tblName)
	return &EventStore{
		KEventBus: NewEventBus(svcName, brokerUrl),
		db:        db,
		tableName: tblName,
	}
}

// LoadAggregate returns list of
// Event for a given aggregate id.
func (s *EventStore) Load(aggregateID string) ([]flux.Record, error) {
	// ensure valid db connection
	if s.db == nil {
		return nil, errors.New("no db connection")
	}

	// query for events and aggregate type by aggregate id
	rows, err := s.db.Query("select type, data, version from "+s.tableName+" where aggregate_id=$1 order by num asc", aggregateID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// prepare return value
	records := make([]flux.Record, 0)

	// iterate through results
	for rows.Next() {
		// scan row into fields
		// type, data, version
		record := flux.Record{}
		err := rows.Scan(&record.Type, &record.Data, &record.Version)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// Save saves new events for a given
// aggregate id and returns its new version, or an error.
// this method implements optimistic concurrency checking.
//
// this method will also publish the
// events to the event bus after a
// successful save.
func (s *EventStore) Save(records []flux.Record, expectedVersion int, topic string) error {

	// make sure there's at least 1 record.
	if len(records) == 0 {
		return nil
	}

	// make sure all records are of the same aggregate.
	aggregateID, err := getAggregateID(records)
	if err != nil {
		return err
	}

	// init db transaction
	tx, err := s.db.Begin()
	if err != nil {
		return errors.New(flux.ErrUnexpectedDbError)
	}

	// check if aggregate exists, and it's current version
	currentVersion, err := s.maxVersion(aggregateID)
	if err != nil {
		_ = tx.Rollback()
		fmt.Printf("[es] db error: %s\n", err)
		return errors.New(flux.ErrUnexpectedDbError)
	}

	if expectedVersion == -2 {
		// cheat ( -2 means 'never fail' )
		expectedVersion = currentVersion
		// TODO: How to check for idempotency in such case?
	}

	// validate expected version with actual version
	if expectedVersion != currentVersion {
		_ = tx.Rollback()

		if expectedVersion < currentVersion {
			// TODO: Idempotency check!
		}

		fmt.Printf("OptimisticConcurrencyError %d != %d\n", expectedVersion, currentVersion)
		return flux.NewError(errors.New(flux.ErrOptimisticConcurrencyError), flux.ErrOptimisticConcurrencyError,
			"want: %v, got: %v", expectedVersion, currentVersion)
	}

	vi := expectedVersion
	for i := 0; i < len(records); i++ {
		vi++
		r := records[i]
		r.Version = vi
		q := "insert into " + s.tableName + " (aggregate_id, type, data, version) values ($1, $2, $3, $4)"
		_, err := tx.Exec(q, r.AggregateID, r.Type, r.Data, r.Version)
		if err != nil {
			_ = tx.Rollback()
			fmt.Printf("[es] db insert error: %s\n", err)
			return errors.New(flux.ErrUnexpectedDbError)
		}
	}

	// all went well!
	log.Printf("%v events were appended to stream %v (v%v)", len(records), aggregateID, vi)
	err = tx.Commit()
	if err != nil {
		// but db commit failed :(
		_ = tx.Rollback()
		fmt.Printf("[es] db error: %s\n", err)
		return errors.New(flux.ErrUnexpectedDbError)
	}

	// publish to event bus
	// TODO: dispatcher - event.Dispatched ...
	for _, record := range records {
		err := s.Publish(record, topic)
		if err != nil {
			// error dispatching to event bus,
			// dispatcher must retry.
		} else {
			_, err := tx.Exec("update "+s.tableName+" set dispatched=true where aggregate_id=$1 and version=$2",
				record.AggregateID, record.Version)
			if err != nil {
				// if failed dispatcher will retry
			} else {
				log.Printf("[es] event %v:%v has been marked as dispatched.\n", record.AggregateID, record.Version)
			}
		}
	}

	// return the new aggregate version
	return nil
}

// SaveMultiple implements Store, saves multiple
// aggregates to eventstore under a single db transaction.
func (s *EventStore) SaveMultiple(items []flux.MultipleItem) error {

	// make sure each item has only record for a single aggregate id.
	for _, item := range items {
		if _, err := getAggregateID(item.Records); err != nil {
			return err
		}
	}

	// init db transaction
	tx, err := s.db.Begin()
	if err != nil {
		return errors.New(flux.ErrUnexpectedDbError)
	}

	// store items
	storedItems := make([]flux.MultipleItem, 0)
	for _, item := range items {

		// check if aggregate exists, and it's current version
		aggID, err := getAggregateID(item.Records)
		if err != nil {
			return err
		}

		currentVersion, err := s.maxVersion(aggID)
		if err != nil {
			_ = tx.Rollback()
			fmt.Printf("[es] db error: %s\n", err)
			return errors.New(flux.ErrUnexpectedDbError)
		}

		expectedVersion := item.ExpectedVersion
		if expectedVersion == -2 {
			// cheat ( -2 means 'never fail' )
			expectedVersion = currentVersion
			// TODO: How to check for idempotency in such case?
		}

		// validate expected version with actual version
		if expectedVersion != currentVersion {
			_ = tx.Rollback()
			if expectedVersion < currentVersion {
				// TODO: Idempotency check!
			}
			fmt.Printf("OptimisticConcurrencyError %d != %d\n", expectedVersion, currentVersion)
			return flux.NewError(errors.New(flux.ErrOptimisticConcurrencyError), flux.ErrOptimisticConcurrencyError,
				"want: %v, got: %v", expectedVersion, currentVersion)
		}

		vi := expectedVersion
		storedRecords := make([]flux.Record, 0)
		for _, r := range item.Records {
			vi++
			r.Version = vi
			inst := "insert into " + s.tableName + " (aggregate_id, type, data, version) values ($1, $2, $3, $4)"
			_, err := tx.Exec(inst, r.AggregateID, r.Type, r.Data, r.Version)
			if err != nil {
				_ = tx.Rollback()
				fmt.Printf("[es] db insert error: %s\n", err)
				return errors.New(flux.ErrUnexpectedDbError)
			}
			storedRecords = append(storedRecords, r)
		}
		storedItems = append(storedItems, flux.MultipleItem{Records: storedRecords, ExpectedVersion: vi, PublishTopic: item.PublishTopic})
		log.Printf("%v events were appended to stream %v (v%v)", len(item.Records), aggID, vi)
	}

	// all done, can now commit tx.
	err = tx.Commit()
	if err != nil {
		// db commit failed :(
		_ = tx.Rollback()
		fmt.Printf("[es] db commit error: %s\n", err)
		return errors.New(flux.ErrUnexpectedDbError)
	}

	// publish events
	for _, item := range storedItems {
		for _, record := range item.Records {
			err := s.Publish(record, item.PublishTopic)
			if err != nil {
				// error dispatching to event bus,
				// dispatcher must retry.
			} else {
				_, err := s.db.Exec("update "+s.tableName+" set dispatched=true where aggregate_id=$1 and version=$2",
					record.AggregateID, record.Version)
				if err != nil {
					// if failed dispatcher will retry
				} else {
					log.Printf("[es] event %v:%v has been marked as dispatched.\n", record.AggregateID, record.Version)
				}
			}
		}
	}

	fmt.Printf("[SaveMultiple] %v aggregates have been saved to store.\n", len(items))
	return nil
}

// Close attempts to closes
// store & bus connections.
// might return an error.
func (s *EventStore) Close() error {

	// close kafka connection
	err := s.KEventBus.Close()
	if err != nil {
		fmt.Println("[!] error closing kafka connection")
		return err
	}

	// close postgres connection
	err = s.db.Close()
	if err != nil {
		fmt.Println("[!] error closing postgres eventstore")
		return err
	}
	fmt.Println("[EventStore] db connection closed.")
	return nil
}

func (s *EventStore) maxVersion(aggregateID string) (int, error) {
	row, err := s.db.Query(expand("SELECT MAX(version) FROM ${TABLE} WHERE aggregate_id = $1", s.tableName), aggregateID)
	if err != nil {
		return 0, err
	}
	defer row.Close()

	maxVersion := 0
	if row.Next() {
		v := sql.NullInt64{}
		if err := row.Scan(&v); err != nil {
			return 0, err
		}
		maxVersion = int(v.Int64)
	}

	return maxVersion, nil
}

// newDbConnection creates and returns a new db connection.
func newDbConnection(connStr string) *sql.DB {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("[es] new db connection established.")
	return db
}

// getAggregateID gets the aggregate id from slice of records,
// returns an error if not all records are from the same aggregate id.
func getAggregateID(records []flux.Record) (string, error) {
	// make sure all records are of the same aggregate.
	aggregateID := ""
	for _, rec := range records {
		if aggregateID != "" && rec.AggregateID != aggregateID {
			return "", errors.New("multiple aggregates in records")
		}
		aggregateID = rec.AggregateID
	}
	return aggregateID, nil
}
