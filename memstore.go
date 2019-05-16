// ** for Testing usage Only! ***
package flux

import (
	"github.com/pkg/errors"
)

// NewMemoryStore returns a new instance of a memory store.
func NewMemoryStore() *MemStore {
	return &MemStore{
		records: make([]Record, 0),
	}
}

// MemStore is an in-memory implementation of Store.
type MemStore struct {
	records []Record
	// TODO: bus
}

// Load implements Store.
func (s *MemStore) Load(aggregateID string) ([]Record, error) {
	ret := make([]Record, 0)
	for _, rec := range s.records {
		if rec.AggregateID == aggregateID {
			ret = append(ret, rec)
		}
	}
	return ret, nil
}

// Save implements Store.
func (s *MemStore) Save(records []Record, expectedVersion int, topic string) error {
	if len(records) < 1 {
		return nil
	}
	aggID := records[0].AggregateID
	ver := -1
	for _, rec := range s.records {
		if rec.AggregateID == aggID {
			ver = rec.Version
		}
	}
	if expectedVersion == -2 {
		// -2 means never fail.
		expectedVersion = ver
	}
	if expectedVersion != ver {
		return errors.New(ErrOptimisticConcurrencyError)
	}
	recv := expectedVersion
	for _, rec := range records {
		recv++
		rec.Version = recv
		s.records = append(s.records, rec)
		_ = s.Publish(rec, topic)
	}
	//fmt.Printf("[ms] stored %v events\n", len(records))
	return nil
}

// SaveMultiple implements Store.
func (s *MemStore) SaveMultiple(items []MultipleItem) error {
	for _, item := range items {
		err := s.Save(item.Records, item.ExpectedVersion, item.PublishTopic)
		if err != nil {
			return err
		}
	}
	return nil
}

// Publish implements Store.
func (s *MemStore) Publish(record Record, topic string) error {
	// TODO
	return nil
}

// ListenTo implements Store.
func (s *MemStore) ListenTo(topics ...string) {
	// TODO
}

// Subscribe implements Store.
func (s *MemStore) Subscribe(events ...Event) error {
	// TODO
	return nil
}

// StartConsuming implements Store.
func (s *MemStore) StartConsuming() error {
	// TODO
	return nil
}

// RegisterHandler implements Store.
func (s *MemStore) RegisterHandler(h func(Event) error) {
	// TODO
}

// Close implements Store.
func (s *MemStore) Close() error {
	return nil
}
