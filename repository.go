package flux

import (
	"fmt"

	"github.com/pkg/errors"
)

// NewRepository returns a new instance of a repository.
func NewRepository(store Store, serializer *Serializer) *Repository {
	return &Repository{
		store:      store,
		serializer: serializer,
	}
}

// Repository is where Entities / ARs are loaded and saved with.
// It's an abstraction layer providing actors (e.g services)
// an ability to load / save objects with an 'in-memory collection feeling',
// with actual plug-able store behind the abstraction.
type Repository struct {
	store      Store
	serializer *Serializer
}

// Load loads an AggregateRoot by its' type and ID.
func (r Repository) Load(ar AggregateRoot) (AggregateRoot, error) {

	// make sure there's an ID
	if ar.ID() == "" {
		return nil, errors.New("ErrMissingIdentity")
	}

	// load records from store,
	// return error if no records found.
	records, err := r.store.Load(ar.ID())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load ar, %v\n", ar.ID())
	}
	if len(records) == 0 {
		return nil, errors.Wrapf(errors.New(ErrAggregateNotFound), "AggregateNotFound: %v\n", ar.ID())
	}

	// instantiate AR, apply previous
	// events to get current state.
	for _, rec := range records {
		// record -> event
		event, err := r.serializer.ToEvent(rec)
		if err != nil {
			return nil, err
		}
		// apply event on ar
		ar.On(event)
	}

	// set ar version as last records' version.
	ar.SetVersion(records[len(records)-1].Version)
	return ar, nil
}

// Save stores an AggregateRoot and
// publishes its' changes to the event bus.
func (r Repository) Save(ar AggregateRoot, expectedVersion int, pubTopic string) error {
	events := ar.GetChanges()
	if len(events) == 0 {
		// nothing to save
		return nil
	}
	// events -> records
	records, err := r.getRecords(ar)
	if err != nil {
		return err
	}
	return r.store.Save(records, expectedVersion, pubTopic)
}

// GetMultipleItem returns a MultipleItem based on a given AR and other args.
// Usually used for SaveMultiple.
func (r Repository) GetMultipleItem(ar AggregateRoot, expectedVersion int, topic string) (MultipleItem, error) {
	records, err := r.getRecords(ar)
	if err != nil {
		return MultipleItem{}, err
	}
	return MultipleItem{
		Records:         records,
		ExpectedVersion: expectedVersion,
		PublishTopic:    topic,
	}, nil
}

// SaveMultiple saves multiple aggregates under a single db transaction.
// In general, should be used with a special reason / use-case.
// Mostly should use Save.
func (r Repository) SaveMultiple(items []MultipleItem) error {
	return r.store.SaveMultiple(items)
}

// getRecords returns slice of Records from aggregate changes (events).
func (r Repository) getRecords(ar AggregateRoot) ([]Record, error) {
	events := ar.GetChanges()
	if len(events) == 0 {
		// nothing to save
		return make([]Record, 0), nil
	}
	// events -> records
	records := make([]Record, 0)
	for _, ev := range events {
		// make sure each event IDs match AR ID.
		if ev.AggregateID() != ar.ID() {
			return nil, errors.New(fmt.Sprintf("aggregateID mismatch, want %v, got %v\n", ar.ID(), ev.AggregateID()))
		}
		// convert to record
		rec, err := r.serializer.ToRecord(ev)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}
