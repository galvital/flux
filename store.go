package flux

// Record is a storage
// representation of an event.
type Record struct {
	AggregateID string `json:"aggregate_id"`
	Type        string `json:"type"`
	Data        []byte `json:"data"`
	Version     int    `json:"version"`
	Event       Event
}

// MultipleItem is used
// when using SaveMultiple.
type MultipleItem struct {
	Records         []Record
	ExpectedVersion int
	PublishTopic    string
}

// Store represents an eventstore.
type Store interface {
	EventBus
	Load(aggregateID string) ([]Record, error)
	Save(records []Record, expectedVersion int, topic string) error
	SaveMultiple(items []MultipleItem) error
	Close() error
}
