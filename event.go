package flux

type Event interface {
	AggregateID() string
}

// NewEvent returns a new EventModel.
func NewEvent(id string) EventModel {
	return EventModel{ID: id}
}

// EventModel provides a default implementation of an Event
type EventModel struct {
	// id contains the AggregateID
	ID string
}

func (m EventModel) AggregateID() string {
	return m.ID
}
