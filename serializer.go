package flux

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// NewSerializer returns an instance of a new serializer.
func NewSerializer(events ...Event) *Serializer {
	serializer := &Serializer{
		eventTypes: map[string]reflect.Type{},
	}
	serializer.Bind(events...)
	return serializer
}

// Serializer converts events to records, and records to events.
// before save: events -> records
// after load: records -> events
type Serializer struct {
	eventTypes map[string]reflect.Type
}

// Bind registers one or more events with the serializer.
// Only bound events can be encoded / decoded.
func (s *Serializer) Bind(events ...Event) {
	for _, event := range events {
		typeName, t := EventType(event)
		s.eventTypes[typeName] = t
	}
}

// ToRecord converts an event to a record.
func (s *Serializer) ToRecord(ev Event) (Record, error) {
	data, err := json.Marshal(ev)
	if err != nil {
		return Record{}, NewError(err, ErrInvalidEncoding, "unable to encode event")
	}
	typeName, _ := EventType(ev)
	return Record{
		AggregateID: ev.AggregateID(),
		Type:        typeName,
		Data:        data,
		Event:       ev,
	}, nil
}

// ToEvent converts a record to an event.
func (s *Serializer) ToEvent(r Record) (Event, error) {
	t, ok := s.eventTypes[r.Type]
	if !ok {
		return nil, errors.New(fmt.Sprintf("unbound event type, %v", r.Type))
	}
	evPtr := reflect.New(t).Interface()
	err := json.Unmarshal(r.Data, evPtr)
	if err != nil {
		return nil, NewError(err, ErrInvalidEncoding, "unable to unmarshal event data into %v (%v) \n(%v)\n", evPtr, err, r.Data)
	}
	event := evPtr.(Event)
	return event, nil
}
