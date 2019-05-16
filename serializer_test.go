package flux_test

import (
	"reflect"
	"testing"

	"github.com/galvital/flux"
	"github.com/stretchr/testify/assert"
)

type DummySerializerEvent struct {
	flux.EventModel
	Name  string
	Age   int
	Items []string
}

func TestNewSerializer(t *testing.T) {
	s := flux.NewSerializer(&DummySerializerEvent{})
	_ = s
}

func TestSerializer_ToRecord(t *testing.T) {
	id := "some-id"
	s := flux.NewSerializer(&DummySerializerEvent{})
	ev := &DummySerializerEvent{EventModel: flux.NewEvent(id), Name: "John", Age: 33, Items: []string{"one", "two", "three"}}
	rec, err := s.ToRecord(ev)
	assert.Nil(t, err, "unable to convert to record")
	assert.Equal(t, "DummySerializerEvent", rec.Type)
	assert.Equal(t, id, rec.AggregateID)
	assert.Equal(t, reflect.TypeOf(rec.Event), reflect.TypeOf(ev))
}

func TestSerializer_ToEvent(t *testing.T) {
	id := "some-id"
	s := flux.NewSerializer(&DummySerializerEvent{})
	rec := flux.Record{
		AggregateID: "some-id",
		Type:        "DummySerializerEvent",
		Data:        []byte(`{"ID": "some-id", "Name": "John", "Age": 33, "Items": ["one", "two", "three"]}`),
		Version:     17,
		Event:       nil,
	}
	event, err := s.ToEvent(rec)
	assert.Nil(t, err, "unable to convert to event")
	ev := event.(*DummySerializerEvent)
	assert.Equal(t, "John", ev.Name)
	assert.Equal(t, 33, ev.Age)
	assert.Equal(t, 3, len(ev.Items))
	assert.Equal(t, "two", ev.Items[1])
	assert.Equal(t, id, ev.AggregateID())
	assert.Equal(t, "DummySerializerEvent", reflect.TypeOf(ev).Elem().Name())
}
