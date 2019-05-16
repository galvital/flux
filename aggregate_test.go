package flux_test

import (
	"testing"

	"github.com/galvital/flux"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type DummyEvent struct {
	flux.EventModel
	Name string
}

type DummyEntity struct {
	*flux.RootModel
	Name string
}

func (e *DummyEntity) On(event flux.Event) {
	switch ev := event.(type) {
	case *DummyEvent:
		e.Name = ev.Name
	default:
		panic(errors.New(flux.ErrUnhandledEvent))
	}
}

func TestAggregateRootModel(t *testing.T) {
	id := "some-id"
	event1 := &DummyEvent{EventModel: flux.NewEvent(id), Name: "first name"}
	event2 := &DummyEvent{EventModel: flux.NewEvent(id), Name: "second name"}
	entity := &DummyEntity{RootModel: flux.Root(id)}
	entity.On(event1)
	entity.On(event2)
	assert.Equal(t, "second name", entity.Name)
}

func TestAggregateRootModel_ApplyChange(t *testing.T) {
	id := "some-id"
	event1 := &DummyEvent{EventModel: flux.NewEvent(id), Name: "first name"}
	event2 := &DummyEvent{EventModel: flux.NewEvent(id), Name: "second name"}
	entity := &DummyEntity{RootModel: flux.Root(id)}
	flux.ApplyChange(entity, event1)
	flux.ApplyChange(entity, event2)
	assert.Equal(t, 2, len(entity.GetChanges()))
	assert.Equal(t, "second name", entity.Name)
}
