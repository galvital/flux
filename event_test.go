package flux_test

import (
	"testing"

	"github.com/galvital/flux"
	"github.com/stretchr/testify/assert"
)

type SomethingHappend struct {
	flux.EventModel
}

func TestEventModel(t *testing.T) {
	id := "some-id"
	ev := SomethingHappend{EventModel: flux.EventModel{ID: id}}
	assert.Equal(t, ev.AggregateID(), id)
}
