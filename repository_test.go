package flux_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/galvital/flux"
	"github.com/pkg/errors"
)

type DummyRepoEvent struct {
	flux.EventModel
	Name string
}

type DummyRepoAggregateRoot struct {
	*flux.RootModel
	Name string
}

func (e *DummyRepoAggregateRoot) On(event flux.Event) {
	switch ev := event.(type) {
	case *DummyRepoEvent:
		e.Name = ev.Name
	default:
		panic(errors.New(flux.ErrUnhandledEvent))
	}
}

func getRepo(eventTypes ...flux.Event) *flux.Repository {
	store := flux.NewMemoryStore()
	sz := flux.NewSerializer(eventTypes...)
	return flux.NewRepository(store, sz)
}

func TestNotFound(t *testing.T) {
	id := "some-random-id"
	repo := getRepo()
	_, err := repo.Load(&DummyRepoAggregateRoot{RootModel: flux.Root(id)})
	assert.NotNil(t, err, "expected error")
	assert.Contains(t, err.Error(), flux.ErrAggregateNotFound, "expected ErrAggregateNotFound")
}

func TestRepository_Save(t *testing.T) {
	id := "some-random-id"
	ev1 := &DummyRepoEvent{EventModel: flux.NewEvent(id), Name: "first name"}
	ev2 := &DummyRepoEvent{EventModel: flux.NewEvent(id), Name: "second name"}
	repo := getRepo(&DummyRepoEvent{})
	ar := &DummyRepoAggregateRoot{RootModel: flux.Root(id)}
	flux.ApplyChange(ar, ev1)
	flux.ApplyChange(ar, ev2)
	err := repo.Save(ar, -1, "test")
	assert.Nil(t, err, "Save Error")
}

func TestRepository_SaveLoad(t *testing.T) {
	id := "some-random-id"
	ev1 := &DummyRepoEvent{EventModel: flux.NewEvent(id), Name: "first name"}
	ev2 := &DummyRepoEvent{EventModel: flux.NewEvent(id), Name: "second name"}
	repo := getRepo(&DummyRepoEvent{})
	ar := &DummyRepoAggregateRoot{RootModel: flux.Root(id)}
	flux.ApplyChange(ar, ev1)
	flux.ApplyChange(ar, ev2)
	err := repo.Save(ar, -1, "test")
	assert.Nil(t, err, "Save Error")
	//
	arLoad, err := repo.Load(&DummyRepoAggregateRoot{RootModel: flux.Root(id)})
	entity := arLoad.(*DummyRepoAggregateRoot)
	assert.Nil(t, err, "Load error")
	assert.Equal(t, "second name", entity.Name)
	assert.Equal(t, 0, len(entity.GetChanges()))
}
