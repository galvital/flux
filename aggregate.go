package flux

// Root returns a new RootModel.
func Root(id string) *RootModel {
	return &RootModel{
		id:      id,
		version: -1,
		changes: make([]Event, 0),
	}
}

// AggregateRoot (AR) represents a root entity of a ddd aggregate.
// AR is an Entity with additional (writing) abilities.
type AggregateRoot interface {

	// ID returns the id of the entity.
	ID() string

	// On mutates entity state by a given event.
	On(event Event)

	// GetVersion returns the current
	// version of the entity.
	GetVersion() int

	// SetVersion sets the version of the entity.
	SetVersion(int)

	// GetChanges returns a slice of events
	// that were applied since last commit.
	GetChanges() []Event

	// AddChange adds a change to AR changes.
	AddChange(Event)

	// CommitChanges clears the applied changes (events)
	// from memory, should be called after a successful save.
	CommitChanges()
}

// ApplyChange applies an event to an
// aggregate and adds it to changes list.
func ApplyChange(ar AggregateRoot, event Event) {
	ar.On(event)
	ar.AddChange(event)
}

// RootModel is an implementation of AggregateRoot,
// an optional helper for not needing to re-implement
// AggregateRoot repeatably.
// To be embedded in a concrete AR struct.
type RootModel struct {
	id      string
	version int
	changes []Event
}

// ID implements AggregateRoot.
func (ar *RootModel) ID() string {
	return ar.id
}

// SetVersion implements AggregateRoot.
func (ar *RootModel) SetVersion(v int) {
	ar.version = v
}

// On implements AggregateRoot.
func (ar *RootModel) On(event Event) {
	panic("NotImplemented")
}

// GetVersion implements AggregateRoot.
func (ar *RootModel) GetVersion() int {
	return ar.version
}

// GetChanges implements AggregateRoot.
func (ar *RootModel) GetChanges() []Event {
	return ar.changes
}

// AddChange implements AggregateRoot.
func (ar *RootModel) AddChange(event Event) {
	ar.changes = append(ar.changes, event)
}

// CommitChanges implements AggregateRoot.
func (ar *RootModel) CommitChanges() {
	ar.changes = make([]Event, 0)
}
