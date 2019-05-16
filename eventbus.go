package flux

// EventBus interface represents how the
// system expects an event bus to behave,
// no matter which provider it implements with.
type EventBus interface {

	// EventPublisher interface.
	EventPublisher

	// EventSubscriber interface.
	EventSubscriber
}

// EventPublisher publishes events to the bus.
type EventPublisher interface {
	// Publish publishes an event record to a given topic.
	Publish(Record, string) error
}

// EventSubscriber subscribes to events from the bus.
type EventSubscriber interface {
	// ListenTo sets the topics that
	// a subscriber is interested in.
	ListenTo(...string)

	// RegisterHandler lets the subscriber register
	// a callback method for when an event has
	// occurred, passing the event as context.
	RegisterHandler(h func(Event) error)

	// Subscribe subscribes to one or more
	// events so when it occurs - the handler
	// method of the subscriber will be invoked.
	Subscribe(...Event) error

	// StartConsuming enters the consumer loop,
	// should be called after all topics, events
	// and handler were registered via ListenTo,
	// Subscribe and RegisterHandler.
	StartConsuming() error
}
