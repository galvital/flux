package flux

import (
	"fmt"
)

// Service is the actual unit that handles events.
// it claims a message from the bus and reacts on it.
type Service struct {
	name       string
	repo       *Repository
	topics     []string
	eventTypes []Event // subscribed to
	factory    []Event
	handler    func(Event) error
}

func (s Service) Name() string {
	return s.name
}

func (s Service) Subscriptions() []Event {
	return s.eventTypes
}

type AppServiceOption func(*Service)

func WithName(name string) AppServiceOption {
	return func(s *Service) {
		s.name = name
	}
}

func WithRepository(repo *Repository) AppServiceOption {
	return func(s *Service) {
		s.repo = repo
	}
}

func WithTopics(topics ...string) AppServiceOption {
	return func(s *Service) {
		s.topics = topics
	}
}

func WithSubscription(eventTypes ...Event) AppServiceOption {
	return func(s *Service) {
		s.eventTypes = eventTypes
	}
}

func WithFactory(eventTypes ...Event) AppServiceOption {
	return func(s *Service) {
		s.factory = eventTypes
	}
}

func WithHandler(h func(Event) error) AppServiceOption {
	return func(s *Service) {
		s.handler = h
	}
}

func NewService(opts ...AppServiceOption) *Service {

	// apply options
	svc := &Service{}
	for _, opt := range opts {
		opt(svc)
	}

	// register handler
	svc.repo.store.RegisterHandler(svc.handler)

	// listen to topics
	svc.repo.store.ListenTo(svc.topics...)

	// subscribe to events
	err := svc.repo.store.Subscribe(svc.eventTypes...)
	if err != nil {
		panic(err)
	}

	fmt.Printf("[%s] Initiated (%d subscriptions on %d topics) ...\n", svc.name, len(svc.eventTypes), len(svc.topics))
	return svc
}

func (s *Service) Listen() {
	// start consuming from event bus
	fmt.Printf("[%s] consuming ...\n", s.name)
	err := s.repo.store.StartConsuming()
	if err != nil {
		panic(err)
	}
}

// Close attempts to close
// all service related resources.
func (s *Service) Close() error {
	fmt.Printf("[%s] closing ...\n", s.name)
	return s.repo.store.Close()
}

func (s *Service) Load(ar AggregateRoot) (AggregateRoot, error) {
	return s.repo.Load(ar)
}

func (s *Service) Save(ar AggregateRoot, topic string, occCheck bool) error {
	ver := ar.GetVersion()
	if !occCheck {
		ver = -2
	}
	return s.repo.Save(ar, ver, topic)
}

func (s Service) Repo() *Repository {
	return s.repo
}
