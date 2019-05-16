package eventstore

import (
	"fmt"
)

// KEventBus is a kafka based
// implementation of the bus.EventBus
// interface.
type KEventBus struct {
	*Producer        // producer instance
	*Consumer        // consumer instance
	url       string // broker(s) url
}

// NewKafkaEventBus creates and returns a
// pointer to a new kafka-based event bus.
func NewEventBus(svcName string, brokersUrl string) *KEventBus {

	// init producer
	p, err := NewProducer(svcName, brokersUrl)
	if err != nil {
		panic(err)
	}

	// init consumer
	c, err := NewConsumer(svcName, brokersUrl)
	if err != nil {
		panic(err)
	}

	return &KEventBus{
		Producer: p,
		Consumer: c,
		url:      brokersUrl,
	}
}

// Close closes producer and consumer connections.
// might return an error from consumer.Close
func (kb *KEventBus) Close() error {
	// close producer
	kb.producer.Close()
	fmt.Println("[KEventBus] kafka producer connection closed.")

	// try to close consumer
	err := kb.consumer.Close()
	if err != nil {
		fmt.Println("[!] error closing kafka consumer")
		return err
	}
	fmt.Println("[KEventBus] kafka consumer connection closed.")
	return nil
}
