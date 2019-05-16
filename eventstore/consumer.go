package eventstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/galvital/flux"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer represents a kafka bus consumer.
type Consumer struct {
	name       string
	consumer   *kafka.Consumer
	listens    []string
	handles    []flux.Event
	handler    func(flux.Event) error
	serializer *flux.Serializer
}

// NewConsumer creates and returns a
// pointer to a new Consumer instance.
func NewConsumer(name string, url string) (*Consumer, error) {

	// prepare consumer config
	conf := &kafka.ConfigMap{
		"bootstrap.servers":  url,
		"group.id":           name,
		"enable.auto.commit": false,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "smallest", //"earliest",
		"default.topic.config": kafka.ConfigMap{
			"auto.offset.reset": "earliest",
		},
	}

	// init confluent consumer
	c, err := kafka.NewConsumer(conf)
	if err != nil {
		return nil, err
	}

	fmt.Printf("[Consumer] kafka consumer initiated (%s).\n", name)
	return &Consumer{
		name:       name,
		consumer:   c,
		listens:    make([]string, 0),
		handles:    make([]flux.Event, 0),
		serializer: flux.NewSerializer(),
	}, nil
}

// ListenTo sets the topics that
// a subscriber is interested in.
func (c *Consumer) ListenTo(topics ...string) {
	c.listens = topics
}

// Subscribe subscribes to an event so
// when it occurs - the Handle method
// of the subscriber will be invoked.
func (c *Consumer) Subscribe(events ...flux.Event) error {
	for _, ev := range events {
		for _, h := range c.handles {
			if h == ev {
				return ErrAlreadySubscribed
			}
		}
		c.handles = append(c.handles, ev)
	}
	c.serializer.Bind(events...)
	return nil
}

// IsSubscribed returns true if a
// consumer is registered to a
// given event, else false.
func (c *Consumer) IsSubscribed(eventType string) bool {
	for _, et := range c.handlesStr() {
		if et == eventType {
			return true
		}
	}
	return false
}

// RegisterHandler lets the subscriber register
// a callback method for when an event has
// occurred, passing the event as context.
//
// if an error is returned by this function,
// the event is ignored, otherwise it'll be committed.
//
// if a handler is already set it'll replace it silently.
func (c *Consumer) RegisterHandler(h func(event flux.Event) error) {
	c.handler = h
}

// Commit commits a consumed message
// offset to kafka after a successful
// handling of it.
func (c *Consumer) Commit(msg *kafka.Message) error {
	_, err := c.consumer.CommitMessage(msg)
	if err != nil {
		fmt.Printf("[!] [%s] error committing message (%s)\n", c.name, err)
		return err
	} else {
		// committed successfully
		fmt.Printf("[%s] Ack ( %s )\n", c.name, msg.TopicPartition)
	}
	return nil
}

// StartConsuming enters the consumer loop,
// should be called after all topics, events
// and handler were registered via ListenTo,
// Subscribe and RegisterHandler.
func (c *Consumer) StartConsuming() error {

	// subscribe to topics
	err := c.consumer.SubscribeTopics(c.listens, nil)
	if err != nil {
		fmt.Printf("[!] [%s] [StartConsuming] SubscribeTopics failed, %v.\n", c.name, c.listens)
		return err
	}
	fmt.Printf("[%s] subscribed to topics: %s\n", c.name, c.listens)

	// enter infinite loop.
	for {
		// tick handles
		// the polling.
		err := tick(c)
		if err != nil {
			if err.Error() == "continue" {
				continue
			} else {
				panic(err)
			}
		}
	}
}

// tick represents a single
// iteration of the consumer
// polling infinite loop.
func tick(c *Consumer) error {
	// poll for next message
	msg, err := c.consumer.ReadMessage(-1)
	// ReadMessage blocks until a
	// new message (or error) arrives
	if err == nil {

		// new message incoming
		//fmt.Printf("[<<] [%s] new messasge on %s ...\n", c.name, msg.TopicPartition)

		// convert payload to a Record
		record, err := buildRecord(msg.Value)
		if err != nil {
			return err
		}

		// check if this event is interesting (is subscribed?)
		if !c.IsSubscribed(record.Type) {
			// commit message
			//fmt.Printf("[%s] Skipping unsubscribed event `%s` ...\n", c.name, record.Type)
			err := c.Commit(msg)
			if err != nil {
				return flux.NewError(err, flux.ErrConsumerError, "unable to commit message")
			}
			return errors.New("continue")
		}

		// Record -> Event
		event, err := c.serializer.ToEvent(record)
		if err != nil {
			return flux.NewError(err, flux.ErrInvalidEncoding, "unable to convert record to event")
		}
		typeName, _ := flux.EventType(event)

		// invoke the event handler
		// passing the DomainEvent.
		fmt.Printf("[%s] Handling `%s` ...\n", c.name, typeName)
		t0 := time.Now()
		err = c.handler(event)
		if err != nil {
			// an error occurred on
			// the subscriber scope.
			// not committing message.
			fmt.Printf("[!] [%s] handler error: %v (%v)\n", c.name, err, msg)
			return flux.NewError(err, flux.ErrEventHandlerError, "event handler error")
		}
		//
		t1 := time.Now()
		fmt.Printf("[ total %vms ]\n", t1.Sub(t0).Nanoseconds()/int64(time.Millisecond))
		//
		// handled successfully,
		// committing message.
		err = c.Commit(msg)
		if err != nil {
			return flux.NewError(err, flux.ErrConsumerError, "unable to commit message")
		}
	} else {
		// The client will automatically
		// try to recover from all errors.
		fmt.Printf("[!] [%s] consumer error: %v (%v)\n", c.name, err, msg)
		fmt.Printf("[!] [%s] trying to recover ...\n", c.name)
	}
	return nil
}

func (c Consumer) handlesStr() []string {
	ret := make([]string, 0)
	for _, h := range c.handles {
		n, _ := flux.EventType(h)
		ret = append(ret, n)
	}
	return ret
}

func buildRecord(fromData []byte) (flux.Record, error) {
	// convert []byte to map
	var payload map[string]interface{}
	err := json.Unmarshal(fromData, &payload)
	if err != nil {
		return flux.Record{}, flux.NewError(err, flux.ErrInvalidEncoding, "unable to decode message\n(%v)", string(fromData))
	}

	// make sure map has required fields
	for _, field := range []string{"aggregate_id", "type", "data", "version"} {
		_, ok := payload[field]
		if !ok {
			return flux.Record{}, flux.NewError(err, flux.ErrInvalidEncoding, "missing required field, %v", field)
		}
	}

	// payload["data"] is map[string]interface{}
	payloadBytes, err := json.Marshal(payload["data"])
	if err != nil {
		return flux.Record{}, flux.NewError(err, flux.ErrInvalidEncoding, "unable to marshal payload")
	}

	// create record
	rec := flux.Record{
		AggregateID: payload["aggregate_id"].(string),
		Type:        payload["type"].(string),
		Version:     int(payload["version"].(float64)),
		Data:        payloadBytes,
	}
	return rec, nil
}

// ErrAlreadySubscribed is returned when
// attempting to Subscribe to an already
// subscribed event.
var ErrAlreadySubscribed = errors.New("already subscribed to event")
