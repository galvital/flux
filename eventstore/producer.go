package eventstore

import (
	"encoding/json"
	"fmt"

	"github.com/galvital/flux"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer represents a kafka bus producer,
// implementing the EventPublisher interface.
type Producer struct {
	name     string
	producer *kafka.Producer
}

// NewProducer creates and returns a
// pointer to a new Producer instance.
func NewProducer(name string, url string) (*Producer, error) {

	// init confluent producer
	conf := &kafka.ConfigMap{"bootstrap.servers": url}
	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, err
	}

	fmt.Printf("[Producer] kafka producer initiated (%s).\n", name)
	return &Producer{
		name:     name,
		producer: p,
	}, nil
}

// Publish publishes an Event to a given topic.
func (p *Producer) Publish(r flux.Record, topic string) error {
	// Delivery report handler for produced messages
	go func() {
		for e := range p.producer.Events() {
			switch t := e.(type) {
			case *kafka.Message:
				if t.TopicPartition.Error != nil {
					fmt.Printf("[!] [%s] delivery failed ( %s => %s ).\n", p.name, r.Type, t.TopicPartition)
				} else {
					fmt.Printf("[>>] [%s] delivered ( %s => %s ).\n", p.name, r.Type, t.TopicPartition)
				}
			}
		}
	}()

	// encode record by:
	//	1. converting it to a map[string]interface{}
	// 	2. placing actual Event struct into map["data"]
	//	3. apply json.Marshal on map, and use as payload.
	rmap := make(map[string]interface{})
	rmap["type"] = r.Type
	rmap["version"] = r.Version
	rmap["aggregate_id"] = r.AggregateID
	rmap["data"] = r.Event
	payload, err := json.Marshal(rmap)
	if err != nil {
		return err
	}

	// Produce message to topic (asynchronously)
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}, nil)

	if err != nil {
		return err
	}

	// Wait for message deliveries before shutting down
	p.producer.Flush(5 * 1000)
	return nil
}
