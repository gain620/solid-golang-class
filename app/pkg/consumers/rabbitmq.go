package consumers

import (
	"context"
	"event-data-pipeline/pkg/rabbitmq"
	"event-data-pipeline/pkg/sources"
)

// compile type assertion check
var _ Consumer = new(RabbitMQConsumerClient)
var _ ConsumerFactory = NewRabbitMQConsumerClient

// ConsumerFactory 에 rabbitmq 컨슈머를 등록
func init() {
	Register("rabbitmq", NewRabbitMQConsumerClient)
}

/*
type RabbitMQConsumerConfig struct {
	Host         string `json:"host,omitempty"`
	ExchangeName string `json:"exchange_name,omitempty"`
	ExchangeType string `json:"exchange_type,omitempty"`
	QueueName    string `json:"queue_name,omitempty"`
	RoutingKey   string `json:"routing_key,omitempty"`
}
*/

type RabbitMQConsumerClient struct {
	rabbitmq.Consumer
	sources.Source
}

func NewRabbitMQConsumerClient(config jsonObj) Consumer {
	//TODO: 1주차 과제 구현

	// RabbitMQConsumerConfig 값을 담기 위한 오브젝트
	rmqCnsmr := rabbitmq.NewRabbitMQConsumer(config)

	// create a new Consumer concrete type - RabbitMQConsumer
	client := &RabbitMQConsumerClient{
		Consumer: rmqCnsmr,
		Source:   sources.NewRabbitMQSource(rmqCnsmr),
	}
	return client
}

// Init implements Consumer
func (rc *RabbitMQConsumerClient) Init() error {
	//TODO: 1주차 과제 구현

	var err error

	// CreateChannel included
	err = rc.CreateConsumer()
	if err != nil {
		return err
	}

	// InitDeliveryChannel
	err = rc.InitDeliveryChannel()
	if err != nil {
		return err
	}

	return nil
}

// Consume implements Consumer
func (rc *RabbitMQConsumerClient) Consume(ctx context.Context) error {
	//TODO: 1주차 과제 구현

	err := rc.Read(ctx)
	if err != nil {
		return err
	}

	return nil
}
