package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/payloads"
	"time"

	"github.com/streadway/amqp"
)

var _ Consumer = new(RabbitMQConsumer)

type Consumer interface {
	CreateConsumer() error
	Read(ctx context.Context) error
	Connect() error
	CreateChannel() error
	Delete() error
	ReConnect() error
	FetchRecords() (map[string]interface{}, error)
	ExchangeDeclare() error
	QueueBind() error
	InitDeliveryChannel() error

	//Source 구현체에서 필요한 인터페이스
	Stream() chan interface{}
	PutPaylod(p payloads.Payload) error
	GetPaylod() payloads.Payload
}

type RabbitMQConsumer struct {
	config         *RabbitMQConsumerConfig
	conn           *amqp.Connection
	ConnMaxRetries int
	ch             *amqp.Channel
	q              amqp.Queue
	message        <-chan amqp.Delivery
	ctx            context.Context
	stream         chan interface{}
	errCh          chan error
}

//Copy RabbitMQConsumer instance
func (c *RabbitMQConsumer) Copy() *RabbitMQConsumer {
	return &RabbitMQConsumer{
		config: c.config,
		ctx:    c.ctx,
		stream: c.stream,
		errCh:  c.errCh,
	}
}

// Read implements Consumer
func (c *RabbitMQConsumer) Read(ctx context.Context) error {
	// TODO: 1주차 과제 구현

	//cCnsmr := c.Copy()

	// Kafka 예제와 유사하게 함수 분리 필요
	// 실제 데이터를 읽어오는 고루틴 생성
	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Debugf("shutting down consumer read")
				return
			default:
				records, err := c.FetchRecords()
				if err != nil {
					logger.Errorf("error fetching records : %v", err)
				}
				c.stream <- records
				data, _ := json.MarshalIndent(records, "", " ")
				logger.Debugf("%s", string(data))
			}
		}
	}()

	return nil
}

func NewRabbitMQConsumer(config jsonObj) *RabbitMQConsumer {
	// TODO: 1주차 과제 구현
	// Fix config ingestion like kafka example

	pipeParams, ok := config["pipeParams"].(jsonObj)
	if !ok {
		logger.Panicf("no pipeParams provided")
	}

	//extract context from config
	ctx, ok := pipeParams["context"].(context.Context)
	if !ok {
		logger.Panicf("no topic provided")
	}

	//extract stream chan from config
	stream, ok := pipeParams["stream"].(chan interface{})
	if !ok {
		logger.Panicf("no stream provided")
	}

	//extract error chan from config
	errch, ok := pipeParams["errch"].(chan error)
	if !ok {
		logger.Panicf("no errch provided")
	}

	// RabbitMQConsumerConfig 값을 담기 위한 오브젝트
	consumerCfgObj, ok := config["consumerCfg"].(jsonObj)
	if !ok {
		logger.Panicf("no consumer configuration provided")
	}

	var rmqCfg RabbitMQConsumerConfig
	cfgData, err := json.Marshal(consumerCfgObj)
	if err != nil {
		logger.Panicf("error in mashalling rabbitmq configuration: %v", err)
		return nil
	}

	err = json.Unmarshal(cfgData, &rmqCfg)
	if err != nil {
		logger.Panicf("error in loading rabbitmq configuration: %v", err)
		return nil
	}

	c := &RabbitMQConsumer{
		config: &rmqCfg,
		ctx:    ctx,
		stream: stream,
		errCh:  errch,
	}
	return c
}
func (c *RabbitMQConsumer) CreateConsumer() error {

	// best practice is to reuse connections and channels
	err := c.Connect()
	if err != nil {
		return err
	}
	err = c.CreateChannel()
	if err != nil {
		return err
	}
	logger.Debugf("Check in consumer creation: %v", c.ch)

	return nil
}

func (c *RabbitMQConsumer) Connect() error {
	c.ConnMaxRetries = -1
	retry := 0
	for {
		conn, err := amqp.Dial(c.config.Host)
		if conn != nil {
			logger.Infof("rabbitmq connection made")
			c.conn = conn
			break
		}
		retry++
		if c.ConnMaxRetries >= 0 && retry > c.ConnMaxRetries {
			return err
		}
		time.Sleep(1 * time.Second)
		logger.Infof("retried %v times to connect to rabbitmq", retry)
	}
	//Listen to NotifyClose
	go func() {
		<-c.conn.NotifyClose(make(chan *amqp.Error))
		c.errCh <- errors.New("rabbitmq connection closed")
	}()
	return nil
}

func (c *RabbitMQConsumer) CreateChannel() error {
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	c.ch = ch
	return nil
}

func (c *RabbitMQConsumer) Delete() error {
	logger.Debugf("deleting rabbit mq consumer connection: %s and channel: %s", c.conn, c.ch)
	if c.ch != nil {
		err := c.ch.Close()
		if err != nil {
			return err
		}
	}
	if c.conn != nil && !c.conn.IsClosed() {
		err := c.conn.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
func (c *RabbitMQConsumer) ReConnect() error {
	logger.Debugf("Reconnecting")
	err := c.Connect()
	if err != nil {
		return err
	}
	err = c.CreateChannel()
	if err != nil {
		return err
	}

	err = c.InitDeliveryChannel()
	if err != nil {
		return err
	}
	return nil

}

func (c *RabbitMQConsumer) FetchRecords() (map[string]interface{}, error) {
	var body map[string]interface{}

	// Check RabbitMQ Connection Error
	if c.conn == nil {
		err := c.ReConnect()
		if err != nil {
			return nil, err
		}
	}

	logger.Debugf("waiting letter")

	var letter amqp.Delivery
	select {
	case message := <-c.message:
		letter = message
	case <-c.ctx.Done():
		return nil, nil
	}
	logger.Debugf("Recevied Letter :%s", string(letter.Body))
	err := json.Unmarshal(letter.Body, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (c *RabbitMQConsumer) ExchangeDeclare() error {

	err := c.ch.ExchangeDeclare(
		c.config.ExchangeName, // name
		c.config.ExchangeType, // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		return err
	}
	return nil
}

func (c *RabbitMQConsumer) QueueDeclare() error {
	q, err := c.ch.QueueDeclare(
		c.config.QueueName, // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	c.q = q
	if err != nil {
		return err
	}
	return nil
}

func (c *RabbitMQConsumer) QueueBind() error {
	err := c.ch.QueueBind(
		c.config.QueueName,    // queue name
		c.config.RoutingKey,   // routing key
		c.config.ExchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}
	return nil
}

func (c *RabbitMQConsumer) InitDeliveryChannel() error {
	logger.Debugf(c.config.QueueName)

	msg, err := c.ch.Consume(
		c.config.QueueName, // queue
		"",                 // consumer
		true,               // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	if err != nil {
		return err
	}
	c.message = msg
	return nil
}

// GetPaylod implements Consumer
func (*RabbitMQConsumer) GetPaylod() payloads.Payload {
	panic("unimplemented")
}

// PutPaylod implements Consumer
func (*RabbitMQConsumer) PutPaylod(p payloads.Payload) error {
	panic("unimplemented")
}

// Stream implements Consumer
func (*RabbitMQConsumer) Stream() chan interface{} {
	panic("unimplemented")
}
