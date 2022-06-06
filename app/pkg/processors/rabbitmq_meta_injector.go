package processors

import (
	"context"
	"event-data-pipeline/pkg/payloads"
	"time"
)

func init() {
	// TODO: 2주차 과제 구현

	Register("rabbitmq_meta_injector", NewRabbitMQMetaInjector)
}

type RabbitMQMetaInjector struct {
	// TODO: 2주차 과제 구현
}

func NewRabbitMQMetaInjector(config jsonObj) Processor {
	// TODO: 2주차 과제 구현
	return &RabbitMQMetaInjector{}
}

// Process implements Processor
func (r *RabbitMQMetaInjector) Process(ctx context.Context, p payloads.Payload) (payloads.Payload, error) {
	// TODO: 2주차 과제 구현

	rmqPayload := p.(*payloads.RabbitMQPayload)

	meta := make(jsonObj)
	meta["data-processor-id"] = "rabbitmq-event-data-processor"
	meta["data-processor-timestamp"] = time.Now()
	meta["data-processor-env"] = "local"

	rmqPayload.Value["meta"] = meta

	return rmqPayload, nil
}
