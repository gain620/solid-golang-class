package processors

import (
	"context"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/payloads"
	"time"
)

func init() {
	Register("rabbitmq_meta_injector", NewRabbitMQMetaInjector)
}

type RabbitMQMetaInjector struct {
	// TODO: 2주차 솔루션입니다.
}

func NewRabbitMQMetaInjector(config jsonObj) Processor {
	// TODO: 2주차 솔루션입니다.
	return &RabbitMQMetaInjector{}
}

// Process implements Processor
func (*RabbitMQMetaInjector) Process(ctx context.Context, p payloads.Payload) (payloads.Payload, error) {
	// TODO: 2주차 솔루션입니다.
	logger.Debugf("InjectMetaRabbitMQPayload processing...")
	rbbtPayload := p.(*payloads.RabbitMQPayload)

	meta := make(jsonObj)
	meta["data-processor-id"] = "rabbitmq-event-data-processor"
	meta["data-processor-timestamp"] = time.Now()
	meta["data-processor-env"] = "local"

	rbbtPayload.Value["meta"] = meta

	return rbbtPayload, nil

}
