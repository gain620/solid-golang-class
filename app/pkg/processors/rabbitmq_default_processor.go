package processors

import (
	"context"
	"event-data-pipeline/pkg/payloads"
)

// 컴파일 타임 인터페이스 타입 체크
var _ Processor = new(RabbitMQDefaultProcessor)

func init() {
	// TODO: 2주차 과제 구현
	Register("rabbitmq_default", NewRabbitMQDefaultProcessor)
}

type RabbitMQDefaultProcessor struct {
	// TODO: 2주차 과제 구현
	Validator
	RabbitMQMetaInjector
}

func NewRabbitMQDefaultProcessor(config jsonObj) Processor {
	// TODO: 2주차 과제 구현
	return &RabbitMQDefaultProcessor{
		Validator{},
		RabbitMQMetaInjector{},
	}
}

func (r *RabbitMQDefaultProcessor) Process(ctx context.Context, p payloads.Payload) (payloads.Payload, error) {
	// TODO: 2주차 과제 구현
	err := r.Validate(ctx, p)
	if err != nil {
		return nil, err
	}
	//RabbitMQMetaInject method forwarding
	p, err = r.RabbitMQMetaInjector.Process(ctx, p)
	if err != nil {
		return nil, err
	}
	return p, nil
}
