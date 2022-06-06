package processors_test

import (
	"context"
	"encoding/json"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/payloads"
	"event-data-pipeline/pkg/pipelines"
	"event-data-pipeline/pkg/processors"
	"os"
	"testing"
)

func TestRabbitMQDefaultProcessor_Process(t *testing.T) {
	// TODO: 2주차 과제 구현

	type args struct {
		desc          string
		processorName string
		payload       payloads.Payload
		testcase      string
	}

	tests := []struct {
		name    string
		args    args
		want    payloads.Payload
		wantErr bool
	}{
		// TODO: Add test cases.

		{
			name: "RabbitMQ Test 1",
			args: args{
				desc:          "valid payload",
				processorName: "rabbitmq_default",
				payload:       &payloads.RabbitMQPayload{Value: make(jsonObj)},
				testcase:      "valid",
			},
			want: &payloads.RabbitMQPayload{Value: make(jsonObj)},
			//wantErr: false,
		},
		{
			name: "RabbitMQ Test 2",
			args: args{
				desc:          "invalid payload",
				processorName: "rabbitmq_default",
				payload:       nil,
				testcase:      "invalid",
			},
			want: &payloads.RabbitMQPayload{Value: make(jsonObj)},
			//wantErr: false,
		},
	}

	os.Args = nil
	os.Setenv("EDP_ENABLE_DEBUG_LOGGING", "true")
	logger.Setup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 프로세서 생성
			rp, err := processors.CreateProcessor(tt.args.processorName, nil)
			if err != nil {
				t.Error(err)
			}
			// 스테이지 러너 생성
			fifo := pipelines.FIFO(rp)

			// 파라미터 생성
			ctx, cancelFunc := context.WithCancel(context.Background())

			// Stage 개수는 1개로 고정
			// 채널 개수는 Stage 개수 +1
			stageCh := make([]chan payloads.Payload, 1+1)
			// 에러채널 개수는 Stage 개수 +2
			errCh := make(chan error, 1+2)
			for i := 0; i < len(stageCh); i++ {
				stageCh[i] = make(chan payloads.Payload)
			}

			// FiFO Stage Runner 에게 넘길 파라미터 생성
			wp := &workerParams{
				stage: 0,
				inCh:  stageCh[0],
				outCh: stageCh[1],
				errCh: errCh,
			}
			// StageRunner 구현체 FIFO 실행
			// Goroutine 으로 실행
			go fifo.Run(ctx, wp)

			stageCh[0] <- tt.args.payload

			for {
				select {
				case err := <-errCh:
					if tt.args.testcase == "invalid" {
						t.Log(err.Error())
					} else {
						t.Errorf(err.Error())
					}
					cancelFunc()
					return
				case msg := <-stageCh[1]:
					data, _ := json.MarshalIndent(msg, "", " ")
					t.Log(string(data))
					cancelFunc()
					return
				}
			}

			//k := &processors.RabbitMQDefaultProcessor{}
			//got, err := k.Process(tt.args.ctx, tt.args.p)
			//if (err != nil) != tt.wantErr {
			//	t.Errorf("Process() error = %v, wantErr %v", err, tt.wantErr)
			//	return
			//}
			//if !reflect.DeepEqual(got, tt.want) {
			//	t.Errorf("Process() got = %v, want %v", got, tt.want)
			//}
		})
	}
}
