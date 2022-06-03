package consumers

import (
	"context"
	"event-data-pipeline/pkg/cli"
	"event-data-pipeline/pkg/config"
	"event-data-pipeline/pkg/logger"
	"github.com/alexflint/go-arg"
	"os"
	"testing"
)

func TestRabbitMQConsumerClient_Consume(t *testing.T) {
	//TODO: 1주차 과제 구현

	configPath := getCurDir() + "/test/consumers/rabbitmq.json"
	os.Setenv("EDP_ENABLE_DEBUG_LOGGING", "true")
	os.Setenv("EDP_CONFIG", configPath)
	os.Args = nil
	arg.MustParse(&cli.Args)
	logger.Setup()
	cfg := config.NewConfig()
	pipeCfgs := config.NewPipelineConfig(cfg.PipelineCfgsPath)

	ctx := context.TODO()
	stream := make(chan interface{})
	errCh := make(chan error)
	logger.Println(errCh)

	for _, cfg := range pipeCfgs {
		cfgParams := make(jsonObj)
		pipeParams := make(jsonObj)

		// context
		pipeParams["context"] = ctx
		pipeParams["stream"] = stream

		// consumer error channel
		errCh := make(chan error)
		pipeParams["errch"] = errCh

		cfgParams["pipeParams"] = pipeParams
		cfgParams["consumerCfg"] = cfg.Consumer.Config

		rmqCnsmr, err := CreateConsumer(cfg.Consumer.Name, cfgParams)
		if err != nil {
			t.Error(err)
		}

		err = rmqCnsmr.Init()
		if err != nil {
			t.Error(err)
		}
		//rmqCnsmr.Consume(context.TODO())
	}

	//for {
	//	select {
	//	case data := <-stream:
	//		t.Logf("data: %v", data)
	//		return
	//	case err := <-errCh:
	//		t.Logf("err: %v", err)
	//	default:
	//		//t.Logf("tick")
	//		time.Sleep(1 * time.Second)
	//	}
	//}

}
