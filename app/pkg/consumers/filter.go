package consumers

import (
	"context"
	"event-data-pipeline/pkg/logger"
	"reflect"
	"runtime"
)

//type (
//	jsonObj = map[string]interface{}
//	jsonArr = []interface{}
//)

// consumer interface
type Interceptor interface {
	////초기 작업
	//Initializer
	// 필터링 함수
	Filter(ctx context.Context) error
}

// 컨슈머 팩토리 함수
type FilterFunc func(testName string) error

// 컨슈머 팩토리 저장소
var filterList = make([]FilterFunc, 0, 5)

//var consumerFactories = make(map[string]ConsumerFactory)

// 컨슈머를 최초 등록하기 위한 함수
func RegisterFunc(filter FilterFunc) {
	logger.Debugf("Registering a filter function")
	if filter == nil {
		logger.Panicf("Filter function does not exist")
	}

	for _, f := range filterList {
		func1 := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
		func2 := runtime.FuncForPC(reflect.ValueOf(filter).Pointer()).Name()

		if func1 == func2 {
			logger.Errorf("Filter function already registered. Ignoring.")
		}
	}

	filterList = append(filterList, filter)
}

func GetFilterList() []FilterFunc {
	return filterList
}
