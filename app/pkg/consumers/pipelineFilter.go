package consumers

import (
	"errors"
	"fmt"
)

// FilterList 에 필터 함수 등록
func init() {
	RegisterFunc(filterPipeline)
}

func filterPipeline(testName string) error {
	if testName == "err" {
		return errors.New("filter pipeline error")
	}

	fmt.Println(testName)
	return nil
}
