package main

import (
	"event-data-pipeline/pkg"
	"event-data-pipeline/pkg/consumers"
	"fmt"
	"github.com/common-nighthawk/go-figure"
	"log"
	"reflect"
	"runtime"
)

func main() {
	PrintLogo()
	//logger.Setup()
	//cfg := config.NewConfig()
	//http := server.NewHttpServer()
	//cmd.Run(*cfg, http)

	for _, f := range consumers.GetFilterList() {
		err := f("testsetset")
		if err != nil {
			log.Fatalf("err : %v", err)
		}
		funcName1 := runtime.FuncForPC(reflect.ValueOf(f).Pointer())
		fmt.Println(funcName1)
	}
}

func PrintLogo() {
	logo := figure.NewColorFigure("Youngstone", "", "green", true)
	logo.Print()
	class := figure.NewColorFigure("Week 5 - SOLID GO", "", "yellow", true)
	class.Print()
	ccssLite := figure.NewColorFigure("Event Data Pipeline", "", "blue", true)
	ccssLite.Print()
	version := figure.NewColorFigure(fmt.Sprintf("v%s", pkg.GetVersion()), "", "red", true)
	version.Print()
	fmt.Println()
}
