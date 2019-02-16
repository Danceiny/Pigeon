package main

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Danceiny/Pigeon/core"
	"github.com/Danceiny/gocelery"
)

func main() {
	// create broker and backend
	celeryBroker := gocelery.NewRedisCeleryBroker(core.CELERY_BROKER_HOST,
		core.CELERY_BROKER_PORT, 0, core.CELERY_BROKER_PASSWORD)
	celeryBackend := gocelery.NewRedisCeleryBackend(core.CELERY_BACKEND_HOST,
		core.CELERY_BACKEND_PORT, 0, core.CELERY_BACKEND_PASSWORD)

	// create client
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker, celeryBackend)

	var asyncResult, err = celeryClient.Delay("add", 1, 2)
	// check if result is ready
	isReady, _ := asyncResult.Ready()
	fmt.Printf("Ready status: %v\n", isReady)

	asyncResult, err = celeryClient.DelayKwargs("runTask", map[string]interface{}{
		"cmd": "ls",
	})
	if err != nil {
		panic(err)
	}

	// check if result is ready
	isReady, _ = asyncResult.Ready()
	fmt.Printf("Ready status: %v\n", isReady)

	// get result with 1s timeout
	res2, err := asyncResult.Get(10 * time.Second)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Result: %v of type: %v\n", res2, reflect.TypeOf(res2))
	}
}
