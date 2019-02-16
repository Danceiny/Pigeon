package core

import (
	"fmt"
	"github.com/Danceiny/gocelery"
	"log"
	"os"
	"os/exec"
	"strconv"
)

var CELERY_BROKER_HOST = "127.0.0.1"
var CELERY_BROKER_PORT = 6379
var CELERY_BROKER_PASSWORD = ""
var CELERY_BACKEND_HOST = "127.0.0.1"
var CELERY_BACKEND_PORT = 6379
var CELERY_BACKEND_PASSWORD = ""

func init() {
	var tmp string
	var b bool
	if tmp, b = os.LookupEnv("CELERY_BROKER_HOST"); b {
		CELERY_BROKER_HOST = tmp
	}
	if tmp, b = os.LookupEnv("CELERY_BROKER_PORT"); b {
		CELERY_BROKER_PORT, _ = strconv.Atoi(tmp)
	}
	if tmp, b = os.LookupEnv("CELERY_BROKER_PASSWORD"); b {
		CELERY_BROKER_PASSWORD = tmp
	}
	if tmp, b = os.LookupEnv("CELERY_BACKEND_HOST"); b {
		CELERY_BACKEND_HOST = tmp
	}
	if tmp, b = os.LookupEnv("CELERY_BACKEND_PORT"); b {
		CELERY_BACKEND_PORT, _ = strconv.Atoi(tmp)
	}
	if tmp, b = os.LookupEnv("CELERY_BACKEND_PASSWORD"); b {
		CELERY_BACKEND_PASSWORD = tmp
	}
}

type Task struct {
	Id     int64
	Cmd    string
	Env    *Env
	DataId int64
	/**
	  竞价相关
	*/
	PredictedDuration       int64
	ExpectedPriceWindow     [2]int64
	ExpectedStartTimeWindow [2]int64
}

/**
定义一套运行时环境
*/
type Env struct {
	Name       string
	Dockerfile string
	Cpu        int8  /* core num */
	Mem        int64 /* MB */
	Gpu        int8  /* core num */
}
type TaskReturn struct {
}
type TaskStat struct {
	Performance TaskPerf
}

/**
Performance 性能
*/
type TaskPerf struct {
}

func (task *Task) RunTask() (interface{}, error) {
	var argv = []string{task.Cmd}
	binary, lookErr := exec.LookPath("echo")
	// binary, lookErr := exec.LookPath("marx")
	if lookErr != nil {
		panic(lookErr)
	}
	log.Printf("cmd: %s %s", binary, task.Cmd)
	var cmd = exec.Command(binary, argv...)
	if err := cmd.Run(); err != nil {
		log.Fatalf("run failed: %s", err.Error())
	}
	return true, nil
}

/**
kwargs -> Task的字典（k-v）形式
*/
func (task *Task) ParseKwargs(kwargs map[string]interface{}) error {
	cmd, ok := kwargs["cmd"]
	if !ok {
		return fmt.Errorf("undefined kwarg cmd")
	}
	task.Cmd = cmd.(string)
	return nil
}
func Add(a, b int) int {
	return a + b
}

func Start() {
	// create broker and backend
	celeryBroker := gocelery.NewRedisCeleryBroker(CELERY_BROKER_HOST, CELERY_BROKER_PORT, 0, CELERY_BROKER_PASSWORD)
	celeryBackend := gocelery.NewRedisCeleryBackend(CELERY_BACKEND_HOST, CELERY_BACKEND_PORT, 0, CELERY_BACKEND_PASSWORD)

	// Configure with 2 celery workers
	celeryServer, _ := gocelery.NewCeleryServer(celeryBroker, celeryBackend, 10)

	// worker.add name reflects "add" task method found in "worker.py"
	// this worker uses args
	celeryServer.Register("runTask", &Task{})
	celeryServer.Register("add", Add)
	celeryServer.StartWorker()
}
