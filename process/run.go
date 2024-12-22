package process

import (
	"context"
	"fmt"
	"github.com/hebeinanji/process-run/consumer"
	"github.com/hebeinanji/process-run/dataService"
	"github.com/hebeinanji/process-run/task"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"
)

const (
	UNSTART  = 0
	RUNNING  = 1
	CLOSEING = 2
	CLOSED   = 3
)

type ProcessRun struct {
	TaskList            []task.Task
	ServiceList         []dataService.DataService
	Consumer            []consumer.Consumer
	RunningServiceList  RunningServiceMap
	RunningConsumerList RunningConsumerMap
	signalChan          chan os.Signal
	status              int8
}

type RunningConsumerMap struct {
	rw           sync.RWMutex
	ConsumerList map[string]consumer.Consumer
}

type RunningServiceMap struct {
	rw          sync.RWMutex
	ServiceList map[string]dataService.DataService
}

func (p *ProcessRun) Run() {
	fmt.Println("ProcessRun run start...")
	p.RunningServiceList = RunningServiceMap{
		ServiceList: make(map[string]dataService.DataService, len(p.ServiceList)),
	}
	p.RunningConsumerList = RunningConsumerMap{
		ConsumerList: make(map[string]consumer.Consumer, len(p.Consumer)),
	}
	p.signalChan = make(chan os.Signal, 1)
	p.status = RUNNING
	go runService(p)
	go runConsumer(p)
	go listenShutdown(p)
}

func runService(p *ProcessRun) {
	fmt.Println("ProcessRun runService start...")
	for _, s := range p.ServiceList {
		go p.dataServiceRun(s)
	}
}

func runConsumer(p *ProcessRun) {
	fmt.Println("ProcessRun runConsumer start...")
	for _, c := range p.Consumer {
		go p.consumerRun(c)
	}
}

func listenShutdown(p *ProcessRun) {
	fmt.Println("Process listenShutdown start...")
	signal.Notify(p.signalChan, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	signalStr := <-p.signalChan
	p.status = CLOSEING
	fmt.Println("Process listenShutdown receive", signalStr)
	p.status = CLOSED
}

func (p *ProcessRun) ShutdownHook() func() error {
	return func() error {
		return pShutdown(p)
	}
}

func pShutdown(p *ProcessRun) error {
	fmt.Println("Process pShutdown start...")
	startTime := time.Now().Unix()
	for len(p.RunningServiceList.ServiceList) > 0 {
		if time.Now().Unix()-startTime > 180 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if len(p.RunningServiceList.ServiceList) > 0 {
		fmt.Printf("Process RunningServicceList running len[%+#v]", len(p.RunningServiceList.ServiceList))
		fmt.Println()
		newCtx := context.Background()
		for sName, s := range p.RunningServiceList.ServiceList {
			p.RunningServiceList.rw.Lock()
			s.FinishRun(newCtx)
			delete(p.RunningServiceList.ServiceList, sName)
			p.RunningServiceList.rw.Unlock()
		}
	}
	for len(p.RunningConsumerList.ConsumerList) > 0 {
		if time.Now().Unix()-startTime > 180 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("Process RunningConsumerList running len[%+#v]", len(p.RunningConsumerList.ConsumerList))
	fmt.Println()
	return nil
}

func (p *ProcessRun) dataServiceRun(s dataService.DataService) {
	fmt.Printf("dataServiceRun start--%T", s)
	fmt.Print()
	defer func() {
		if runErr := recover(); runErr != nil {
			fmt.Println(runErr)
		}
		ctx := context.Background()
		s.FinishRun(ctx)
	}()
	for p.status == RUNNING {
		ctx := context.Background()
		canRun := s.CanRun(ctx)
		if !canRun {
			continue
		}
		sName := reflect.ValueOf(s).Elem().FieldByName("Name").String()
		p.RunningServiceList.rw.Lock()
		p.RunningServiceList.ServiceList[sName] = s
		p.RunningServiceList.rw.Unlock()
		s.Run(ctx)
		s.FinishRun(ctx)
		p.RunningServiceList.rw.Lock()
		delete(p.RunningServiceList.ServiceList, sName)
		p.RunningServiceList.rw.Unlock()
	}
}

func (p *ProcessRun) consumerRun(c consumer.Consumer) {
	defer func() {
		if runErr := recover(); runErr != nil {
			fmt.Printf("consumerRun start fail %T", c)
			fmt.Print()
		}
	}()
	for p.status == RUNNING {
		cName := reflect.ValueOf(c).Elem().FieldByName("Name").String()
		p.RunningConsumerList.rw.Lock()
		p.RunningConsumerList.ConsumerList[cName] = c
		p.RunningConsumerList.rw.Unlock()
		ctx := context.Background()
		c.RunConsumer(ctx)
		p.RunningConsumerList.rw.Lock()
		delete(p.RunningConsumerList.ConsumerList, cName)
		p.RunningConsumerList.rw.Unlock()
	}
}
