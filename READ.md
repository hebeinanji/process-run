install

go get github.com/hebeinanji/process-run@latest

use

newProcess := process.ProcessRun{
ServiceList: []dataService.DataService{
//写自己的逻辑
},
Consumer:    []consumer.Consumer{
//todo
},
}
newProcess.Run()