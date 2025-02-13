package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool
	GetFileIndex() int
	GetPartIndex() int
	SetNow()
}

type TaskStat struct {
	begintime time.time
	filename string
	fileIndex int
	partIndex int
	nReduce int
	nFiles int
}

type MapTaskSate struct {
	TaskStat
}

type ReduceTaskSate struct {
	TaskStat
}

func (this *MapTaskSate) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State: TaskMap,
		FileName: this.filename,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce: this.nReduce,
		NFiles: this.nFiles,
	}
}

func (this *ReduceTaskSate) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State: TaskReduce,
		FileName: this.filename,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce: this.nReduce,
		NFiles: this.nFiles,
	}
}

func (this *TaskStat) OutOfTime() bool {
	return time.Now().Sub(this.beginTime) > time.Duration(time.Second*60)
}

func (this *TaskStat) SetNow() {
	this.beginTime = time.Now()
}

func (this *TaskStat) GetFileIndex() int {
	return this.fileIndex
}

func (this *TaskStat) GetPartIndex() int {
	return this.partIndex
}

type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mu sync.Mutex
}

func (this *TaskStatQueue) Lock() {
	this.mu.Lock()
}
func (this *TaskStatQueue) Unlock() {
	this.mutex.Unlock()
}

func (this *TaskStatQueue) Size() int {
	return len(this.taskArray)
}

func (this *TaskStatQueue) Pop() TaskStatInterface {
	this.Lock()
	n := this.Size()
	if n == 0 {
		this.Unlock()
		return nil
	}
	ret := this.taskArray[n-1]
	this.taskArray = this.taskArray[:n-1]
	this.Unlock()
	return ret
}

func (this *TaskStatQueue) Push(task TaskStatInterface) {
	this.Lock()
	if task == nil {
		this.Unlock()
		return nil
	}
	this.taskArray = append(this.taskArray,task)
	this.Unlock()
}

func (this *TaskStatQueue) RemoveTask(findex int, pindex int) {
	this.Lock()
	for index := 0; index < len(this.taskArray); {
		task := this.taskArray[index]
		if task.GetFileIndex() == findex && task.GetPartIndex() == pindex {
			this.taskArray = append(this.taskArray[:index], this.taskArray[index+1:])
		} else {
			index ++
		}
	}
	this.Unlock()
}

func (this *Master) distributeReduce() {
	reducetask := ReduceTaskSate{
		TaskStat{
			fileIndex: 0,
			partIndex: 0,
			nFiles: len(this.filenames),
			nReduce: this.nReduce,
		},
	}
	for reduceindex := 0; reduceindex < len(this.filenames); reduceindex ++{
		reducetask.partIndex = reduceindex
		this.reduceTaskWaiting.Push(reducetask)
		
	}
}

func (this *TaskStatQueue) TimeOutQueue() []TaskStatInterface {
	this.Lock()
	outArray := make([]TaskStatInterface,0)
	for timeindex := 0; timeindex < len(this.taskArray); {
		if this.taskArray[timeindex].OutOfTime() {
			outArray = append(outArray,this.taskArray[timeindex])
			this.taskArray = append(this.taskArray[:timeindex],this.taskArray[timeindex+1:])
		} else {
			timeindex ++
		}
	}
	this.Unlock()
	return outArray
}

func (this *TaskStatQueue) MoveAppend(rhs *[]TaskStatInterface) {
	this.Lock()
	this.taskArray = append(this.taskArray,rhs)
	rhs = make([]TaskStatInterface,0)
	this.Unlock()
}





type Master struct {
	// Your definitions here.
	filenames []string

	// reduce task queue
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	// map task statistics
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	// machine state
	isDone  bool
	nReduce int

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	mapArray := make([]TaskStatInterface,0)
	for fileindex,filename := range files {
		maptask := MapTaskSate{
			TaskStat{
				fileIndex: fileindex,
				filename: filename,
				partIndex: 0,
				nFiles: len(files),
				nReduce: nReduce,
			},
		}
		mapArray = append(mapArray,&maptask)
	}
	m := Master{
		filenames: files,
		nReduce: nReduce,

	}
	// create tmp file
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Print("Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}
	}

	// begin a thread to collect tasks out of time
	go m.collectOutOfTime()
	
	
	// Your code here.


	m.server()
	return &m
}

func (this *Master) AskTask(arg *ExampleArgs, reply *TaskInfo) error {
	if this.isDone {
		reply.State = TaskEnd
		return nil
	}
	// check for map task
	maptask := this.mapTaskWaiting.Pop()
	if maptask != nil {
		maptask.SetNow()
		this.mapTaskRunning.Push(maptask)
		*reply = maptask.GenerateTaskInfo()
		fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}
	// check for reduce task
	reduceTask := this.reduceTaskWaiting.Pop()
	if reduceTask != nil {
		reduceTask.SetNow()
		this.reduceTaskRunning.Push(reduceTask)
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on part %v %vth file %v\n", reply.PartIndex, reply.FileIndex, reply.FileName)
		return nil
	}
	// all tasks distributed
	if this.mapTaskRunning.Size() > 0 || this.reduceTaskRunning.Size() > 0 {
		// must wait for new tasks
		reply.State = TaskWait
		return nil
	}
	// all tasks complete
	reply.State = TaskEnd
	this.isDone = true
	return nil
}

func (this *Master) TaskDone(arg *TaskInfo, reply *ExampleReply) {
	switch arg.State {
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		this.mapTaskRunning.RemoveTask(arg.FileIndex,arg.PartIndex)
		if this.mapTaskRunning.Size() == 0 && this.mapTaskWaiting.Size() == 0 {
			this.distributeReduce()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)
		this.reduceTaskRunning.RemoveTask(arg.FileIndex,arg.PartIndex)
	default:
		panic("task done error")
	}
	return nil
}

func (this *Master) collectOutOfTime() {
	for {
		time.Sleep(time.Duration(time.Second*5))
		timeouts := this.mapTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.mapTaskWaiting.MoveAppend(timeouts)
		}
		timeouts = this.reduceTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.reduceTaskWaiting.MoveAppend(timeouts)
		}
	}
} 