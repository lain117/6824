package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// channel which hold uncompleted task
	mutex sync.Mutex
	mapIndex int
	reduceIndex int
	nReduce int
	map1 map[int]bool //false 未完成且计时不到10s true 未完成且计时已过10s ///time-out
	reducephase bool
	files []string
}

func (c *Coordinator) AskReduceNum(arg *AskReduceArgs, reply *AskReduceNumReply) error {
	reply.ReduceNum = c.nReduce
	return nil
}


func (c *Coordinator) AskTask(arg *AskTaskArgs, reply *AskTaskReply) error {
	if c.reducephase {
		reply.StartReduce = true
		return nil
	}

	if c.mapIndex < len(c.files) {
		reply.Task = c.files[c.mapIndex]
		c.map1[c.mapIndex] = false
		go c.waitWorker(c.mapIndex)
		c.mapIndex++
	} else if len(c.map1) > 0 {
		key := -1
		for k,v := range c.map1 {
			if v {
				key = k
				break
			}
		}
		if key >= 0 {
			reply.Task = c.files[key]
			c.map1[key] = false
			go c.waitWorker(key)
		}
	} else {
		reply.StartReduce = true
		c.reducephase = true
	}

	return nil
}

func (c *Coordinator) MapSuccess(arg *MapSuccessArgs, reply *MapSuccessReply) error {
	for k,v := range c.files {
		if v == arg.Task {
			delete(c.map1,k)
			break
		}
	}
	return nil
}


func (c *Coordinator) AskReduce(arg *AskReduceArgs, reply *AskReduceNumReply) error {
	if c.reduceIndex < c.nReduce { 
		reply.ReduceNum = c.reduceIndex + 1
		c.map1[c.reduceIndex] = false
		go c.waitWorker(c.reduceIndex)
		c.reduceIndex++
	} else if len(c.map1) > 0 {
		key := -1
		for k,v := range c.map1 {
			if v {
				key = k
				break
			}
		}
		if key >= 0 {
			reply.ReduceNum = key
			c.map1[key] = false
			go c.waitWorker(key)
		}
	} 
	return nil
}

func (c *Coordinator) ReduceSuccess(arg *ReduceSuccessArgs, reply *ReduceSuccessReply) error {
	delete(c.map1,arg.ReduceNum)
	return nil
}

func (c *Coordinator) waitWorker(id int) {
	time.Sleep(10*time.Second)
	if _,ok := c.map1[id]; ok {
		c.map1[id] = true
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		nReduce: nReduce,
		reduceIndex: 0,
		mapIndex: 0,
		reducephase: false,
		map1: make(map[int]bool),
	}
	c.server()
	return &c
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		http.DefaultServeMux.ServeHTTP(w, r)
	}))
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if c.reducephase && c.nReduce == c.reduceIndex && len(c.map1) == 0 {
		return true
	}
	return false
}

//before serve