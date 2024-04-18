package mr

import (
	"encoding/gob"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	Lock          sync.Mutex
	MapTasks      []Reply
	ReduceTasks   []Reply
	IsProcessDone bool
	NReduce       int
	NLeftFiles    int
	NLeftReduces  int
	DoneFiles     map[string]bool
}

func (c *Coordinator) checkWorkerActivity(reply Reply) {
	// Sleep to simulate worker activity
	time.Sleep(time.Second * 10)

	c.Lock.Lock()
	defer c.Lock.Unlock()

	var isDone bool

	// Check the type of task in the reply and handle accordingly
	switch reply.Type {
	case MapTask:
		// Check if the map task is already done
		isDone = c.DoneFiles[reply.FileName]
		if !isDone {
			// If not done, append it to the list of map tasks
			c.MapTasks = append(c.MapTasks, reply)
		}
	case ReduceTask:
		// Check if the reduce task is already done
		isDone = c.DoneFiles[strconv.Itoa(reply.FileIdx)]
		if !isDone {
			// If not done, append it to the list of reduce tasks
			c.ReduceTasks = append(c.ReduceTasks, reply)
		}
	}
}

func (c *Coordinator) TaskCompleted(task *Task, reply *Reply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if !c.DoneFiles[task.FileName] {
		switch task.Type {
		case MapTask:
			if c.NLeftFiles > 0 {
				c.NLeftFiles--
			}
		case ReduceTask:
			if c.NLeftReduces > 0 {
				c.NLeftReduces--
			}
		}
	}

	c.DoneFiles[task.FileName] = true
	return nil
}

func (c *Coordinator) AskForNextTask(task *Task, reply *Reply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if len(c.MapTasks) > 0 {
		// Get the next map task if available
		poppedTask := c.MapTasks[0]
		c.MapTasks = c.MapTasks[1:]

		*reply = poppedTask
		reply.Success = true

		go c.checkWorkerActivity(*reply)
	} else if c.NLeftFiles == 0 && len(c.ReduceTasks) > 0 && c.NLeftReduces > 0 {
		// Get the next reduce task if available
		poppedTask := c.ReduceTasks[0]
		c.ReduceTasks = c.ReduceTasks[1:]

		*reply = poppedTask
		reply.Success = true

		go c.checkWorkerActivity(*reply)
	} else {
		// If no tasks are available, set to Idle
		reply.Success = true
		reply.Type = Idle
	}

	return nil
}

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	status := c.NLeftFiles == 0 && c.NLeftReduces == 0
	c.Lock.Unlock()
	return status
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Register Task and Reply types for gob serialization
	gob.Register(Task{})
	gob.Register(Reply{})

	c.server()

	// Initialize coordinator fields
	c.NReduce = nReduce
	c.IsProcessDone = false

	// Initialize map tasks with input files
	for i, file := range files {
		newTask := Reply{
			Type:      MapTask,
			FileIdx:   i,
			FileName:  file,
			NReducers: nReduce,
			IsDone:    false,
			Success:   false,
		}
		c.MapTasks = append(c.MapTasks, newTask)
	}

	// Initialize the counts of remaining map tasks and reduce tasks
	c.NLeftFiles = len(c.MapTasks)
	c.NLeftReduces = nReduce

	// Initialize a map to keep track of completed tasks
	c.DoneFiles = make(map[string]bool)

	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		newTask := Reply{Type: ReduceTask, FileIdx: i, IsDone: false}
		c.ReduceTasks = append(c.ReduceTasks, newTask)
	}

	return &c
}
