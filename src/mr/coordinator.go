package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	TaskIdle TaskState = iota
	TaskInProgress
	TaskFinished
)

type Task struct {
	TaskID    int
	Filename  string
	TaskState TaskState
	StartTime time.Time
}

type Phase int

const (
	PhaseMap Phase = iota
	PhaseReduce
	PhaseDone
)

type Coordinator struct {
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
	phase       Phase
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Helper Functions
// Check whether every task in tasks has finished.
func allCompleted(tasks []Task) bool {
	for _, task := range tasks {
		if task.TaskState != TaskFinished {
			return false
		}
	}
	return true
}

// Assign the first idle task, or a task whose worker has held it for too long.
func (c *Coordinator) assignIdleTask(tasks []Task, reply *TaskReply, taskType TaskType) bool {
	for i := range tasks {
		task := &tasks[i]
		if task.TaskState == TaskIdle || (task.TaskState == TaskInProgress && time.Since(task.StartTime) >= 10*time.Second) {
			task.TaskState = TaskInProgress
			task.StartTime = time.Now()
			reply.TaskType = taskType
			reply.TaskID = task.TaskID
			reply.Filename = task.Filename
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			return true
		}
	}
	return false
}

// Assign one map task. If all map tasks are complete, it advances the coordinator to PhaseReduce.
func (c *Coordinator) handleMap(reply *TaskReply) bool {
	if c.assignIdleTask(c.mapTasks, reply, TaskMap) {
		return true
	}
	if allCompleted(c.mapTasks) {
		c.phase = PhaseReduce
		return false
	}
	reply.TaskType = TaskWait
	return true
}

// Assign one reduce task. If all reduce tasks are complete, it advances the coordinator to PhaseDone.
func (c *Coordinator) handleReduce(reply *TaskReply) bool {
	if c.assignIdleTask(c.reduceTasks, reply, TaskReduce) {
		return true
	}
	if allCompleted(c.reduceTasks) {
		c.phase = PhaseDone
		return false
	}
	reply.TaskType = TaskWait
	return true
}

// Assigns task to a worker
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == PhaseMap {
		if c.handleMap(reply) {
			return nil
		}
	}

	if c.phase == PhaseReduce {
		if c.handleReduce(reply) {
			return nil
		}
	}

	if c.phase == PhaseDone {
		reply.TaskType = TaskExit
	}
	return nil
}

// Records a worker's successful completion of a map or reduce task.
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case TaskMap:
		task := &c.mapTasks[args.TaskID]
		task.TaskState = TaskFinished
		if c.phase == PhaseMap && allCompleted(c.mapTasks) {
			c.phase = PhaseReduce
		}
	case TaskReduce:
		task := &c.reduceTasks[args.TaskID]
		task.TaskState = TaskFinished
		if c.phase == PhaseReduce && allCompleted(c.reduceTasks) {
			c.phase = PhaseDone
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == PhaseDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:    len(files),
		nReduce: nReduce,
		phase:   PhaseMap,
	}

	c.mapTasks = make([]Task, len(files))
	for i, filename := range files {
		c.mapTasks[i] = Task{
			TaskID:    i,
			Filename:  filename,
			TaskState: TaskIdle,
		}
	}

	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			TaskID:    i,
			TaskState: TaskIdle,
		}
	}

	c.server()
	return &c
}
