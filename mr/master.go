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

// JobStatus stores the status of a job
type JobStatus uint8

const (
	// Queued represents a job that has yet to be assigned
	Queued JobStatus = iota
	// InProgress represents a running job
	InProgress
	// Completed represents a completed job
	Completed
)

// MapJobEntry stores a job and its status
type MapJobEntry struct {
	status    JobStatus
	startTime time.Time
	mapJob    MapJob
}

// ReduceJobEntry stores a job and its status
type ReduceJobEntry struct {
	status    JobStatus
	startTime time.Time
	reduceJob ReduceJob
}

// Master contains logic for master server
type Master struct {
	// Your definitions here.
	sync.RWMutex
	mapJobs          []*MapJobEntry
	nMap             int
	mapRemaining     int
	mapPhaseComplete bool

	reduceJobs      []*ReduceJobEntry
	nReduce         int
	reduceRemaining int
}

// MakeMaster - create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)
	m := Master{
		mapJobs:          make([]*MapJobEntry, 0, nMap),
		nMap:             nMap,
		mapRemaining:     nMap,
		mapPhaseComplete: false,
		reduceJobs:       make([]*ReduceJobEntry, 0, nReduce),
		nReduce:          nReduce,
		reduceRemaining:  nReduce,
	}

	for i, f := range files {
		j := MapJob{NoneRemaining: false, Filename: f, MapID: i, NReduce: nReduce}
		m.mapJobs = append(m.mapJobs, &MapJobEntry{status: Queued, mapJob: j})
	}

	for i := 0; i < nReduce; i++ {
		j := ReduceJob{NoneRemaining: false, ReduceID: i, NMap: nMap}
		m.reduceJobs = append(m.reduceJobs, &ReduceJobEntry{status: Queued, reduceJob: j})
	}

	m.server()
	return &m
}

// Your code here -- RPC handlers for the worker to call.

// Example is an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetMapJob returns a map job if one is available or blocks until one is available or all are complete
func (m *Master) GetMapJob(req *JobRequest, job *MapJob) error {
	for {
		if m.mapDone() {
			job.NoneRemaining = true
			job.MapID = -1
			job.Filename = ""
			return nil
		}

		m.Lock()
		for _, j := range m.mapJobs {
			readyQueued := j.status == Queued
			workerFailed := j.status == InProgress && time.Now().After(j.startTime.Add(10*time.Second))
			if readyQueued || workerFailed {
				*job = j.mapJob
				j.status = InProgress
				j.startTime = time.Now()
				m.Unlock()
				return nil
			}
		}
		m.Unlock()

		time.Sleep(time.Second)
	}
}

// GetReduceJob returns a reduce job if one is available or blocks until one is available or all are complete
func (m *Master) GetReduceJob(req *JobRequest, job *ReduceJob) error {
	for {
		if m.Done() {
			job.NoneRemaining = true
			job.ReduceID = -1
			return nil
		}

		m.Lock()
		for _, j := range m.reduceJobs {
			readyQueued := j.status == Queued
			workerFailed := j.status == InProgress && time.Now().After(j.startTime.Add(10*time.Second))
			if readyQueued || workerFailed {
				*job = j.reduceJob
				j.status = InProgress
				j.startTime = time.Now()
				m.Unlock()
				return nil
			}
		}
		m.Unlock()

		time.Sleep(time.Second)
	}
}

// MapComplete marks that a map job has completed
func (m *Master) MapComplete(job *MapComplete, res *CompleteAck) error {
	m.Lock()
	defer m.Unlock()

	if m.mapJobs[job.ID].status != Completed {
		m.mapJobs[job.ID].status = Completed
		m.mapRemaining--
	}

	if m.mapRemaining == 0 {
		m.mapPhaseComplete = true
	}

	return nil
}

// ReduceComplete marks that a map job has completed
func (m *Master) ReduceComplete(job *ReduceComplete, res *CompleteAck) error {
	m.Lock()
	defer m.Unlock()

	if m.reduceJobs[job.ID].status != Completed {
		m.reduceJobs[job.ID].status = Completed
		m.reduceRemaining--
	}

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

func (m *Master) mapDone() bool {
	m.RLock()
	defer m.RUnlock()

	return m.mapPhaseComplete
}

// Done - main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.RLock()
	defer m.RUnlock()

	return m.reduceRemaining == 0
}
