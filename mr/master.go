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

// Master contains logic for master server
type Master struct {
	// Your definitions here.
	sync.RWMutex
	mapJobs          chan MapJob
	nMap             int
	mapCompleted     []bool
	mapRemaining     int
	mapPhaseComplete bool

	reduceJobs      chan ReduceJob
	nReduce         int
	reduceCompleted []bool
	reduceRemaining int
}

// MakeMaster - create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)
	m := Master{
		mapJobs:          make(chan MapJob, nMap),
		nMap:             nMap,
		mapCompleted:     make([]bool, nMap, nMap),
		mapRemaining:     nMap,
		mapPhaseComplete: false,
		reduceJobs:       make(chan ReduceJob, nReduce),
		nReduce:          nReduce,
		reduceCompleted:  make([]bool, nReduce, nReduce),
		reduceRemaining:  nReduce,
	}

	for i, f := range files {
		m.mapJobs <- MapJob{NoneRemaining: false, Filename: f, MapID: i, NReduce: nReduce}
	}

	for i := 0; i < nReduce; i++ {
		m.reduceJobs <- ReduceJob{NoneRemaining: false, ReduceID: i, NMap: nMap}
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
	if m.mapDone() {
		job.NoneRemaining = true
		job.MapID = -1
		job.Filename = ""
		return nil
	}

	done := make(chan struct{})
	go func() {
		for {
			if m.mapDone() {
				close(done)
				return
			}
			time.Sleep(time.Second)
		}
	}()

	for {
		select {
		case <-done:
			job.NoneRemaining = true
			job.MapID = -1
			job.NReduce = 0
			job.Filename = ""
			return nil
		case j := <-m.mapJobs:
			*job = j

			go func() {
				time.Sleep(10 * time.Second)
				if !m.checkMapComplete(j.MapID) {
					m.mapJobs <- j
				}
			}()

			return nil
		}
	}
}

// GetReduceJob returns a reduce job if one is available or blocks until one is available or all are complete
func (m *Master) GetReduceJob(req *JobRequest, job *ReduceJob) error {
	if m.Done() {
		job.NoneRemaining = true
		job.ReduceID = -1
		job.NMap = 0
		return nil
	}

	done := make(chan struct{})
	go func() {
		for {
			if m.mapDone() {
				close(done)
				return
			}
			time.Sleep(time.Second)
		}
	}()

	for {
		select {
		case <-done:
			job.NoneRemaining = true
			job.ReduceID = -1
			job.NMap = 0
			return nil
		case j := <-m.reduceJobs:
			*job = j

			go func() {
				time.Sleep(10 * time.Second)
				if !m.checkReduceComplete(j.ReduceID) {
					m.reduceJobs <- j
				}
			}()

			return nil
		}
	}
}

// MapComplete marks that a map job has completed
func (m *Master) MapComplete(job *MapComplete, res *CompleteAck) error {
	m.Lock()
	defer m.Unlock()

	m.mapCompleted[job.ID] = true
	m.mapRemaining--

	return nil
}

// ReduceComplete marks that a map job has completed
func (m *Master) ReduceComplete(job *ReduceComplete, res *CompleteAck) error {
	m.Lock()
	defer m.Unlock()

	m.reduceCompleted[job.ID] = true
	m.reduceRemaining--

	return nil
}

func (m *Master) checkMapComplete(id int) bool {
	m.RLock()
	defer m.RUnlock()

	return m.mapCompleted[id]
}

func (m *Master) checkReduceComplete(id int) bool {
	m.RLock()
	defer m.RUnlock()

	return m.reduceCompleted[id]
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
