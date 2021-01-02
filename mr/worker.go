package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// MapF represents the map function
type MapF func(string, string) []KeyValue

// ReduceF represents the reduce function
type ReduceF func(string, []string) string

// KeyValue Stores intermediate result for worker
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	mapf    MapF
	reducef ReduceF
	conn    *rpc.Client
}

func (w *worker) run() {
	for {
		mapJob := MapJob{}
		err := w.conn.Call("GetMapJob", &JobRequest{}, &mapJob)
		if err != nil {
			log.Println("GetMapJob(): ", err)
			continue
		}
		if mapJob.NoneRemaining {
			break
		}
		w.handleMapJob(&mapJob)
	}

	for {
		reduceJob := ReduceJob{}
		err := w.conn.Call("GetReduceJob", &JobRequest{}, &reduceJob)
		if err != nil {
			log.Println("GetReduceJob(): ", err)
		}
		if reduceJob.NoneRemaining {
			break
		}
		w.handleReduceJob(&reduceJob)
	}
}

func (w *worker) handleMapJob(job *MapJob) {
	file, err := os.Open(job.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", job.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", job.Filename)
	}
	file.Close()
	kva := w.mapf(job.Filename, string(content))

	reducePartitions := make([][]KeyValue, job.NReduce, job.NReduce)

	for _, kv := range kva {
		r := ihash(kv.Key) % job.NReduce
		reducePartitions[r] = append(reducePartitions[r], kv)
	}

	for reduceID, partition := range reducePartitions {
		filename := mapFileName(job.MapID, reduceID)
		file, err := ioutil.TempFile("", filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}

		enc := json.NewEncoder(file)
		for _, kv := range partition {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("json encoding failed %v", err)
			}
		}

		err = os.Rename(file.Name(), filename)
		if err != nil {
			log.Fatalf("rename failed: %v", err)
		}

		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close %v", err)
		}
	}

	err = w.conn.Call("MapComplete", &MapComplete{ID: job.MapID}, &CompleteAck{})
	if err != nil {
		log.Println("MapComplete(): ", err)
	}
}

type byKey []KeyValue

// for sorting by key.
func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (w *worker) handleReduceJob(job *ReduceJob) {
	intermediate := make([]KeyValue, 0, 100)
	for mapID := 0; mapID < job.NMap; mapID++ {
		filename := mapFileName(mapID, job.ReduceID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close %v", err)
		}
	}

	sort.Sort(byKey(intermediate))

	outputFilename := fmt.Sprintf("mr-out-%d", job.ReduceID)
	ofile, err := ioutil.TempFile("", outputFilename)
	if err != nil {
		log.Fatalf("Cannot create file %v: %v", outputFilename, err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err = os.Rename(ofile.Name(), outputFilename)
	if err != nil {
		log.Fatalf("rename failed: %v", err)
	}

	err = ofile.Close()
	if err != nil {
		log.Fatalf("cannot close %v", err)
	}

	err = w.conn.Call("ReduceComplete", &ReduceComplete{ID: job.ReduceID}, &CompleteAck{})
	if err != nil {
		log.Println("ReduceComplete(): ", err)
	}
}

func mapFileName(mapID, reduceID int) string {
	return fmt.Sprintf("map-%d-%d", mapID, reduceID)
}

// Worker  main/mrworker.go calls this function.
func Worker(mapf MapF, reducef ReduceF) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

}

// CallExample - example function to show how to make an RPC call to the master.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
