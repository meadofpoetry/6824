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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var pid = os.Getpid()

var mapFun func(string, string) []KeyValue
var reduceFun func(string, []string) string

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	mapFun = mapf
	reduceFun = reducef

	log.Printf("Starting worker\n")

	idle()
}

func idle() {
	idleMsg := IdleMessage{ pid }
	
	for {
		reply := AssignJob{}
		log.Printf("Worker: calling idle\n")
		ok := call("Coordinator.Idle", &idleMsg, &reply)
		if ok {
			switch reply.Job {
			case JobMap:
				log.Printf("Worker: got map job with id %s\n", reply.Id)
				runMap(reply.Id, reply.MapArg, reply.NReduce)
			case JobReduce:
				log.Printf("Worker: got reduce job with id %s\n", reply.Id)
				runReduce(reply.Id, reply.ReduceArg)
			}
			
		}
		time.Sleep(time.Second)
	}
}

func runMap(id string, filename string, nReduce int) {	
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapFun(filename, string(content))
	tempfiles := make(map[int]string)

	for reduceId, kvs := range groupByHash(kva, nReduce) {

		outfile, err := ioutil.TempFile(".", "map-tmp-*")
		defer outfile.Close()
		if err != nil {
			log.Fatalf("cannot open tmp file %v", err)
		}

		enc := json.NewEncoder(outfile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode data: %v", err)
			}
		}

		tempfiles[reduceId] = outfile.Name()
	}

	var result []string
	for reduceId, tempfile := range tempfiles {
		name := fmt.Sprintf("mr-%s-%d", id, reduceId)
		err := os.Rename(tempfile, name)
		if err != nil {
			log.Fatalf("cannot rename temporary file %s to %s", tempfile, name)
		}
		result = append(result, name)
	}

	arg := CompleteJob{
		Id : id,
		Job : JobMap,
		MapResult : result,
	}
	reply := CompleteAccepted{}
	call("Coordinator.Complete", &arg, &reply)

	if !reply.Success {
		// Try to cleanup on failure
		for _, name := range result {
			os.Remove(name)
		}
		log.Fatal("map completion not committed")
	}
}

func runReduce(id string, files []string) {
	var kva []KeyValue
	
	for _, filename := range files {
		file, err := os.Open(filename)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	outfile, err := ioutil.TempFile(".", "reduce-tmp-*")
	defer outfile.Close()
	if err != nil {
		log.Fatalf("cannot create temporary file %v", err)
	}

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reduceFun(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	
	name := fmt.Sprintf("mr-out-%s", id)
	err = os.Rename(outfile.Name(), name)
	if err != nil {
		log.Fatalf("cannot rename temporary file %s", outfile.Name())
	}
	
	arg := CompleteJob{
		Id : id,
		Job : JobReduce,
		ReduceResult : name,
	}
	reply := CompleteAccepted{}
	call("Coordinator.Complete", &arg, &reply)
	
	if !reply.Success {
		// Try to cleanup on failure
		os.Remove(name)
		log.Fatal("reduce completion not committed")
	}
}

func groupByHash(kva []KeyValue, nReduce int) map[int][]KeyValue {
	res := make(map[int][]KeyValue)
	
	for _, kv := range kva {
		id := ihash(kv.Key) % nReduce
		arr := res[id]
		res[id] = append(arr, kv)
	}

	return res
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
