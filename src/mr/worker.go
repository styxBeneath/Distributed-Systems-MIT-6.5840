package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// Len for sorting by key.
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

func getFileName(oname string, Files *[]KeyValue) string {
	for _, kv := range *Files {
		if kv.Value == oname {
			return kv.Key
		}
	}
	return ""
}

func doMap(nReduce int, filename string, mapf func(string, string) []KeyValue, idx int) {
	// Open the input file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v: %v", filename, err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("cannot close %v: %v", file, err)
		}
	}(file)

	// Create a buffer to read the file contents
	buf := make([]byte, 1024)
	var contents []byte

	for {
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			log.Fatalf("cannot read %v: %v", filename, err)
		}
		if n == 0 {
			break
		}
		contents = append(contents, buf[:n]...)
	}

	// Apply the map function to generate key-value pairs
	kva := mapf(filename, string(contents))
	sort.Sort(ByKey(kva))

	// Create temporary intermediate files
	var intermediateFiles []KeyValue
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", idx, i)
		tempFile, err := ioutil.TempFile(".", oname)
		if err != nil {
			log.Fatalf("error creating temp file: %v", err)
		}
		intermediateFiles = append(intermediateFiles, KeyValue{tempFile.Name(), oname})

		// Write key-value pairs to the output file
		encoder := json.NewEncoder(tempFile)
		if encoder == nil {
			log.Fatalf("error creating encoder")
		}

		for _, kv := range kva {
			if ihash(kv.Key)%nReduce == i {
				err := encoder.Encode(&kv)
				if err != nil {
					log.Fatalf("error encoding JSON: %v", err)
				}
			}
		}

		tempFile.Close()
	}

	// Rename the temporary files to their final names
	for _, kv := range intermediateFiles {
		err := os.Rename(kv.Key, kv.Value)
		if err != nil {
			log.Fatalf("error renaming file: %v", err)
		}
	}
}

func doReduce(reducef func(string, []string) string, idx int) {
	// Create a map to store key-value pairs
	keyValueMap := make(map[string][]string)

	// Collect key-value pairs from intermediate files
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), strconv.Itoa(idx)) {
			ofile, err := os.Open(file.Name())
			if err != nil {
				log.Fatalf("error opening file: %v\n", err)
			}
			defer func(ofile *os.File) {
				err := ofile.Close()
				if err != nil {
					log.Fatalf("cannot close %v: %v", ofile, err)
				}
			}(ofile)

			decoder := json.NewDecoder(ofile)
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				keyValueMap[kv.Key] = append(keyValueMap[kv.Key], kv.Value)
			}
		}
	}

	// Create the output file
	outFileName := fmt.Sprintf("mr-out-%d", idx)
	outputFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatalf("error creating output file: %v\n", err)
	}
	defer func(outputFile *os.File) {
		err := outputFile.Close()
		if err != nil {
			log.Fatalf("cannot close %v: %v", outputFile, err)
		}
	}(outputFile)

	// Sort the keys
	var keys []string
	for key := range keyValueMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Apply reduce function and write results to the output file
	for _, key := range keys {
		values := keyValueMap[key]
		output := reducef(key, values)
		_, err := fmt.Fprintf(outputFile, "%v %v\n", key, output)
		if err != nil {
			log.Fatalf("error writing to output file: %v\n", err)
		}
	}
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		var task Task
		var reply Reply

		// Get the next task from the coordinator
		status := call("Coordinator.AskForNextTask", &task, &reply)
		if !status {
			os.Exit(-1)
		}

		task.Type = reply.Type
		nReduce := reply.NReducers
		file := reply.FileName
		idx := reply.FileIdx

		if task.Type == MapTask {
			task.FileName = reply.FileName
		} else {
			task.FileName = strconv.Itoa(reply.FileIdx)
		}

		if reply.Type == Idle {
			time.Sleep(time.Second * 2)
		} else {
			if reply.Type == MapTask {
				// Perform map task
				doMap(nReduce, file, mapf, idx)
			} else {
				// Perform reduce task
				doReduce(reducef, idx)
			}

			// Notify the coordinator that the task is done
			call("Coordinator.TaskCompleted", &task, &reply)
		}
	}

}

// CallExample
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
