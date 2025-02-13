package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		taskinfo := CallAskTask()
		switch taskinfo.State {
		case TaskMap:
			workerMap(mapf,taskinfo)
			break
		case TaskReduce:
			workerReduce(reducef,taskinfo)
			break
		case TaskWait:
			time.Sleep(time.Duration(time.Second * 5))
			break
		case TaskEnd:
			fmt.Println("Master all tasks complete. Nothing to do...")
			return
		default:
			panic("Invalid Task state received by worker")
		}
	}
}

//
// example function to show how to make an RPC call to the master.
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

//call task
func CallAskTask() *TaskInfo {
	args := ExampleArgs{}
	reply := TaskInfo{}
	call("Master.AskTask",&args,&reply)
	return &reply
}

func CalltaskDone(taskinfo *TaskInfo) {
	reply := ExampleReply{}
	call("Master.AskDone",taskinfo,&reply)
}

func workerMap(mapf func(string, string) []KeyValue,taskinfo *TaskInfo) {
	fmt.Println("now worker get a map mission,index is %d,",taskinfo.fileIndex)
	intermediate := []KeyValue{}
	file, err := os.Open(taskinfo.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(content, ff)
	kva := mapf(taskInfo.FileName, string(content))
	intermediate = append(intermediate, kva...)

	nReduce := taskInfo.NReduce
	outprefix := "mr-tmp/mr-"
	outprefix += strconv.Itoa(taskInfo.FileIndex)
	outprefix += "-"
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for outindex := 0; outindex < nReduce; outindex++ {
		//outname := outprefix + strconv.Itoa(outindex)
		//outFiles[outindex], _ = os.Create(outname)
		outFiles[outindex], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}
	for _,kv := range intermediate {
		outindex := ihash(kv.Key) % nReduce
		file := outFiles[outindex]
		enc := fileEncs[outindex]
		err = enc.Encoder(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", taskInfo.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}
	for outindex, file := range outFiles {
		outname := outprefix + strconv.Itoa(outindex)
		oldpath := filepath.Join(file.Name())
		//fmt.Printf("temp file oldpath %v\n", oldpath)
		os.Rename(oldpath, outname)
		file.Close()
	}
	// acknowledge master
	CallTaskDone(taskInfo)

}

func workerReduce(reducef func(string, string) []KeyValue,taskinfo *TaskInfo) {
	fmt.Println("Got assigned reduce task on part %v\n", taskInfo.PartIndex)
	outname := "mr-out-" + strconv.Itoa(taskInfo.PartIndex)
	innameprefix := "mr-tmp/mr-"
	innamesuffix := "-" + strconv.Itoa(taskInfo.PartIndex)
	intermediate := []KeyValue{}
	for index := 0; index < taskinfo.NFiles; index ++ {
		inname := innameprefix + strconv.Itoa(index) + innamesuffix
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
			panic("Open file error")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decoder(&kv); err != nil {
				break
			}
			intermediate = append(intermediate,kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	ofile, err := ioutil.TempFile("mr-tmp", "mr-*")
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", outname, err)
		panic("Create file error")
	}
	//fmt.Printf("%v\n", intermediate)
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile,outname)
	ofile.Close()
	CallTaskDone(taskinfo)
}