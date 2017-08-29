package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  idle bool
  in_progress bool
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func DispatchWork(mr *MapReduce, worker *WorkerInfo, num int, work string) {
   fmt.Printf("dispatch work : %s\n", work)
   args := &DoJobArgs{}
   //args.File = MapName(mr.file, mr.remainMap)
   args.File = mr.file
   args.Operation = JobType(work)
   args.JobNumber = num
   //args.JobNumber = mr.remainMap
   args.NumOtherPhase = mr.nMap
   fmt.Printf("here ? \n");
   var reply DoJobReply
   ok := call(worker.address, "Worker.DoJob", args, &reply)
   fmt.Printf("here 1? \n");
   if ok == false {
      fmt.Printf("worker %s DoJob error\n", worker.address)
   } else {
      if work == "Map" {
         mr.remainMap--
      } else {
         mr.remainReduce--
      }
   }
   worker.idle = true
}

func (mr *MapReduce) RunMaster() *list.List {
   fmt.Printf("master dispatch work begin...\n");
   fmt.Printf("worker num = %d ...\n", len(mr.Workers));
   var num int
   num = mr.remainMap
   var jobs [num]int
   //jobs = make()
   for num > 0 {
      for _, w := range mr.Workers  {
         if w.idle == true {
            w.idle = false
            //mr.remainMap--
            //num = mr.remainMap - 1
            num--
            go DispatchWork(mr, w, num, "Map")
         }
      }
   }
   for mr.remainMap > 0 {

   }

   num = mr.remainReduce
   for num > 0 {
      for _, w := range mr.Workers {
         if w.idle == true {
            w.idle = false
            //mr.remainReduce--
            //num = mr.remainReduce
            num--
            go DispatchWork(mr, w, num, "Reduce")
         }
      }
   }

   for mr.remainReduce > 0 {

   }
   // Your code here
   return mr.KillWorkers()
}
