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

func DispatchWork(mr *MapReduce, worker *WorkerInfo, jobNumber int, num int, work string) {
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
      mr.jobsNotDone[jobNumber] = 0
   } else {
      mr.jobsNotDone[jobNumber] = 1
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
   for mr.remainMap > 0 {
      for i := 0; i < mr.nMap; i++ {
         if (mr.jobsNotDone[i] == 0) {
            for _, w := range mr.Workers  {
               if w.idle == true {
                  w.idle = false
                  fmt.Printf("i = %d, done[%d] = %d\n", i, i, mr.jobsNotDone[i])
                  mr.jobsNotDone[i] = 1
                  go DispatchWork(mr, w, i, i, "Map")
                  break
               }
            }
         }
      }

   }

   for mr.remainReduce > 0 {
      for i := mr.nMap; i < mr.nReduce + mr.nMap; i++ {
         if (mr.jobsNotDone[i] == 0) {
            for _, w := range mr.Workers {
               if w.idle == true {
                  w.idle = false
                  mr.jobsNotDone[i] = 1
                  go DispatchWork(mr, w, i, i - mr.nMap, "Reduce")
                  break
               }
            }
         }
      }
   }
   // Your code here
   return mr.KillWorkers()
}
