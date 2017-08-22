package main

import "os"
import "fmt"
import "strings"
import "strconv"
import "mapreduce"
import "container/list"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
   l := list.New()
   for _, v := range strings.Split(value, " ") {
      var kv mapreduce.KeyValue
      kv.Key = v
      kv.Value = v
      l.PushBack(kv)
   }
   return l
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
   value := 0
   for e := values.Front(); e != nil; e = e.Next() {
      if e.Value == key {
         value++
      }
   }
   return strconv.Itoa(value)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
  if len(os.Args) != 4 {
    fmt.Printf("%s: see usage comments in file\n", os.Args[0])
  } else if os.Args[1] == "master" {
    if os.Args[3] == "sequential" {
      mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
    } else {
      mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])    
      // Wait until MR is done
      <- mr.DoneChannel
    }
  } else {
    mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
  }
}
