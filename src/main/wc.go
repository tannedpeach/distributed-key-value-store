package main

import (
	"os"
	"strconv"
	"strings"
	"unicode"
)
import "fmt"
import "src/mapreduce"
import "container/list"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {

	f := func(c rune) bool {
		return !unicode.IsLetter(c)
	}

	list1 := list.New()

	for _, element := range strings.FieldsFunc(value, f) {
		k1 := mapreduce.KeyValue{Key: element, Value: strconv.Itoa(1)}
		//fmt.Printf(element)
		list1.PushBack(k1)
	}
	//fmt.Println("list:", list1)
	//fmt.Printf("Fields are: %q", strings.FieldsFunc(value, f))

	return list1

}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	result := 0
	for e := values.Front(); e != nil; e = e.Next() {
		result += 1
	}
	return strconv.Itoa(result)
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
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
