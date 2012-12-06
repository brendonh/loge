package main

import (
	. "loge"

	"fmt"
	"sync"
	"time"

	"runtime"
)

type Counter struct {
	Value int
}

func TestIncrements(db *LogeDB) {

	runtime.GOMAXPROCS(8)

	db.CreateType("counter")

	db.CreateObj("counter", "counter", &Counter{
		Value: 0,
	})

	var start = time.Now()

	var group sync.WaitGroup
	for i := 0; i < 50; i++ {
		group.Add(1)
		go LoopIncrement(db, &group, 5000)
	}
	group.Wait()

	var dur = time.Since(start)
	var val = db.GetObj("counter").Current.Object.(*Counter).Value
	fmt.Printf("Final val: %d (%s)\n", val, dur)

}


func LoopIncrement(db *LogeDB, group *sync.WaitGroup, count int) {
	for i := 0; i < count; i++ {		
		if !db.Transact(Increment, 100 * time.Millisecond) {
			fmt.Printf("Timeout!\n")
		}
	}
	group.Done()
}


func Increment(trans *Transaction) {
	var counter = trans.WriteObj("counter").(*Counter)
	counter.Value += 1
}
