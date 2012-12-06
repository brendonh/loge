package main

import (
	. "loge"

	"fmt"
	"sync"
	"time"
	"strings"
	"strconv"
	"runtime"
)

type Counter struct {
	Value int
}

func TestIncrements(db *LogeDB) {
	db.CreateType("counters", &Counter{})

	fmt.Printf("Testing without contention...\n")
	TestIncrement(db, false)

	fmt.Printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
	
	fmt.Printf("Testing with contention...\n")
	TestIncrement(db, true)
}

func TestIncrement(db *LogeDB, contend bool) {

	runtime.GOMAXPROCS(8)

	var procs = 1
	var loops = 50000
	var total = float64(procs * loops)

	if contend {
		var lc = db.CreateObj("counters", "counter")
		var v = lc.NewVersion()
		v.Object = &Counter{ Value: 0 }
		lc.ApplyVersion(v)
		db.EnsureObj(lc)
	} else {
		for i := 0; i < procs; i++ {
			key := fmt.Sprintf("counter%d", i)

			var lc = db.CreateObj("counters", key)
			var v = lc.NewVersion()
			v.Object = &Counter{ Value: 0 }
			lc.ApplyVersion(v)
			db.EnsureObj(lc)
		}
	}			

	var start = time.Now()

	var group sync.WaitGroup
	for i := 0; i < procs; i++ {
		var key string
		if contend {
			key = "counter"
		} else {
			key = fmt.Sprintf("counter%d", i)
		}
		group.Add(1)
		go LoopIncrement(db, key, &group, loops)
	}
	group.Wait()

	var dur = time.Since(start)
	var secs = float64(dur) / float64(time.Second)

	var val string
	if contend {
		val = strconv.Itoa(db.GetObj("counters", "counter").Current.Object.(*Counter).Value)
	} else {
		var bits = make([]string, 0, procs)
		for i := 0; i < procs; i++ {
			key := fmt.Sprintf("counter%d", i)
			var temp = db.GetObj("counters", key).Current.Object.(*Counter).Value
			bits = append(bits, strconv.Itoa(temp))
		}
		val = strings.Join(bits, ", ")
	}		
	fmt.Printf("Final val(s): %s (%s, %.0f/s)\n", val, dur, total / secs)

}


func LoopIncrement(db *LogeDB, key string, group *sync.WaitGroup, count int) {
	var actor = func(t *Transaction) { Increment(t, key) }
	for i := 0; i < count; i++ {		
		if !db.Transact(actor, 0) {
			fmt.Printf("Timeout!\n")
		}
	}
	group.Done()
}


func Increment(trans *Transaction, key string) {
	var counter = trans.WriteObj("counters", key).(*Counter)
	counter.Value += 1
}
