package loge

import (
	"testing"
	"runtime"
	"strconv"
	"sync"
)

type TestCounter struct {
	Value int
}

func BenchmarkNoContention(b *testing.B) {
	b.StopTimer()

	var procs = runtime.NumCPU()
	var origProcs = runtime.GOMAXPROCS(procs)

	var db = NewLogeDB(NewMemStore())
	db.CreateType("counters", StructTypeFor(&TestCounter{}))

	db.Transact(func (t *Transaction) {
		for i := 0; i < procs; i++ {
			var key LogeKey = LogeKey(strconv.Itoa(i))
			t.SetObj("counters", key, &TestCounter{Value: 0})
		}
	}, 0)

	b.StartTimer()

	var group sync.WaitGroup
	for i := 0; i < procs; i++ {
		var key = LogeKey(strconv.Itoa(i))
		group.Add(1)
		go LoopIncrement(db, key, &group, b.N)
	}
	group.Wait()

	b.StopTimer()

	db.Transact(func (t *Transaction) {
		for i := 0; i < procs; i++ {
			var key = LogeKey(strconv.Itoa(i))
			var counter = t.ReadObj("counters", key).(*TestCounter)
			if counter.Value != b.N {
				b.Errorf("Wrong count for counter %d: %d / %d", 
					i, counter.Value, b.N)
			}
		}
	}, 0)

	runtime.GOMAXPROCS(origProcs)
}



func BenchmarkContention(b *testing.B) {
	b.StopTimer()

	var procs = runtime.NumCPU()
	var origProcs = runtime.GOMAXPROCS(procs)

	var db = NewLogeDB(NewMemStore())
	db.CreateType("counters", StructTypeFor(&TestCounter{}))

	db.Transact(func (t *Transaction) {
		t.SetObj("counters", "contended", &TestCounter{Value: 0})
	}, 0)

	b.StartTimer()

	var group sync.WaitGroup
	for i := 0; i < procs; i++ {
		group.Add(1)
		go LoopIncrement(db, "contended", &group, b.N)
	}
	group.Wait()

	b.StopTimer()

	db.Transact(func (t *Transaction) {
		var target = b.N * procs
		var counter = t.ReadObj("counters", "contended").(*TestCounter)
		if counter.Value != target {
			b.Errorf("Wrong count for counter: %d / %d", 
				counter.Value, target)
		}
	}, 0)

	runtime.GOMAXPROCS(origProcs)
}


func LoopIncrement(db *LogeDB, key LogeKey, group *sync.WaitGroup, count int) {
	var actor = func(t *Transaction) { Increment(t, key) }
	for i := 0; i < count; i++ {		
		db.Transact(actor, 0)
	}
	group.Done()
}


func Increment(trans *Transaction, key LogeKey) {
	var counter = trans.WriteObj("counters", key).(*TestCounter)
	counter.Value += 1
}
