package main

import (
	"loge"
	"fmt"
	"time"
	"runtime"
)

const TOTAL = 1000000
const BATCH_SIZE = 10000

func WriteBench() {
	var cores = runtime.NumCPU()
	fmt.Printf("Using %d cores\n", cores)
	runtime.GOMAXPROCS(cores)

	DoWrite(cores, 0)
	// DoWrite(cores, 1)
	// DoWrite(cores, 2)
	// DoWrite(cores, 3)
	// DoWrite(cores, 4)

}

func DoWrite(cores int, idx int) {
	var db = loge.NewLogeDB(loge.NewLevelDBStore(fmt.Sprintf("data/bench%d", idx)))
	defer db.Close()
	db.CreateType(loge.NewTypeDef("person", 1, &Person{}))

	var startTime = time.Now()

	var tokens = make(chan bool, cores)
	for i := 0; i < cores; i++ {
		tokens<- true
	}

	for startId := 0; startId < TOTAL; startId += BATCH_SIZE {
		<-tokens
		var endId = startId + BATCH_SIZE - 1
		if endId >= TOTAL {
			endId = TOTAL - 1
		}
		go WritePeopleBatch(db, startId, endId, tokens)
	}

	for i := 0; i < cores; i++ {
		<-tokens
	}

	fmt.Printf("Done in %v\n", time.Since(startTime))

	fmt.Printf("Flushed\n")

}

func WritePeopleBatch(db *loge.LogeDB, start int, end int, tokens chan bool) {
	db.Transact(func(t *loge.Transaction) {
		fmt.Printf("Writing batch %d => %d\n", start, end)
		for i := start; i <= end; i++ {
			var name = fmt.Sprintf("Person %d", i)
			var person = Person{
				Name: name,
				Age: uint32(i),
			}
			t.Set("person", loge.LogeKey(name), &person)
		}
	}, 0)
	tokens<- true
}