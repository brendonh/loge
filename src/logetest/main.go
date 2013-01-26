package main

import (
	"fmt" 
	"loge"
)

type Person struct {
	Name string
	Age uint32
	Bits []uint16
}


func main() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/logetest"))

	db.CreateType("person", 2, &Person{})

	fmt.Printf("Done\n")

	// db.Transact(func(trans *loge.Transaction) {
	// 	var prev = trans.ReadObj("person", "brendon")

	// 	fmt.Printf("Previous: %#v\n", prev)

	// 	var brend = &Person{ 
	// 		Name: "Brendon", 
	// 		Age: 31,
	// 		DB: db,
	// 		Bits: []int{1,2,3},
	// 	}
		
	// 	trans.SetObj("person", "brendon", brend)
	// }, 0)

	//Example(db)
}