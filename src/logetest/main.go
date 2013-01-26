package main

import (
	"fmt" 
	"loge"
)

type Person struct {
	Name string
	Age uint32
	Bits []uint16 `loge:"copy"`
}


func main() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/logetest"))

	db.CreateType("person", 2, &Person{})

	db.Transact(func(trans *loge.Transaction) {
		var prev = trans.ReadObj("person", "brendon").(*Person)

		fmt.Printf("Previous: %v\n", prev)

		var brend = Person{ 
			Name: "Brendon", 
			Age: 32,
			Bits: []uint16{1,4,3},
		}
		
		trans.SetObj("person", "brendon", &brend)
	}, 0)

	//Example(db)
}