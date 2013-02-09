package main

import (
	"loge"
	"fmt"
)

func Sandbox() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/sandbox"))

	db.CreateType("person", 1, &Person{}, nil)

	db.CreateType("pet", 1, &Pet{}, loge.LinkSpec{
		"owner": "person",
		"friend": "pet",
	})

	db.Transact(func(trans *loge.Transaction) {
		var prev = trans.Read("person", "brendon").(*Person)

		fmt.Printf("Previous: %v\n", prev)

		var brend = Person{ 
			Name: "Brendon", 
			Age: 32,
			Bits: []uint16{1,4,3},
		}
		
		trans.Set("person", "brendon", &brend)
	}, 0)

	db.Transact(func(trans *loge.Transaction) {
		trans.Delete("person", "brendon")
	}, 0)
}