package main

import (
	"fmt" 
	"loge"
)

type Person struct {
	Name string
	Age int
}


func main() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/logetest"))

	db.CreateType("blob", loge.StructTypeFor(&Person{}))

	db.Transact(func(trans *loge.Transaction) {
		var prev = trans.ReadObj("blob", "brendon")

		fmt.Printf("Previous: %#v\n", prev)

		
		trans.SetObj("blob", "brendon", 
			&Person{ Name: "Brendon", Age: 31})
	}, 0)

	//Example(db)
}