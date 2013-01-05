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

	db.CreateType("blob", &loge.StringType{})

	var t = db.CreateTransaction()
	t.WriteObj("blob", "brendon")

	db.Transact(func(trans *loge.Transaction) {
		//trans.SetObj("person", "brendon", &Person{ Name: "Brendon", Age: 31 })
		trans.SetObj("blob", "brendon", "Hello World")
	}, 0)

	fmt.Printf("Commit: %v\n", t.Commit())

	//Example(db)
}