package main

import (
	"loge"
)

type Person struct {
	Name string
	Age int
}


func main() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/logetest"))

	db.CreateType("blob", "")

	var t = db.CreateTransaction()
	t.ReadObj("blob", "brendon")

	db.Transact(func(trans *loge.Transaction) {
		//trans.SetObj("person", "brendon", &Person{ Name: "Brendon", Age: 31 })
		trans.SetObj("blob", "brendon", "Hello World")
	}, 0)

	//Example(db)
}