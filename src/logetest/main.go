package main

import (
	"loge"
)

type Person struct {
	Name string
	Age int
}

func main() {
	//loge.Sandbox()

	var db = loge.NewLogeDB(loge.NewLevelDBStore())

	db.CreateType("person", &Person{})

	db.Transact(func(trans *loge.Transaction) {
		trans.SetObj("person", "brendon", &Person{ Name: "Brendon", Age: 31 })
	}, 0)

	//Example(db)
}