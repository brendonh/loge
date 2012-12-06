package main

import (
	. "loge"

	"fmt"
)


type Created struct {
	Name string
}


func TestCreation(db *LogeDB) {
	db.CreateType("created", &Created{})

	var trans = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans.SetObj("created", "foo", &Created{Name: "One"})
	trans2.SetObj("created", "foo", &Created{Name: "Two"})
	
	fmt.Printf("Commit 1: %v\n", trans.Commit())
	fmt.Printf("Commit 2: %v\n", trans2.Commit())

	fmt.Printf("Obj: %v\n", db.GetObj("created", "foo").Current.Object)

	trans = db.CreateTransaction()
	trans.SetObj("created", "foo", &Created{Name: "Three"})
	fmt.Printf("Commit 3: %v\n", trans.Commit())

	fmt.Printf("Obj: %v\n", db.GetObj("created", "foo").Current.Object)


	trans = db.CreateTransaction()
	trans.SetObj("created", "bar", &Created{Name: "Four"})

	trans2 = db.CreateTransaction()

	var bar = trans2.ReadObj("created", "bar").(*Created)
	fmt.Printf("Bar: %v\n", bar)

	var foo = trans2.WriteObj("created", "foo").(*Created)

	foo.Name = "Five"

	fmt.Printf("Commit 4: %v\n", trans2.Commit())

	fmt.Printf("Foo: %v\n", db.GetObj("created", "foo").Current.Object)
}