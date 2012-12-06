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

	// var trans = db.CreateTransaction()
	// var trans2 = db.CreateTransaction()

	// trans.SetObj("created", "foo", &Created{Name: "One"})
	// trans2.SetObj("created", "foo", &Created{Name: "Two"})
	
	// fmt.Printf("Commit 1: %v\n", trans.Commit())
	// fmt.Printf("Commit 2: %v\n", trans2.Commit())

	// fmt.Printf("Obj: %v\n", db.GetObj("created", "foo").Current.Object)

	// fmt.Printf("---------------------\n")

	// trans = db.CreateTransaction()
	// trans.SetObj("created", "foo", &Created{Name: "Three"})
	// fmt.Printf("Commit 3: %v\n", trans.Commit())

	// fmt.Printf("Obj: %v\n", db.GetObj("created", "foo").Current.Object)

	// fmt.Printf("---------------------\n")

	// trans = db.CreateTransaction()
	// trans.SetObj("created", "bar", &Created{Name: "Four"})

	// trans2 = db.CreateTransaction()

	// fmt.Printf("Bar exists: %v\n", trans2.Exists("created", "bar"))

	// trans2.SetObj("created", "bar", &Created{Name: "Bar"})

	// var foo = trans2.WriteObj("created", "foo").(*Created)

	// foo.Name = "Five"

	// fmt.Printf("Commit 4: %v\n", trans2.Commit())

	// fmt.Printf("Foo: %v\n", db.GetObj("created", "foo").Current.Object)
	// fmt.Printf("Bar: %v\n", db.GetObj("created", "bar").Current.Object)

	// fmt.Printf("---------------------\n")

	var trans = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans.SetObj("created", "wib", &Created{Name: "Wib"})
	trans.SetObj("created", "wob", &Created{Name: "Wob"})

	fmt.Printf("Wib exists in 2: %v\n", trans2.Exists("created", "wib"))
	trans2.SetObj("created", "wob", &Created{Name: "Wob2"})

	fmt.Printf("Success 1: %v\n", trans.Commit())
	fmt.Printf("Success 2: %v\n", trans2.Commit())

	fmt.Printf("Wib: %v\n", db.GetObj("created", "wib").Current.Object)
	fmt.Printf("Wob: %v\n", db.GetObj("created", "wob").Current.Object)


}