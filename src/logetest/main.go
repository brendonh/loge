package main

import (
	"loge"
	"fmt"
)

type Person struct {
	Name string
	Age int
}

func main() {
	var db = loge.NewLogeDB()

	db.CreateType("person", &Person{})


	// ------------------------------------
	// Create an object

	db.Transact(func(trans *loge.Transaction) {
		trans.SetObj("person", "brendon", &Person{ Name: "Brendon", Age: 31 })
	}, 0)


	// ------------------------------------
	// Load some objects

	db.Transact(func(trans *loge.Transaction) {
		if trans.Exists("person", "brendon") {
			var brendon = trans.WriteObj("person", "brendon").(*Person)

			fmt.Printf("Existing Brendon: %v\n", brendon)

			// Update
			brendon.Age = 41
		}

		var defaultObj = trans.ReadObj("person", "someone else").(*Person)
		fmt.Printf("Default value: %v\n", defaultObj)
	}, 0)


	// ------------------------------------
	// Check the update

	db.Transact(func(trans *loge.Transaction) {
		var brendon = trans.ReadObj("person", "brendon").(*Person)
		fmt.Printf("Updated Brendon: %v\n", brendon)
	}, 0)


	// ------------------------------------
	// Intermingle transactions
	
	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.SetObj("person", "nai", &Person{ Name: "Nai Yu", Age: 32 })
	trans2.SetObj("person", "nai", &Person{ Name: "Not Nai Yu", Age: 16 })

	fmt.Printf("Commit 1: %v\n", trans1.Commit())
	fmt.Printf("Commit 2: %v\n", trans2.Commit())


	// ------------------------------------
	// Check which succeeded

	db.Transact(func(trans *loge.Transaction) {
		var nai = trans.ReadObj("person", "nai")
		fmt.Printf("Nai: %v\n", nai)
	}, 0)
	
}