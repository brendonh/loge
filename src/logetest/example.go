package main

import (
	"loge"
	"fmt"
)

func Example() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/example"))
	db.CreateType(loge.NewTypeDef("person", 1, &Person{}))


	// ------------------------------------
	// Create an object

	db.Transact(func(trans *loge.Transaction) {
		trans.Set("person", "brendon", &Person{ Name: "Brendon", Age: 31 })
	}, 0)


	// ------------------------------------
	// Load some objects

	db.Transact(func(trans *loge.Transaction) {
		if trans.Exists("person", "brendon") {
			var brendon = trans.Write("person", "brendon").(*Person)

			fmt.Printf("Existing Brendon: %v\n", brendon)

			// Update
			brendon.Age = 41
		}

		var defaultObj = trans.Read("person", "someone else").(*Person)
		fmt.Printf("Default value: %v\n", defaultObj)
	}, 0)


	// ------------------------------------
	// Check the update

	db.Transact(func(trans *loge.Transaction) {
		var brendon = trans.Read("person", "brendon").(*Person)
		fmt.Printf("Updated Brendon: %v\n", brendon)
	}, 0)


	// ------------------------------------
	// Intermingle transactions
	
	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.Set("person", "nai", &Person{ Name: "Nai Yu", Age: 32 })
	trans2.Set("person", "nai", &Person{ Name: "Not Nai Yu", Age: 16 })

	fmt.Printf("Commit 1: %v\n", trans1.Commit())
	fmt.Printf("Commit 2: %v\n", trans2.Commit())


	// ------------------------------------
	// Check which succeeded

	db.Transact(func(trans *loge.Transaction) {
		var nai = trans.Read("person", "nai")
		fmt.Printf("Nai: %v\n", nai)
	}, 0)
}