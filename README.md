loge -- Go object database
==========================

Current Features:

* Stores Go objects
* Full per-object version history
* Arbitrary ACID transactions
* Increment benchmark (in `logetest/increment.go`) currently runs 1M TPS on my box, or ~250k TPS when lock contention is high

Upcoming features (in approximate order):

* Object reference traversal
* Durability
* Replication and failover (no auto-sharding)
* REST API
* Javascript transactions


Synopsis:
---------

```go
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
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/example"))
	db.CreateType("person", 1, &Person{}, nil)


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
}```

Output:

```bash
$ go install logetest && ./bin/logetest 
Updating type info: person
Existing Brendon: &{Brendon 31 []}
Default value: <nil>
Updated Brendon: &{Brendon 41 []}
Commit 1: true
Commit 2: false
Nai: &{Nai Yu 32 []}
```

Random Notes
------------

* Generally, all DB interaction happens in a transaction
* `ReadObj`, `WriteObj`, and `SetObj` mark an object as important, and the transaction will abort at commit time if the object has changed
* Changes to an object retrieved with `ReadObj` are discarded, unless `WriteObj` or `SetObj` are called for it later in the transaction.
* Object creation (via `SetObj`) follows transaction semantics
* A transaction run by `db.Transact(Func, Timeout)` will retry in a loop until it succeeds or times out
* Manual transactions via `db.CreateTransaction` do not retry
