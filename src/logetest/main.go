package main

import (
	"fmt" 
	"loge"
)

type Person struct {
	Name string
	Age uint32
	Bits []uint16 `loge:"copy"`
}

type Pet struct {
	Name string
	Species string
}

func main() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/logetest"))

	db.CreateType("person", 1, &Person{}, nil)

	db.CreateType("pet", 1, &Pet{}, loge.LinkSpec{
		"owner": "person",
		"friend": "pet",
	})

	db.Transact(func(trans *loge.Transaction) {
		var prev = trans.ReadObj("person", "brendon").(*Person)

		fmt.Printf("Previous: %v\n", prev)

		var brend = Person{ 
			Name: "Brendon", 
			Age: 32,
			Bits: []uint16{1,4,3},
		}
		
		trans.SetObj("person", "brendon", &brend)

		var ted = Pet{
			Name: "Ted",
			Species: "Hairball",
		}
		trans.SetObj("pet", "ted", &ted)

		var owner = trans.ReadLinks("pet", "owner", "ted")
		fmt.Printf("Owner: %v\n", owner)

		trans.AddLink("pet", "owner", "ted", "brendon")

	}, 0)

	//Example(db)
}