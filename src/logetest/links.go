package main

import (
	"loge"
	"fmt"
)

func LinkSandbox() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/links"))
	defer db.Close()

	db.CreateType("person", 1, &Person{}, nil)

	db.CreateType("pet", 1, &Pet{}, loge.LinkSpec{
		"owner": "person",
	})

	db.Transact(func (t *loge.Transaction) {
		t.SetObj("person", "Brendon", &Person{ "Brendon", 31, []uint16{} })
		t.SetObj("person", "Mike", &Person{ "Mike", 38, []uint16{} })
		t.SetObj("pet", "Ted", &Pet { "Ted", "dog" })
		t.SetObj("pet", "Bones", &Pet { "Bones", "dog" })
		t.SetObj("pet", "BaoBao", &Pet { "BaoBao", "dog" })
		t.SetObj("pet", "Ruby", &Pet { "Ruby", "dog" })
		t.SetObj("pet", "HenYou", &Pet { "HenYou", "dog" })
		t.SetObj("pet", "Garcon", &Pet { "Garcon", "dog" })
		t.SetObj("pet", "Flower", &Pet { "Flower", "cat" })

		t.SetLinks("pet", "owner", "Ted", []loge.LogeKey{"Brendon"})
		t.SetLinks("pet", "owner", "Bones", []loge.LogeKey{"Brendon"})
		t.SetLinks("pet", "owner", "BaoBao", []loge.LogeKey{"Brendon"})
		t.SetLinks("pet", "owner", "Ruby", []loge.LogeKey{"Brendon"})
		t.SetLinks("pet", "owner", "HenYou", []loge.LogeKey{"Mike"})
		t.SetLinks("pet", "owner", "Garcon", []loge.LogeKey{"Mike"})
		t.SetLinks("pet", "owner", "Flower", []loge.LogeKey{"Mike"})
	}, 0)

	db.Transact(func (t *loge.Transaction) {
		t.RemoveLink("pet", "owner", "Ruby", "Brendon")
		t.AddLink("pet", "owner", "Ruby", "Mike")
	}, 0)

	db.Transact(func (t *loge.Transaction) {
		fmt.Printf("Ruby links: %v\n", t.ReadLinks("pet", "owner", "Ruby"))
	}, 0)

	var pets = db.Find("pet", "owner", "Brendon")
	
	for pets.Valid() {
		var pet = pets.Next()
		fmt.Printf("Found Brendon pet: %s\n", pet)
	}
	
	pets = db.Find("pet", "owner", "Mike")
	
	for pets.Valid() {
		var pet = pets.Next()
		fmt.Printf("Found Mike pet: %s\n", pet)
	}
	
	pets = db.Find("pet", "owner", "Nai")
	
	for pets.Valid() {
		var pet = pets.Next()
		fmt.Printf("Found Nai pet: %s\n", pet)
	}

	fmt.Printf("Done\n")
}
	

func LinkBench() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/linkbench"))
	defer db.Close()

	db.CreateType("person", 1, &Person{}, nil)

	db.CreateType("pet", 1, &Pet{}, loge.LinkSpec{
		"owner": "person",
	})

	fmt.Printf("Inserting...\n")

	db.Transact(func (t *loge.Transaction) {
		t.SetObj("person", "Brendon", &Person{ "Brendon", 31, []uint16{} })
		for i := 0; i < 10000; i++ {
			var key = fmt.Sprintf("pet-%d", i)
			t.SetObj("pet", loge.LogeKey(key), &Pet { key, "dog" })
			t.AddLink("pet", "owner", loge.LogeKey(key), "Brendon")
		}
	}, 0)

	fmt.Printf("Finding...\n")

	var pets = db.Find("pet", "owner", "Brendon")

	var count = 0

	for pets.Valid() {
		pets.Next()
		count++
	}

	fmt.Printf("Found %d pets\n", count)
}
