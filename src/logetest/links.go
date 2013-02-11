package main

import (
	"loge"
	"fmt"
)

func LinkSandbox() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/links"))
	defer db.Close()

	db.CreateType(loge.NewTypeDef("person", 1, &Person{}))

	var petDef = loge.NewTypeDef("pet", 1, &Pet{})
	petDef.Links = loge.LinkSpec{ "owner": "person" }
	db.CreateType(petDef)

	db.Transact(func (t *loge.Transaction) {
		t.Set("person", "Brendon", &Person{ "Brendon", 31, []uint16{} })
		t.Set("person", "Mike", &Person{ "Mike", 38, []uint16{} })
		t.Set("pet", "Ted", &Pet { "Ted", "dog" })
		t.Set("pet", "Bones", &Pet { "Bones", "dog" })
		t.Set("pet", "BaoBao", &Pet { "BaoBao", "dog" })
		t.Set("pet", "Ruby", &Pet { "Ruby", "dog" })
		t.Set("pet", "HenYou", &Pet { "HenYou", "dog" })
		t.Set("pet", "Garcon", &Pet { "Garcon", "dog" })
		t.Set("pet", "Flower", &Pet { "Flower", "cat" })

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

	for pet := range db.Find("pet", "owner", "Brendon") {
		fmt.Printf("Found Brendon pet: %s\n", pet)
	}
	
	for pet := range db.Find("pet", "owner", "Mike") {
		fmt.Printf("Found Mike pet: %s\n", pet)
	}
	
	for pet := range db.Find("pet", "owner", "Nai") {
		fmt.Printf("Found Nai pet: %s\n", pet)
	}

	fmt.Printf("Done\n")
}
	

func LinkBench() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/linkbench"))
	defer db.Close()

	db.CreateType(loge.NewTypeDef("person", 1, &Person{}))

	var petDef = loge.NewTypeDef("pet", 1, &Pet{})
	petDef.Links = loge.LinkSpec{ "owner": "person" }
	db.CreateType(petDef)

	fmt.Printf("Inserting...\n")

	db.Transact(func (t *loge.Transaction) {
		t.Set("person", "Brendon", &Person{ "Brendon", 31, []uint16{} })
		for i := 0; i < 10000; i++ {
			var key = fmt.Sprintf("pet-%04d", i)
			t.Set("pet", loge.LogeKey(key), &Pet { key, "dog" })
			t.AddLink("pet", "owner", loge.LogeKey(key), "Brendon")
		}
	}, 0)

	fmt.Printf("Finding...\n")

	var count = 0

	db.Transact(func (t *loge.Transaction) {
		var pets = t.Find("pet", "owner", "Brendon")

		for pets.Valid() {
			pets.Next()
			count++
		}
	}, 0)

	fmt.Printf("Found %d pets\n", count)

	count = 0

	var lastPet loge.LogeKey = ""

	var loops = 0
	for loops < 1000 {
		db.Transact(func (t *loge.Transaction) {
			var somePets = t.FindSlice("pet", "owner", "Brendon", lastPet, 100)
			for somePets.Valid() {
				lastPet = somePets.Next()
				count++
			}
		}, 0)
		loops++
	}

	fmt.Printf("Sliced %d pets\n", count)
}
