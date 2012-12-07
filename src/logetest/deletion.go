package main

import (
	. "loge"

	"fmt"
)


type Deletable struct {
	Name string
}


func TestDeletion(db *LogeDB) {
	db.CreateType("deletable", &Deletable{})

	db.Transact(func (t *Transaction) {
		t.SetObj("deletable", "one", &Deletable{Name: "One"})
	}, 0)

	db.Transact(func (t *Transaction) {
		var one = t.ReadObj("deletable", "one").(*Deletable)
		fmt.Printf("Pre-delete: %v\n", one)
	}, 0)

	db.Transact(func (t *Transaction) {
		t.DeleteObj("deletable", "one")
	}, 0)

	db.Transact(func (t *Transaction) {
		var one = t.ReadObj("deletable", "one").(*Deletable)
		fmt.Printf("Post-delete: %v\n", one)
		fmt.Printf("Exists: %v\n", t.Exists("deletable", "one"))
	}, 0)

	db.Transact(func (t *Transaction) {
		fmt.Printf("Exists: %v\n", t.Exists("deletable", "one"))
		t.SetObj("deletable", "one", &Deletable{Name: "One Again"})
	}, 0)

	db.Transact(func (t *Transaction) {
		fmt.Printf("Exists: %v\n", t.Exists("deletable", "one"))
		var one = t.ReadObj("deletable", "one").(*Deletable)
		fmt.Printf("Recreated: %v\n", one)
	}, 0)

}