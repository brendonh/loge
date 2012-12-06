package main

import (
	. "loge"

	"fmt"
)


type Person struct {
	Name string
	Age int

	Father *Person

	Children []*Person `loge:"copy"`
	Temp []*Person
}

func (p *Person) TypeName() string {
	return "person"
}

func (p *Person) Key() string {
	return fmt.Sprintf("%s::%s", p.TypeName(), p.Name)
}


func main() {
	var db = NewLogeDB()

	fmt.Printf("DB: %v\n", db)

	db.CreateType("person")

	initPeople(db)
	printPeople(db)
	updatePeople(db)
	printPeople(db)
}

func initPeople(db *LogeDB) {
	var brend = &Person{
		Name: "Brendon",
		Age: 31,
		Children: []*Person{},
	}
	db.Add(brend)

	var owen = &Person{
		Name: "Owen",
		Age: 1,
		Father: brend,
	}
	db.Add(owen)
}

func printPeople(db *LogeDB) {
	fmt.Printf("~~~~~~~~~~~~~~~~~~\n")
	fmt.Printf("Keys: %v\n", db.Keys())

	var loBrend = db.GetObj("person::Brendon")
	var brend = loBrend.Current.Object.(*Person)
	fmt.Printf("Brend: %s (%d) %v\n", loBrend.Key, loBrend.Current.Version, brend)

	var loOwen = db.GetObj("person::Owen")
	var owen = loOwen.Current.Object.(*Person)
	fmt.Printf("Owen: %s (%d) %v\n", loOwen.Key, loOwen.Current.Version, owen)
}

func updatePeople(db *LogeDB) {
	var loBrend = db.GetObj("person::Brendon")
	var brendV2 = loBrend.NewVersion()
	var brendV3 = loBrend.NewVersion()

	brendV2.Object.(*Person).Age = 59
	brendV3.Object.(*Person).Age = 61

	var success = loBrend.ApplyVersion(brendV2)
	fmt.Printf("Apply success: %v\n", success)

	success = loBrend.ApplyVersion(brendV3)
	fmt.Printf("Apply success: %v\n", success)

}