package main

import (
	. "loge"

	"fmt"
)


type Person struct {
	Loge *LogeObject

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

	var brend = &Person{
		Name: "Brendon",
		Age: 31,
		Children: []*Person {},
	}
	db.Add(brend)

	var owen = &Person {
		Name: "Owen",
		Age: 1,
		Father: brend,
	}
	db.Add(owen)

	brend.Children = append(brend.Children, owen)
	brend.Temp = append(brend.Temp, owen, brend, owen)

	var brend2 = brend.Loge.Update().(*Person)
	brend2.Age = 59

	fmt.Printf("Brend: %s::%d %v\n", brend.Loge.Key, brend.Loge.Version, brend)
	//fmt.Printf("Owen: %s %v\n", owen.Loge.Key, owen)

	fmt.Printf("Brend2: %s::%d %v\n", brend2.Loge.Key, brend2.Loge.Version, brend2)

}