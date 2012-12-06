package main

import (
	. "loge"

	"fmt"
	"time"
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
	
	TestIncrements(db)
}