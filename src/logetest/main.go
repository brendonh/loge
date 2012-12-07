package main

import (
	"loge"
)

type Person struct {
	Name string
	Age int
}

func main() {
	var db = loge.NewLogeDB()

	Example(db)
}