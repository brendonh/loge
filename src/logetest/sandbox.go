package main

import (
	"loge"
	"fmt"
)

type Version0 struct {
	Name string
}

type Version1 struct {
	Name string
	Age uint32
}

func Sandbox() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/sandbox"))

	db.CreateType(loge.NewTypeDef("person", 1, &Version0{}))

	db.SetOne("person", "brendon", &Version0{ "Brendon" })
	fmt.Printf("Version 0: %v\n", db.ReadOne("person", "brendon"))

	var def2 = loge.NewTypeDef("person", 2, &Version1{})
	def2.Upgrader = func(v0 interface{}) (interface{}, error) {
		var obj = v0.(*Version0)
		return &Version1{ obj.Name, 18 }, nil
	}
	db.CreateType(def2)

	fmt.Printf("Version 1: %v\n", db.ReadOne("person", "brendon"))
	fmt.Printf("Version 1: %v\n", db.ReadOne("person", "brendon"))
	fmt.Printf("Version 1: %v\n", db.ReadOne("person", "brendon"))

}