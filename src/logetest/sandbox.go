package main

import (
	"loge"
	"fmt"
)

func Sandbox() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/sandbox"))

	db.CreateType("person", 1, &Person{}, loge.LinkSpec{ "friend": "person" })

	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.Set("person", "Brendon", &Person{ "Brendon", 31, nil })
	trans1.Set("person", "Mike", &Person{ "Mike", 38, nil })
	trans1.SetLinks("person", "friend", "Brendon", []loge.LogeKey{ "Mike" })

	fmt.Printf("%v\n", trans1.Find("person", "friend", "Mike").All())

	trans1.Commit()

	fmt.Printf("%v\n", trans2.Find("person", "friend", "Mike").All())

	fmt.Printf("%v\n", db.Find("person", "friend", "Mike").All())

	db.SetOne("person", "Another", &Person{ "Another", 19, nil })
	
	fmt.Printf("%v\n", db.DirtyRead("person", "Another"))

	db.DeleteOne("person", "Another")

	fmt.Printf("%v\n", db.DirtyRead("person", "Another"))
}