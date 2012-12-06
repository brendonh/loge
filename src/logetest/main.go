package main

import (
	. "loge"
)




func main() {
	var db = NewLogeDB()
	
	//TestIncrements(db)
	TestCreation(db)
}