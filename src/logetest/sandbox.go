func Sandbox(db *LogeDB) {
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
	fmt.Printf("Owen's father's age: %d\n", owen.Father.Age)
}

func updatePeople(db *LogeDB) {
	fmt.Printf("~~~~~~~~~~~~~~~~~~\n")

	//var loOwen = db.GetObj("person::Owen")
	//loOwen.Locked = LOCKED
	go (func() {
		time.Sleep(5000 * time.Millisecond)
		//loOwen.Locked = UNLOCKED
	})()

	var trans = db.CreateTransaction()

	fmt.Printf("Transaction: %v\n", trans)

	var brend = trans.WriteObj("person::Brendon").(*Person)
	brend.Age = 59

	var owen = trans.ReadObj("person::Owen").(*Person)

	fmt.Printf("Brend: %v\n", brend)
	fmt.Printf("Owen: %v\n", owen)

	var success = trans.Commit()
	fmt.Printf("Commit success: %v %s\n", success, trans)

}

// func updatePeople(db *LogeDB) {
// 	var loBrend = db.GetObj("person::Brendon")
// 	var brendV2 = loBrend.NewVersion()
// 	var brendV3 = loBrend.NewVersion()

// 	brendV2.Object.(*Person).Age = 59
// 	brendV3.Object.(*Person).Age = 61

// 	var success = loBrend.ApplyVersion(brendV2)
// 	fmt.Printf("Apply success: %v\n", success)

// 	success = loBrend.ApplyVersion(brendV3)
// 	fmt.Printf("Apply success: %v\n", success)
// }