package loge

import "testing"

type TestObj struct {
	Name string
}


func TestSimpleCreation(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType("test", 1, &TestObj{})
	
	db.Transact(func(t *Transaction) {
		t.SetObj("test", "one", &TestObj{Name: "One"})
		
		var one = t.ReadObj("test", "one").(*TestObj)
		if one.Name != "One" {
			test.Error("Created object missing in transaction")
		}
	}, 0)

	db.Transact(func(t *Transaction) {
		var one = t.ReadObj("test", "one").(*TestObj)
		if one.Name != "One" {
			test.Error("Created object missing after transaction")
		}
	}, 0)
}


func TestCreationScoping(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType("test", 1, &TestObj{})

	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()
	var trans3 = db.CreateTransaction()

	trans1.SetObj("test", "one", &TestObj{Name: "One"})

	if trans2.Exists("test", "one") {
		test.Error("Created object visible across transactions")
	}

	trans1.Commit()

	if trans2.Exists("test", "one") {
		test.Error("Created object became visible after commit")
	}

	if !trans3.Exists("test", "one") {
		test.Error("Created object not visible when first read after commit")
	}
}


func TestOverlappingCreation(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType("test", 1, &TestObj{})

	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.SetObj("test", "one", &TestObj{Name: "One"})
	trans2.SetObj("test", "one", &TestObj{Name: "Two"})

	trans1.Commit()

	if trans2.Commit() {
		test.Error("Transaction succeeded with double-created object")
	}

	trans1 = db.CreateTransaction()
	trans2 = db.CreateTransaction()

	trans1.SetObj("test", "two", &TestObj{Name: "One"})
	trans2.SetObj("test", "two", &TestObj{Name: "Two"})

	trans2.Commit()

	if trans1.Commit() {
		test.Error("Transaction succeeded with double-created object")
	}

}