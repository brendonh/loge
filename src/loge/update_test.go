package loge

import "testing"


func TestSimpleUpdate(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType("test", &TestObj{})

	db.Transact(func (t *Transaction) {
		t.SetObj("test", "one", &TestObj{Name: "One"})
	}, 0)

	db.Transact(func (t *Transaction) {
		var one = t.WriteObj("test", "one").(*TestObj)
		one.Name = "Two"
	}, 0)

	db.Transact(func (t *Transaction) {
		var one = t.ReadObj("test", "one").(*TestObj)
		if one.Name != "Two" {
			test.Error("Simple update failed")
		}
	}, 0)
}


func TestUpdateScoping(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType("test", &TestObj{})

	db.Transact(func (t *Transaction) {
		t.SetObj("test", "one", &TestObj{Name: "One"})
		t.SetObj("test", "two", &TestObj{Name: "Two"})
	}, 0)

	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	var one1 = trans1.WriteObj("test", "one").(*TestObj)
	one1.Name = "One Update"

	var two2 = trans2.WriteObj("test", "two").(*TestObj)
	two2.Name = "Two Update"

	if trans1.ReadObj("test", "two").(*TestObj).Name != "Two" {
		test.Error("Update visible across transactions before commit")
	}

	trans2.ReadObj("test", "one")

	if !trans1.Commit() {
		test.Error("Update 1 failed with no object conflict")
	}

	if trans2.ReadObj("test", "one").(*TestObj).Name != "One" {
		test.Error("Transaction got update for already-read object")
	}

	if trans2.Commit() {
		test.Error("Update 2 succeeded with read version conflict")
	}
}


func TestUpdateConflict(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType("test", &TestObj{})

	db.Transact(func (t *Transaction) {
		t.SetObj("test", "one", &TestObj{Name: "One"})
	}, 0)

	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.SetObj("test", "one", &TestObj{Name: "One Update"})
	trans2.SetObj("test", "one", &TestObj{Name: "Two Update"})

	if !trans2.Commit() {
		test.Error("Commit 2 failed")
	}

	if trans1.Commit() {
		test.Error("Commit 1 succeeded")
	}

	db.Transact(func (t *Transaction) {
		if t.ReadObj("test", "one").(*TestObj).Name != "Two Update" {
			test.Error("Wrong name after update")
		}
	}, 0)
}