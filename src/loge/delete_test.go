package loge

import "testing"


func TestSimpleDelete(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType(NewTypeDef("test", 1, &TestObj{}))

	db.Transact(func (t *Transaction) {
		t.Set("test", "one", &TestObj{Name: "One"})
	}, 0)

	db.Transact(func (t *Transaction) {
		t.Delete("test", "one")
		if t.Exists("test", "one") {
			test.Error("Deleted object exists in same transaction")
		}
	}, 0)

	db.Transact(func (t *Transaction) {
		if t.Exists("test", "one") {
			test.Error("Deleted object exists after commit")
		}
	}, 0)

	db.Transact(func (t *Transaction) {
		t.Set("test", "one", &TestObj{Name: "One Again"})
		if !t.Exists("test", "one") {
			test.Error("Re-created object doesn't exist in same transaction")
		}
	}, 0)

	db.Transact(func (t *Transaction) {
		var one = t.Read("test", "one").(*TestObj)
		if one.Name != "One Again" {
			test.Error("Re-created object has wrong name")
		}
	}, 0)
}


func TestDeleteScoping(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType(NewTypeDef("test", 1, &TestObj{}))

	db.Transact(func (t *Transaction) {
		t.Set("test", "one", &TestObj{Name: "One"})
	}, 0)


	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.Read("test", "one")

	trans2.Delete("test", "one")

	if !trans1.Exists("test", "one") {
		test.Error("Deleted object missing across transaction")
	}

	trans2.Commit()

	if trans1.Commit() {
		test.Error("Commit succeeded with read of deleted object")
	}

}