package loge

import "testing"

func TestSimpleUpdate(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType(NewTypeDef("test", 1, &TestObj{}))

	db.Transact(func (t *Transaction) {
		t.Set("test", "one", &TestObj{Name: "One"})
	}, 0)

	db.Transact(func (t *Transaction) {
		var one = t.Write("test", "one").(*TestObj)
		one.Name = "Two"
	}, 0)

	db.Transact(func (t *Transaction) {
		var one = t.Read("test", "one").(*TestObj)
		if one.Name != "Two" {
			test.Error("Simple update failed")
		}
	}, 0)
}

func TestReadWrite(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType(NewTypeDef("test", 1, &TestObj{}))

	db.Transact(func (t *Transaction) {
		t.Read("test", "one")
		t.Set("test", "one", &TestObj{Name: "One"})
	}, 0)

	db.Transact(func (t *Transaction) {
		var one = t.Read("test", "one").(*TestObj)
		if one == nil || one.Name != "One" {
			test.Error("ReadWrite failed")
		}
	}, 0)
}

func TestReadScoping(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType(NewTypeDef("test", 1, &TestObj{}))

	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.Set("test", "one", &TestObj{Name: "One"})

	trans1.Commit()

	if trans2.Read("test", "one").(*TestObj) != nil {
		test.Errorf("Version visible in transaction created before obj create")
	}

	// Now on existing object

	var trans3 = db.CreateTransaction()
	var trans4 = db.CreateTransaction()

	trans3.Set("test", "one", &TestObj{Name: "Two"})
	trans3.Commit()

	if trans4.Read("test", "one").(*TestObj).Name != "One" {
		test.Errorf("Version visible in transaction created before update")
	}

	if db.ReadOne("test", "one").(*TestObj).Name != "Two" {
		test.Errorf("One-shot read got wrong version")
	}
}

func TestUpdateScoping(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType(NewTypeDef("test", 1, &TestObj{}))

	db.Transact(func (t *Transaction) {
		t.Set("test", "one", &TestObj{Name: "One"})
		t.Set("test", "two", &TestObj{Name: "Two"})
	}, 0)


	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.Read("test", "one")
	var one1 = trans1.Write("test", "one").(*TestObj)
	one1.Name = "One Update"

	var two2 = trans2.Write("test", "two").(*TestObj)
	two2.Name = "Two Update"

	var test2 = trans1.Read("test", "two").(*TestObj)

	if test2.Name != "Two" {
		test.Errorf("Update visible across transactions before commit (%v)", test2.Name)
	}

	trans2.Read("test", "one")

	if !trans1.Commit() {
		test.Error("Update 1 failed with no object conflict")
	}

	if trans2.Read("test", "one").(*TestObj).Name != "One" {
		test.Error("Transaction got update for already-read object")
	}

	if trans2.Commit() {
		test.Error("Update 2 succeeded with read version conflict")
	}
}


func TestUpdateConflict(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType(NewTypeDef("test", 1, &TestObj{}))

	db.Transact(func (t *Transaction) {
		t.Set("test", "one", &TestObj{Name: "One"})
	}, 0)

	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.Set("test", "one", &TestObj{Name: "One Update"})
	trans2.Set("test", "one", &TestObj{Name: "Two Update"})

	if !trans2.Commit() {
		test.Error("Commit 2 failed")
	}

	if trans1.Commit() {
		test.Error("Commit 1 succeeded")
	}

	db.Transact(func (t *Transaction) {
		if t.Read("test", "one").(*TestObj).Name != "Two Update" {
			test.Error("Wrong name after update")
		}
	}, 0)
}