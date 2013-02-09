package loge

import (
	"testing"
	"reflect"
)

func TestDirtyOps(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType("test", 1, &TestObj{}, LinkSpec{ "other": "test" })

	db.Transact(func (t *Transaction) {
		t.Set("test", "foo", &TestObj{ "foo" })
		t.Set("test", "bar", &TestObj{ "bar" })
		
		t.AddLink("test", "other", "foo", "bar")
		t.AddLink("test", "other", "bar", "foo")
	}, 0)

	if !db.DirtyExists("test", "foo") {
		test.Error("Dirty foo doesn't exist")
	}

	if !db.DirtyExists("test", "bar") {
		test.Error("Dirty bar doesn't exist")
	}

	if db.DirtyExists("test", "wib") {
		test.Error("Dirty wib exists")
	}

	if db.DirtyRead("test", "foo").(*TestObj).Name != "foo" {
		test.Error("Dirty read has wrong name")
	}

	if db.DirtyRead("test", "bar").(*TestObj).Name != "bar" {
		test.Error("Dirty read has wrong name")
	}

	var wib = db.DirtyRead("test", "wib").(*TestObj)
	if wib != nil {
		test.Errorf("Missing obj is not nil (%v)", wib)
	}

	var fooLinks = db.DirtyReadLinks("test", "other", "foo")
	if !reflect.DeepEqual(fooLinks, []string{ "bar" }) {
		test.Errorf("Wrong dirty links: %v", fooLinks)
	}

	var barLinks = db.DirtyReadLinks("test", "other", "bar")
	if !reflect.DeepEqual(barLinks, []string{ "foo" }) {
		test.Errorf("Wrong dirty links: %v", barLinks)
	}

	var wibLinks = db.DirtyReadLinks("test", "other", "wib")
	if !reflect.DeepEqual(wibLinks, []string{}) {
		test.Errorf("Wrong dirty links: %v", wibLinks)
	}
}