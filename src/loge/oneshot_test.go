package loge

import (
	"testing"
	"reflect"
)

func TestOneshotOps(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
	db.CreateType("test", 1, &TestObj{}, LinkSpec{ "other": "test" })

	db.Transact(func (t *Transaction) {
		t.Set("test", "foo", &TestObj{ "foo" })
		t.Set("test", "bar", &TestObj{ "bar" })
		
		t.AddLink("test", "other", "foo", "bar")
		t.AddLink("test", "other", "bar", "foo")
	}, 0)

	if !db.ExistsOne("test", "foo") {
		test.Error("One-shot foo doesn't exist")
	}

	if !db.ExistsOne("test", "bar") {
		test.Error("One-shot bar doesn't exist")
	}

	if db.ExistsOne("test", "wib") {
		test.Error("One-shot wib exists")
	}

	if db.ReadOne("test", "foo").(*TestObj).Name != "foo" {
		test.Error("One-shot read has wrong name")
	}

	if db.ReadOne("test", "bar").(*TestObj).Name != "bar" {
		test.Error("One1shot read has wrong name")
	}

	var wib = db.ReadOne("test", "wib").(*TestObj)
	if wib != nil {
		test.Errorf("Missing obj is not nil (%v)", wib)
	}

	var fooLinks = db.ReadLinksOne("test", "other", "foo")
	if !reflect.DeepEqual(fooLinks, []string{ "bar" }) {
		test.Errorf("Wrong one-shot links: %v", fooLinks)
	}

	var barLinks = db.ReadLinksOne("test", "other", "bar")
	if !reflect.DeepEqual(barLinks, []string{ "foo" }) {
		test.Errorf("Wrong one-shot links: %v", barLinks)
	}

	var wibLinks = db.ReadLinksOne("test", "other", "wib")
	if len(wibLinks) != 0 {
		test.Errorf("Wrong one-shot links: %#v", wibLinks)
	}
}