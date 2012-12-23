package loge

import (
	"fmt"
	"crypto/rand"
	"time"
)

type LogeTypeMap map[string]*LogeType

type LogeDB struct {
	types LogeTypeMap
	store LogeStore
}

func NewLogeDB(store LogeStore) *LogeDB {
	return &LogeDB {
		types: make(LogeTypeMap),
		store: store,
	}
}

func (db *LogeDB) CreateType(name string, exemplar interface{}) *LogeType {
	_, ok := db.types[name]

	if ok {
		panic(fmt.Sprintf("Type exists: '%s'", name))
	}

	var t = &LogeType {
		Name: name,
		Version: 1,
		Exemplar: exemplar,
	}
	db.types[name] = t
	db.store.RegisterType(t)
	
	return t
}


func (db *LogeDB) CreateTransaction() *Transaction {
	return NewTransaction(db)
}


type Transactor func(*Transaction)

func (db *LogeDB) Transact(actor Transactor, timeout time.Duration) bool {
	var start = time.Now()
	for {
		var t = db.CreateTransaction()
		actor(t)
		if t.Commit() {
			return true
		}
		if timeout > 0 && time.Since(start) > timeout {
			break
		}
	}
	return false
}


func (db *LogeDB) CreateObj(typeName string, key string) *LogeObject {
	var lt = db.types[typeName]

	if key == "" {
		key = RandomLogeKey()
	}

	return InitializeObject(key, db, lt)
}


func (db *LogeDB) GetObj(typeName string, key string) *LogeObject {
	return db.store.Get(typeName, key)
}


func (db *LogeDB) EnsureObj(obj *LogeObject) *LogeObject {
	return db.store.Ensure(obj)
}


func RandomLogeKey() string {
	var buf = make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		panic("Couldn't generate key")
	}
	return fmt.Sprintf("%x", buf)
}
