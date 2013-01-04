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
		Cache: make(objCache),
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

func (db *LogeDB) EnsureObj(typeName string, key string) *LogeObject {
	var typ = db.types[typeName]
	
	var obj, ok = typ.Cache[key]
	if ok {
		return obj
	}

	obj = db.store.Get(typ, key)

	if obj == nil {
		obj = InitializeObject(key, db, typ)
	}

	// Lock after the get, to hold it as briefly as possible
	typ.Mutex.Lock()
	defer typ.Mutex.Unlock()

	// Maybe it got created while we were getting
	obj2, ok := typ.Cache[key]
	if ok {
		return obj2
	}

	typ.Cache[key] = obj
	return obj
}


func (db *LogeDB) StoreObj(obj *LogeObject) {
	db.store.Store(obj)
}

func RandomLogeKey() string {
	var buf = make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		panic("Couldn't generate key")
	}
	return fmt.Sprintf("%x", buf)
}
