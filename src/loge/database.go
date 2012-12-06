package loge

import (
	"fmt"
	"crypto/rand"
	"time"
)

type LogeTypeMap map[string]*LogeType
type LogeObjectMap map[string]*LogeObject

type LogeDB struct {
	types LogeTypeMap
	objects LogeObjectMap
}

func NewLogeDB() *LogeDB {
	return &LogeDB {
		types: make(LogeTypeMap),
		objects: make(LogeObjectMap),
	}
}

func (db *LogeDB) CreateType(name string) *LogeType {
	_, ok := db.types[name]

	if ok {
		panic(fmt.Sprintf("Type exists: '%s'", name))
	}

	var t = &LogeType {
		Name: name,
		Version: 1,
	}
	db.types[name] = t
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


func (db *LogeDB) Add(obj Logeable) *LogeObject {
	return db.CreateObj(obj.TypeName(), obj.Key(), obj)
}


func (db *LogeDB) CreateObj(typeName string, key string, object interface{}) *LogeObject {
	lt, ok := db.types[typeName]

	if !ok {
		panic(fmt.Sprintf("Unknown type '%s'", typeName))
	}

	if key == "" {
		key = RandomLogeKey()
	}

	_, ok = db.objects[key]

	if ok {
		panic(fmt.Sprintf("Key exists: '%s'", key))
	}
	
	var obj = InitializeObject(key, object, db, lt)
	db.objects[key] = obj
	return obj
}


func (db *LogeDB) GetObj(key string) *LogeObject {
	return db.objects[key]
}


// For testing only
func (db *LogeDB) Keys() []string {
	var keys []string
	for k := range db.objects {
		keys = append(keys, k)
	}
	return keys
}


func RandomLogeKey() string {
	var buf = make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		panic("Couldn't generate key")
	}
	return fmt.Sprintf("%x", buf)
}
