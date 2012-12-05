package loge

import (
	"fmt"
	"crypto/rand"
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


func (db *LogeDB) Add(obj Logeable) {
	db.CreateObj(obj.TypeName(), obj.Key(), obj)
}


func (db *LogeDB) CreateObj(typeName string, key string, object interface{}) {
	lt, ok := db.types[typeName]

	if !ok {
		panic(fmt.Sprintf("Unknown type '%s'", typeName))
	}

	if key == "" {
		key = RandomLogeKey()
	}
	
	var obj = &LogeObject{
		DB: db,
		Type: lt,
		Key: key,
		Version: 0,
		Dirty: true,
		TransactionCount: 0,
		Object: object,
	}
	obj.SetOnObject()
}


func (db *LogeDB) GetObj(key string) *LogeObject {
	return db.objects[key]
}


func RandomLogeKey() string {
	var buf = make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		panic("Couldn't generate key")
	}
	return fmt.Sprintf("%x", buf)
}
