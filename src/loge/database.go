package loge

import (
	"fmt"
	"time"
	"sync/atomic"
	"reflect"

	"github.com/brendonh/spack"
)

type LogeDB struct {
	types typeMap
	store LogeStore
	cache objCache
	lastSnapshotID uint64
	lock spinLock
	linkTypeSpec *spack.TypeSpec
}

func NewLogeDB(store LogeStore) *LogeDB {
	return &LogeDB {
		types: make(typeMap),
		store: store,
		cache: make(objCache),
		lastSnapshotID: 1,
		linkTypeSpec: spack.MakeTypeSpec([]string{}),
	}
}


type typeMap map[string]*logeType

type objCache map[string]*logeObject

type Transactor func(*Transaction)


func (db *LogeDB) Close() {
	db.store.close()
}

func (db *LogeDB) CreateType(name string, version uint16, exemplar interface{}, linkSpec LinkSpec) *logeType {
	_, ok := db.types[name]

	if ok {
		panic(fmt.Sprintf("Type exists: '%s'", name))
	}

	var vt = db.store.getSpackType(name)
	var spackExemplar = reflect.ValueOf(exemplar).Elem().Interface()
	vt.AddVersion(version, spackExemplar, nil)
	var typ = NewType(name, version, exemplar, linkSpec, vt)

	db.types[name] = typ
	db.store.registerType(typ)
	
	return typ
}


func (db *LogeDB) CreateTransaction() *Transaction {
	var tID = db.lastSnapshotID
	return NewTransaction(db, tID)
}

func (db *LogeDB) newSnapshotID() uint64 {
	return atomic.AddUint64(&db.lastSnapshotID, 1)
}

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

func (db *LogeDB) Find(typeName string, linkName string, target LogeKey) ResultSet {
	return db.store.find(makeLinkRef(typeName, linkName, ""), target)
}

func (db *LogeDB) FindFrom(typeName string, linkName string, target LogeKey, from LogeKey, limit int) ResultSet {	
	return db.store.findSlice(makeLinkRef(typeName, linkName, ""), target, from, limit)
}


func (db *LogeDB) FlushCache() int {
	var count = 0
	db.lock.SpinLock()
	defer db.lock.Unlock()
	for key, obj := range db.cache {
		if obj.RefCount == 0 {
			delete(db.cache, key)
			count++
		}
	}
	return count
}

// -----------------------------------------------
// One-shot Operations
// -----------------------------------------------

func (db *LogeDB) ExistsOne(typeName string, key LogeKey) bool {
	var obj = db.store.get(makeObjRef(typeName, key))
	return obj != nil
}

func (db *LogeDB) ReadOne(typeName string, key LogeKey) interface{} {
	var typ = db.types[typeName]
	return typ.Decode(db.store.get(makeObjRef(typeName, key)))
}

func (db *LogeDB) ReadLinksOne(typeName string, linkName string, key LogeKey) []string {
	var blob = db.store.get(makeLinkRef(typeName, linkName, key))
	var links linkList
	spack.DecodeFromBytes(&links, db.linkTypeSpec, blob)
	return links
}

func (db *LogeDB) SetOne(typeName string, key LogeKey, obj interface{}) {
	db.Transact(func (t *Transaction) {
		t.Set(typeName, key, obj)
	}, 0)
}

func (db *LogeDB) DeleteOne(typeName string, key LogeKey) {
	db.Transact(func (t *Transaction) {
		t.Delete(typeName, key)
	}, 0)
}

// -----------------------------------------------
// Internals
// -----------------------------------------------

func (db *LogeDB) ensureObj(ref objRef, load bool) *logeObject {
	var typeName = ref.TypeName
	var key = ref.Key

	var objKey = ref.String()
	var typ = db.types[typeName]

	db.lock.SpinLock()
	var obj, ok = db.cache[objKey]

	if ok && (obj.Loaded || !load) {
		db.lock.Unlock()
		return obj
	}

	if !ok {
		obj = initializeObject(db, typ, key)
	} 

	obj.Lock.SpinLock()
	defer obj.Lock.Unlock()

	db.cache[objKey] = obj	

	db.lock.Unlock()

	var version *objectVersion

	var blob []byte
	if load {
		blob = db.store.get(ref)
		obj.Loaded = true
	}

	version = &objectVersion {
		LogeObj: obj,
		Blob: blob,
	}

	if ref.IsLink() { 
		obj.LinkName = ref.LinkName

	}

	obj.Current = version
	return obj
}


