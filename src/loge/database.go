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

func (db *LogeDB) CreateType(def *TypeDef) *logeType {
	var vt = db.store.getSpackType(def.Name)

	var spackExemplar interface{}
	if def.Exemplar != nil {
		spackExemplar = reflect.ValueOf(def.Exemplar).Elem().Interface()
	} else {
		spackExemplar = nil
	}

	vt.AddVersion(def.Version, spackExemplar, def.Upgrader)
	var typ = newType(def.Name, def.Version, def.Exemplar, def.Links, vt)
	db.types[typ.Name] = typ
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
	var t = db.CreateTransaction()
	return db.doTransact(t, actor, timeout)
}

func (db *LogeDB) TransactJSON(actor Transactor, timeout time.Duration) bool {
	var t = db.CreateTransaction()
	t.giveJSON = true
	return db.doTransact(t, actor, timeout)
}

func (db *LogeDB) doTransact(t *Transaction, actor Transactor, timeout time.Duration) bool {
	var start = time.Now()
	for {
		actor(t)
		if t.cancelled {
			return false
		}
		if t.Commit() {
			return true
		}
		if t.state != ABORTED {
			break
		}
		if timeout > 0 && time.Since(start) > timeout {
			break
		}
	}
	return false
}

// -----------------------------------------------
// One-shot Operations
// -----------------------------------------------

func (db *LogeDB) ExistsOne(typeName string, key LogeKey) (exists bool) {
	db.Transact(func (t *Transaction) {
		exists = t.Exists(typeName, key)
	}, 0)
	return
}

func (db *LogeDB) ReadOne(typeName string, key LogeKey) (obj interface{}) {
	db.Transact(func (t *Transaction) {
		obj = t.Read(typeName, key)
	}, 0)
	return
}

func (db *LogeDB) ReadLinksOne(typeName string, linkName string, key LogeKey) (links []string) {
	db.Transact(func (t *Transaction) {
		links = t.ReadLinks(typeName, linkName, key)
	}, 0)
	return
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

func (db *LogeDB) Find(typeName string, linkName string, target LogeKey) (results []LogeKey) {
	db.Transact(func (t *Transaction) {
		results = t.Find(typeName, linkName, target).All()
	}, 0)
	return
}

func (db *LogeDB) FindSlice(typeName string, linkName string, target LogeKey, from LogeKey, limit int) (results []LogeKey) {	
	db.Transact(func (t *Transaction) {
		results = t.FindSlice(typeName, linkName, target, from, limit).All()
	}, 0)
	return
}

func (db *LogeDB) ListSlice(typeName string, from LogeKey, limit int) (results []LogeKey) {	
	db.Transact(func (t *Transaction) {
		results = t.ListSlice(typeName, from, limit).All()
	}, 0)
	return
}

// -----------------------------------------------
// Internals
// -----------------------------------------------

func (db *LogeDB) makeObjRef(typeName string, key LogeKey) objRef {
	typ, ok := db.types[typeName]
	if !ok {
		panic(fmt.Sprintf("Type not registered: %s", typeName))
	}
	return makeObjRef(typ, key)
}

func (db *LogeDB) makeLinkRef(typeName string, linkName string, key LogeKey) objRef {
	typ, ok := db.types[typeName]
	if !ok {
		panic(fmt.Sprintf("Type not registered: %s", typeName))
	}
	return makeLinkRef(typ, linkName, key)
}


func (db *LogeDB) acquireVersion(ref objRef, context transactionContext, load bool) *objectVersion {
	var typeName = ref.Type.Name
	var key = ref.Key

	var objKey = ref.String()
	var typ = db.types[typeName]

	db.lock.SpinLock()
	var obj, ok = db.cache[objKey]

	if !ok {
		obj = initializeObject(db, typ, key)
		if ref.IsLink() { 
			obj.LinkName = ref.LinkName
		}
	}

	obj.Lock.SpinLock()
	defer obj.Lock.Unlock()

	db.cache[objKey] = obj	
	db.lock.Unlock()

	obj.RefCount++

	var version = obj.ensureVersion(context.getSnapshotID())

	if load && !version.loaded {
		version.Blob = context.get(ref)
		version.loaded = true
	}

	return version
}


func (db *LogeDB) releaseVersions(versions []*liveVersion) {
	db.lock.SpinLock()
	defer db.lock.Unlock()

	for _, lv := range versions {
		var obj = lv.version.LogeObj
		obj.RefCount--
		if obj.RefCount == 0 {
			delete(db.cache, obj.makeObjRef().CacheKey)
		}
	}
}

