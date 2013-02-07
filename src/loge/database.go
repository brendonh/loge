package loge

import (
	"fmt"
	"crypto/rand"
	"time"
)

type typeMap map[string]*LogeType

type objCache map[string]*LogeObject

type LogeDB struct {
	types typeMap
	store LogeStore
	cache objCache
	lock SpinLock
}

func NewLogeDB(store LogeStore) *LogeDB {
	return &LogeDB {
		types: make(typeMap),
		store: store,
		cache: make(objCache),
	}
}

type ObjRef struct {
	TypeName string
	Key LogeKey
	LinkName string
	CacheKey string
}

func MakeObjRef(typeName string, key LogeKey) ObjRef {
	var cacheKey = typeName + "^" + string(key)
	return ObjRef { typeName, key, "", cacheKey }
}

func MakeLinkRef(typeName string, linkName string, key LogeKey) ObjRef {
	var cacheKey = "^" + typeName + "^" + linkName + "^" + string(key)
	return ObjRef { typeName, key, linkName, cacheKey }
}

func (objRef ObjRef) String() string {
	return objRef.CacheKey
}

func (objRef ObjRef) IsLink() bool {
	return objRef.LinkName != ""
}


func (db *LogeDB) Close() {
	db.store.Close()
}

func (db *LogeDB) CreateType(name string, version uint16, exemplar interface{}, linkSpec LinkSpec) *LogeType {
	_, ok := db.types[name]

	if ok {
		panic(fmt.Sprintf("Type exists: '%s'", name))
	}

	var linkInfo = make(map[string]*LinkInfo)
	for k, v := range linkSpec {
		linkInfo[k] = &LinkInfo {
			Name: k,
			Target: v,
			Tag: 0,
		}
	}

	var t = &LogeType {
		Name: name,
		Version: version,
		Exemplar: exemplar,
		Links: linkInfo,
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

func (db *LogeDB) EnsureObj(objRef ObjRef, load bool) *LogeObject {
	var typeName = objRef.TypeName
	var key = objRef.Key

	var objKey = objRef.String()
	var typ = db.types[typeName]

	db.lock.SpinLock()
	var obj, ok = db.cache[objKey]

	if ok && (obj.Loaded || !load) {
		db.lock.Unlock()
		return obj
	}

	if !ok {
		obj = InitializeObject(db, typ, key)
	}

	obj.Lock.SpinLock()
	defer obj.Lock.Unlock()

	db.cache[objKey] = obj	

	db.lock.Unlock()

	var version *LogeObjectVersion
	if objRef.IsLink() { 
		var links Links
		if load {
			links = db.store.GetLinks(typ, objRef.LinkName, key)
			obj.Loaded = true
		}

		var linkSet = NewLinkSet()
		linkSet.Original = links
		version = &LogeObjectVersion {
			LogeObj: obj,
			Version: 0,
			Object: linkSet,
		}
		obj.LinkName = objRef.LinkName

	} else {
		var object interface{}
		
		if load {
			object = db.store.Get(typ, key)
			obj.Loaded = true
		}

		if object == nil {
			object = typ.NilValue()
		}

		version = &LogeObjectVersion{
			Version: 0,
			Object: object,
		}

		version.LogeObj = obj
	}

	obj.Current = version
	return obj
}


func (db *LogeDB) NewWriteBatch() LogeWriteBatch {
	return db.store.NewWriteBatch()
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

func RandomLogeKey() string {
	var buf = make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		panic("Couldn't generate key")
	}
	return fmt.Sprintf("%x", buf)
}
