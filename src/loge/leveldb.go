package loge

import (
	"fmt"
	"sync"
	"path/filepath"

	"github.com/jmhodges/levigo"
)

type LevelDBStore struct {
	types map[*LogeType]*LevelDBTypeStore
	basePath string
}

type LevelDBTypeStore struct {
	t *LogeType
	db *levigo.DB
	cache map[string]*LogeObject
	mutex sync.Mutex
}

var writeOptions = levigo.NewWriteOptions()
var readOptions = levigo.NewReadOptions()

func NewLevelDBStore(basePath string) *LevelDBStore {
	return &LevelDBStore {
		types: make(map[*LogeType]*LevelDBTypeStore),
		basePath: basePath,
	}
}

func (store *LevelDBStore) RegisterType(typ *LogeType) {
	fmt.Printf("Register: %v\n", typ)

	var path = filepath.Join(store.basePath, typ.Name)
	var opts = levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(path, opts)

	if err != nil {
		panic(fmt.Sprintf("Can't open DB at %s: %v", path, err))
	}

	store.types[typ] = &LevelDBTypeStore {
		t: typ,
		db: db,
		cache: make(map[string]*LogeObject),
	}
}

func (store *LevelDBStore) Store(obj *LogeObject) error {
	return store.types[obj.Type].Store(obj)
}

func (store *LevelDBStore) Get(typ *LogeType, key string) *LogeObject {
	return store.types[typ].Get(key)
}

func (store *LevelDBStore) Ensure(obj *LogeObject) *LogeObject {
	return store.types[obj.Type].Ensure(obj)
}

// ---------------------------


func (ts *LevelDBTypeStore) Get(key string) *LogeObject {
	// XXX TODO: Cache
	fmt.Printf("%s get: %s\n", ts.t.Name, key)
	val, err := ts.db.Get(readOptions, []byte(key))

	if err != nil {
		panic(fmt.Sprintf("Read error: %v\n", err))
	}

	fmt.Printf("Get got: %v\n", val)

	return nil
}

func (ts *LevelDBTypeStore) Store(obj *LogeObject) error {
	// XXX TODO: Cache, don't write version-0 objects

	var val = obj.Current.Object.(string)
	fmt.Printf("Val: %v\n", val)
	var err = ts.db.Put(writeOptions, []byte(obj.Key), []byte(obj.Current.Object.(string)))
	if err != nil {
		panic(fmt.Sprintf("Write error: %v\n", err))
	}
	return nil
}

func (ts *LevelDBTypeStore) Ensure(obj *LogeObject) *LogeObject {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	fmt.Printf("%s ensure: %v\n", ts.t.Name, obj)

	var existing = ts.Get(obj.Key)

	if existing != nil {
		return existing
	}

	var err = ts.Store(obj)

	if err != nil {
		panic(fmt.Sprintf("Couldn't store object: %v", err))
	}

	return obj
}
