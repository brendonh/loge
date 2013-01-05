package loge

import (
	"fmt"

	"github.com/jmhodges/levigo"
)

const VERSION = 1

type LevelDBStore struct {
	basePath string
	db *levigo.DB
	types map[*LogeType]*LevelDBType
}

type LevelDBType struct {
	T *LogeType
}

var writeOptions = levigo.NewWriteOptions()
var readOptions = levigo.NewReadOptions()

func NewLevelDBStore(basePath string) *LevelDBStore {

	var opts = levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(basePath, opts)

	if err != nil {
		panic(fmt.Sprintf("Can't open DB at %s: %v", basePath, err))
	}

	return &LevelDBStore {
		basePath: basePath,
		db: db,
		types: make(map[*LogeType]*LevelDBType),
	}
}

func (store *LevelDBStore) RegisterType(typ *LogeType) {
	fmt.Printf("Register: %v\n", typ)
	store.types[typ] = &LevelDBType {
		T: typ,
	}
}

func (store *LevelDBStore) Store(obj *LogeObject) error {
	// XXX TODO: Per-type keys

	var encoded = obj.Type.ObjType.Encode(obj.Current.Object)

	fmt.Printf("Store: %v::%v -> %v (%v)\n", obj.Type.Name, obj.Key, 
		encoded, obj.RefCount)

	var err = store.db.Put(writeOptions, []byte(obj.Key), encoded)
	if err != nil {
		panic(fmt.Sprintf("Write error: %v\n", err))
	}
	return nil
}


func (store *LevelDBStore) Get(typ *LogeType, key LogeKey) *LogeObjectVersion {
	val, err := store.db.Get(readOptions, []byte(key))

	if err != nil {
		panic(fmt.Sprintf("Read error: %v\n", err))
	}

	return &LogeObjectVersion{
		Version: 0,
		Previous: nil,
		Object: typ.ObjType.Decode(val),
		Links: typ.NewLinks(),
	}
}


