package loge

import (
	"fmt"
	"bytes"

	"github.com/brendonh/spack"
	"github.com/jmhodges/levigo"
)

const VERSION = 1

type LevelDBStore struct {
	basePath string
	db *levigo.DB
	types *spack.TypeSet
	nextTypeNum int
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

	var store = &LevelDBStore {
		basePath: basePath,
		db: db,
		types: spack.NewTypeSet(),
	}

	store.LoadTypeMetadata()
	
	return store
}


func (store *LevelDBStore) LoadTypeMetadata() {
	var typeType = store.types.Type("_type")
	var tag = typeType.EncodeTag()
	var it = store.iteratePrefix(tag)
	defer it.Close()

	for it = it; it.Valid(); it.Next() {
		var key = typeType.DecodeKey(it.Key())
		var typeInfo, err = typeType.DecodeObj(it.Value())

		if err != nil {
			panic(fmt.Sprintf("Error loading type info: %v", err))
		}

		store.types.LoadType(typeInfo.(*spack.VersionedType))

		fmt.Printf("Loaded type: %s (%d)\n", key, store.types.Type(key).Versions[0].Version)
	}
}


func (store *LevelDBStore) RegisterType(typ *LogeType) {
	fmt.Printf("Registering: %s (%d)\n", typ.Name, typ.Version)

	var vt = store.types.RegisterType(typ.Name)
	vt.AddVersion(typ.Version, typ.Exemplar, nil)

	if (!vt.Dirty) {
		return
	}

	fmt.Printf("Updating type info: %s\n", typ.Name)

	var typeType = store.types.Type("_type")

	var keyVal = typeType.EncodeKey(vt.Name)

	var typeVal, err = typeType.EncodeObj(vt)

	if err != nil {
		panic(fmt.Sprintf("Error encoding type %s: %v", vt.Name, err))
	}

	err = store.db.Put(writeOptions, keyVal, typeVal)
	
	if err != nil {
		panic(fmt.Sprintf("Couldn't write type metadata: %v\n", err))
	}

	vt.Dirty = false
}

func (store *LevelDBStore) Store(obj *LogeObject) error {
	// XXX TODO: Per-type keys

	//var encoded = obj.Type.ObjType.Encode(obj.Current.Object)
	var encoded = []byte{}

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

	fmt.Printf("Val: %v\n", val)

	return &LogeObjectVersion{
		Version: 0,
		Previous: nil,
		Object: nil, //typ.ObjType.Decode(val, []byte{}),
		Links: typ.NewLinks(),
	}
}


// -----------------------------------------------
// Prefix iterator
// -----------------------------------------------

type prefixIterator struct {
	Prefix []byte
	Iterator *levigo.Iterator
	Finished bool
}

func (store *LevelDBStore) iteratePrefix(prefix []byte) *prefixIterator {
	var it = store.db.NewIterator(readOptions)
	it.Seek(prefix)

	return &prefixIterator {
		Prefix: prefix,
		Iterator: it,
		Finished: it.Valid() && !bytes.HasPrefix(it.Key(), prefix),
	}
}

func (it *prefixIterator) Close() {
	it.Iterator.Close()
}

func (it *prefixIterator) Valid() bool {
	return !it.Finished && it.Iterator.Valid()
}

func (it *prefixIterator) Next() {
	it.Iterator.Next()
	it.Finished = it.Valid() && !bytes.HasPrefix(it.Key(), it.Prefix)
}

func (it *prefixIterator) Key() []byte {	
	return it.Iterator.Key()
}

func (it *prefixIterator) Value() []byte {	
	return it.Iterator.Value()
}

