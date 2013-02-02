package loge

import (
	"fmt"
	"bytes"
	"reflect"
	"encoding/binary"

	"github.com/brendonh/spack"
	"github.com/jmhodges/levigo"
)

const VERSION = 1

const LINK_TAG uint16 = 2
const START_TAG uint16 = 8

type LevelDBStore struct {
	basePath string
	db *levigo.DB
	types *spack.TypeSet
	nextTypeNum int
	linkSpec *spack.TypeSpec
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
		linkSpec: spack.MakeTypeSpec([]string{}),
	}

	store.types.LastTag = START_TAG

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

		var typ = store.types.Type(key)

		fmt.Printf("Loaded type: %s (#%d, v%d)\n", key, typ.Tag, typ.Versions[0].Version)
	}
}


func (store *LevelDBStore) RegisterType(typ *LogeType) {
	fmt.Printf("Registering: %s (%d)\n", typ.Name, typ.Version)

	var vt = store.types.RegisterType(typ.Name)

	var exemplar = reflect.ValueOf(typ.Exemplar).Elem().Interface()

	vt.AddVersion(typ.Version, exemplar, nil)

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
	var vt = store.types.Type(obj.Type.Name)
	var key = vt.EncodeKey(string(obj.Key))
	var val, err = vt.EncodeObj(obj.Current.Object)

	if err != nil {
		panic(fmt.Sprintf("Encoding error: %v\n", err))
	}

	err = store.db.Put(writeOptions, key, val)
	if err != nil {
		panic(fmt.Sprintf("Write error: %v\n", err))
	}

	return nil
}

func (store *LevelDBStore) Get(typ *LogeType, key LogeKey) interface{} {

	var vt = store.types.Type(typ.Name)
	var encKey = vt.EncodeKey(string(key))

	val, err := store.db.Get(readOptions, encKey)

	if err != nil {
		panic(fmt.Sprintf("Read error: %v\n", err))
	}

	var obj interface{}
	if val == nil {
		obj = typ.NilValue()
	} else {
		obj, err = vt.DecodeObj(val)
		if err != nil {
			panic(fmt.Sprintf("Decode error: %v", err))
		}
	}

	return obj
}

// -----------------------------------------------
// Links
// -----------------------------------------------

func (store *LevelDBStore) StoreLinks(linkObj *LogeObject) error {
	var set = linkObj.Current.Object.(*LinkSet)

	if len(set.Added) == 0 && len(set.Removed) == 0 {
		return nil
	}

	var vt = store.types.Type(linkObj.Type.Name)
	// XXX BGH TODO: Use tags for link names too
	var lk = linkObj.LinkName + "^" + string(linkObj.Key)
	var key = linkKey(vt.Tag, lk)

	enc, _ := spack.EncodeToBytes(set.ReadKeys(), store.linkSpec)

	var err = store.db.Put(writeOptions, key, enc)
	if err != nil {
		panic(fmt.Sprintf("Write error: %v\n", err))
	}

	return nil
}

func (store *LevelDBStore) GetLinks(typ *LogeType, linkName string, objKey LogeKey) Links {
	var vt = store.types.Type(typ.Name)

	// XXX BGH TODO: Use tags for link names too
	var lk = linkName + "^" + string(objKey)
	var key = linkKey(vt.Tag, lk)

	val, err := store.db.Get(readOptions, key)

	if err != nil {
		panic(fmt.Sprintf("Read error: %v\n", err))
	}

	if val == nil {
		return Links{}
	}

	var links Links
	spack.DecodeFromBytes(&links, store.linkSpec, val)

	return links
}

func linkKey(typeTag uint16, key string) []byte {
	var keyBytes = []byte(key)
	var buf = bytes.NewBuffer(make([]byte, 0, len(keyBytes) + 4))
	binary.Write(buf, binary.BigEndian, LINK_TAG)
	binary.Write(buf, binary.BigEndian, typeTag)
	buf.Write(keyBytes)
	return buf.Bytes()
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

