package loge

import (
	"fmt"
	"bytes"
	"reflect"
	"encoding/binary"
	"runtime"

	"github.com/brendonh/spack"
	"github.com/jmhodges/levigo"
)

const VERSION = 1

const LINK_TAG uint16 = 2
const LINK_INFO_TAG uint16 = 3
const INDEX_TAG uint16 = 4
const START_TAG uint16 = 8

type LevelDBStore struct {
	basePath string
	db *levigo.DB
	types *spack.TypeSet

	writeQueue chan *LevelDBWriteBatch
	flushed bool

	linkSpec *spack.TypeSpec
	linkInfoSpec *spack.TypeSpec
}

type LevelDBWriteBatch struct {
	store *LevelDBStore
	batch []LevelDBWriteEntry
	result chan error
}

type LevelDBWriteEntry struct {
	Key []byte
	Val []byte
	Delete bool
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
		
		writeQueue: make(chan *LevelDBWriteBatch),
		flushed: false,

		linkSpec: spack.MakeTypeSpec([]string{}),
		linkInfoSpec: spack.MakeTypeSpec(LinkInfo{}),
	}

	store.types.LastTag = START_TAG

	store.LoadTypeMetadata()

	go store.Writer()
	
	return store
}

func (store *LevelDBStore) Close() {
	store.writeQueue <- nil
	for !store.flushed {
		runtime.Gosched()
	}
	store.db.Close()
}

func (store *LevelDBStore) LoadTypeMetadata() {
	var typeType = store.types.Type("_type")
	var tag = typeType.EncodeTag()
	var it = store.iteratePrefix(tag)
	defer it.Close()

	for it = it; it.Valid(); it.Next() {
		var typeInfo, err = typeType.DecodeObj(it.Value())

		if err != nil {
			panic(fmt.Sprintf("Error loading type info: %v", err))
		}

		store.types.LoadType(typeInfo.(*spack.VersionedType))
	}
}


func (store *LevelDBStore) RegisterType(typ *LogeType) {

	var vt = store.types.RegisterType(typ.Name)

	var exemplar = reflect.ValueOf(typ.Exemplar).Elem().Interface()

	vt.AddVersion(typ.Version, exemplar, nil)

	store.tagVersions(vt, typ)

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

func (store *LevelDBStore) tagVersions(vt *spack.VersionedType, typ *LogeType) {
	var prefix = encodeTaggedKey([]uint16{LINK_INFO_TAG, vt.Tag}, "")
	var it = store.iteratePrefix(prefix)
	defer it.Close()

	for it = it; it.Valid(); it.Next() {
		var info = &LinkInfo{}
		spack.DecodeFromBytes(info, store.linkInfoSpec, it.Value())
		typ.Links[info.Name] = info
	}


	var maxTag uint16 = 0;
	var missing = make([]*LinkInfo, 0)

	for _, info := range typ.Links {
		if info.Tag > maxTag {
			maxTag = info.Tag
		}
		if info.Tag == 0 {
			missing = append(missing, info)
		}
	}

	for _, info := range missing {
		maxTag++
		info.Tag = maxTag
		var key = encodeTaggedKey([]uint16{LINK_INFO_TAG, vt.Tag}, info.Name)
		enc, _ := spack.EncodeToBytes(info, store.linkInfoSpec)
		fmt.Printf("Updating link: %s::%s (%d)\n", typ.Name, info.Name, info.Tag)
		var err = store.db.Put(writeOptions, key, enc)
		if err != nil {
			panic(fmt.Sprintf("Write error: %v\n", err))
		}
	}

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

func (store *LevelDBStore) GetLinks(typ *LogeType, linkName string, objKey LogeKey) Links {
	var vt = store.types.Type(typ.Name)

	var linkInfo, ok = typ.Links[linkName]
	if !ok {
		panic(fmt.Sprintf("Link info missing for %s", linkName))
	}

	var key = encodeTaggedKey([]uint16{LINK_TAG, vt.Tag, linkInfo.Tag}, string(objKey))

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

func (store *LevelDBStore) NewWriteBatch() LogeWriteBatch {
	return &LevelDBWriteBatch{
		store: store,
		batch: make([]LevelDBWriteEntry, 0),
		result: make(chan error),
	}
}

// -----------------------------------------------
// Write Batches
// -----------------------------------------------

func (store *LevelDBStore) Writer() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	for batch := range store.writeQueue {
		if batch == nil {
			break
		}
		batch.result<- batch.Write()
	}
	store.flushed = true
}

func (batch *LevelDBWriteBatch) Store(obj *LogeObject) error {
	var vt = batch.store.types.Type(obj.Type.Name)
	var key = vt.EncodeKey(string(obj.Key))

	if !obj.Current.HasValue() {
		batch.Delete(key)
		return nil
	}

	var val, err = vt.EncodeObj(obj.Current.Object)

	if err != nil {
		panic(fmt.Sprintf("Encoding error: %v\n", err))
	}
	
	batch.Append(key, val)

	return nil
}

func (batch *LevelDBWriteBatch) StoreLinks(linkObj *LogeObject) error {
	var set = linkObj.Current.Object.(*LinkSet)

	if len(set.Added) == 0 && len(set.Removed) == 0 {
		return nil
	}

	var vt = batch.store.types.Type(linkObj.Type.Name)
	var linkInfo = linkObj.Type.Links[linkObj.LinkName]

	var key = encodeTaggedKey([]uint16{LINK_TAG, vt.Tag, linkInfo.Tag}, string(linkObj.Key))
	val, _ := spack.EncodeToBytes(set.ReadKeys(), batch.store.linkSpec)

	batch.Append(key, val)

	var prefix = append(
		encodeTaggedKey([]uint16{INDEX_TAG, vt.Tag, linkInfo.Tag}, string(linkObj.Key)),
		0)

	for _, target := range set.Added {
		var key = append(prefix, []byte(target)...)
		batch.Append(key, []byte{})
	}

	for _, target := range set.Removed {
		var key = append(prefix, []byte(target)...)
		batch.Delete(key)
	}

	return nil
}

func (batch *LevelDBWriteBatch) Append(key []byte, val []byte) {
	batch.batch = append(batch.batch, LevelDBWriteEntry{ key, val, false })
}

func (batch *LevelDBWriteBatch) Delete(key []byte) {
	batch.batch = append(batch.batch, LevelDBWriteEntry{ key, nil, true })
}

func (batch *LevelDBWriteBatch) Commit() error {
	batch.store.writeQueue <- batch
	return <-batch.result
}

func (batch *LevelDBWriteBatch) Write() error {
	var wb = levigo.NewWriteBatch()
	defer wb.Close()
	for _, entry := range batch.batch {
		if entry.Delete {
			wb.Delete(entry.Key)
		} else {
			wb.Put(entry.Key, entry.Val)
		}
	}

	return batch.store.db.Write(writeOptions, wb)
}


// -----------------------------------------------
// Key encoding
// -----------------------------------------------

func encodeTaggedKey(tags []uint16, key string) []byte {
	var keyBytes = []byte(key)
	var buf = bytes.NewBuffer(make([]byte, 0, len(keyBytes) + (2 * len(tags))))
	for _, tag := range tags {
		binary.Write(buf, binary.BigEndian, tag)
	}
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

