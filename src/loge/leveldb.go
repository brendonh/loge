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

const ldb_LINK_TAG uint16 = 2
const ldb_LINK_INFO_TAG uint16 = 3
const ldb_INDEX_TAG uint16 = 4
const ldb_START_TAG uint16 = 8

type levelDBStore struct {
	basePath string
	db *levigo.DB
	types *spack.TypeSet

	writeQueue chan *levelDBWriteBatch
	flushed bool

	linkSpec *spack.TypeSpec
	linkInfoSpec *spack.TypeSpec
}

type levelDBResultSet struct {
	it *prefixIterator
	prefixLen int
	next string
	closed bool
}

type levelDBWriteBatch struct {
	store *levelDBStore
	batch []levelDBWriteEntry
	result chan error
}

type levelDBWriteEntry struct {
	Key []byte
	Val []byte
	Delete bool
}

var writeOptions = levigo.NewWriteOptions()
var readOptions = levigo.NewReadOptions()

func NewLevelDBStore(basePath string) LogeStore {

	var opts = levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(basePath, opts)

	if err != nil {
		panic(fmt.Sprintf("Can't open DB at %s: %v", basePath, err))
	}

	var store = &levelDBStore {
		basePath: basePath,
		db: db,
		types: spack.NewTypeSet(),
		
		writeQueue: make(chan *levelDBWriteBatch),
		flushed: false,

		linkSpec: spack.MakeTypeSpec([]string{}),
		linkInfoSpec: spack.MakeTypeSpec(linkInfo{}),
	}

	store.types.LastTag = ldb_START_TAG
	store.loadTypeMetadata()
	go store.writer()

	return store
}

func (store *levelDBStore) close() {
	store.writeQueue <- nil
	for !store.flushed {
		runtime.Gosched()
	}
	store.db.Close()
}

func (store *levelDBStore) registerType(typ *logeType) {
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


func (store *levelDBStore) get(typ *logeType, key LogeKey) interface{} {

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

func (store *levelDBStore) getLinks(typ *logeType, linkName string, objKey LogeKey) []string {
	var vt = store.types.Type(typ.Name)

	var linkInfo, ok = typ.Links[linkName]
	if !ok {
		panic(fmt.Sprintf("Link info missing for %s", linkName))
	}

	var key = encodeTaggedKey([]uint16{ldb_LINK_TAG, vt.Tag, linkInfo.Tag}, string(objKey))

	val, err := store.db.Get(readOptions, key)

	if err != nil {
		panic(fmt.Sprintf("Read error: %v\n", err))
	}

	if val == nil {
		return linkList{}
	}

	var links linkList
	spack.DecodeFromBytes(&links, store.linkSpec, val)

	return links
}


// -----------------------------------------------
// Search
// -----------------------------------------------

func (store *levelDBStore) find(typ *logeType, linkName string, target LogeKey) ResultSet {
	var vt = store.types.Type(typ.Name)
	var linkInfo = typ.Links[linkName]

	var prefix = append(
		encodeTaggedKey([]uint16{ldb_INDEX_TAG, vt.Tag, linkInfo.Tag}, string(target)),
		0)

	var it = store.iteratePrefix(prefix)
	if !it.Valid() {
		it.Close()
		return &levelDBResultSet {
			closed: true,
		}
	}

	var prefixLen = len(prefix)

	var next = string(it.Key()[prefixLen:])

	return &levelDBResultSet{
		it: it,
		prefixLen: prefixLen,
		next: next,
		closed: false,
	}
}

func (rs *levelDBResultSet) Valid() bool {
	return !rs.closed
}

func (rs *levelDBResultSet) Next() LogeKey {
	if rs.closed {
		return ""
	}
	var next = rs.next
	rs.it.Next()
	if rs.it.Valid() {
		rs.next = string(rs.it.Key()[rs.prefixLen:])
	} else {
		rs.Close()
	}
	return LogeKey(next)
}

func (rs *levelDBResultSet) Close() {
	rs.it.Close()
	rs.closed = true
}


// -----------------------------------------------
// Write Batches
// -----------------------------------------------

func (store *levelDBStore) newWriteBatch() writeBatch {
	return &levelDBWriteBatch{
		store: store,
		batch: make([]levelDBWriteEntry, 0),
		result: make(chan error),
	}
}

func (store *levelDBStore) writer() {
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

func (batch *levelDBWriteBatch) Store(obj *logeObject) error {
	var vt = batch.store.types.Type(obj.Type.Name)
	var key = vt.EncodeKey(string(obj.Key))

	if !obj.Current.hasValue() {
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

func (batch *levelDBWriteBatch) StoreLinks(linkObj *logeObject) error {
	var set = linkObj.Current.Object.(*linkSet)

	if len(set.Added) == 0 && len(set.Removed) == 0 {
		return nil
	}

	var vt = batch.store.types.Type(linkObj.Type.Name)
	var linkInfo = linkObj.Type.Links[linkObj.LinkName]

	var key = encodeTaggedKey([]uint16{ldb_LINK_TAG, vt.Tag, linkInfo.Tag}, string(linkObj.Key))
	val, _ := spack.EncodeToBytes(set.ReadKeys(), batch.store.linkSpec)

	batch.Append(key, val)

	var prefix = encodeTaggedKey([]uint16{ldb_INDEX_TAG, vt.Tag, linkInfo.Tag}, "")
	var source = string(linkObj.Key)

	for _, target := range set.Removed {
		var key = encodeIndexKey(prefix, target, source)
		batch.Delete(key)
	}

	for _, target := range set.Added {
		var key = encodeIndexKey(prefix, target, source)
		batch.Append(key, []byte{})
	}


	return nil
}

func (batch *levelDBWriteBatch) Append(key []byte, val []byte) {
	batch.batch = append(batch.batch, levelDBWriteEntry{ key, val, false })
}

func (batch *levelDBWriteBatch) Delete(key []byte) {
	batch.batch = append(batch.batch, levelDBWriteEntry{ key, nil, true })
}

func (batch *levelDBWriteBatch) Commit() error {
	batch.store.writeQueue <- batch
	return <-batch.result
}

func (batch *levelDBWriteBatch) Write() error {
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
// Internals
// -----------------------------------------------

func (store *levelDBStore) loadTypeMetadata() {
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

func (store *levelDBStore) tagVersions(vt *spack.VersionedType, typ *logeType) {
	var prefix = encodeTaggedKey([]uint16{ldb_LINK_INFO_TAG, vt.Tag}, "")
	var it = store.iteratePrefix(prefix)
	defer it.Close()

	for it = it; it.Valid(); it.Next() {
		var info = &linkInfo{}
		spack.DecodeFromBytes(info, store.linkInfoSpec, it.Value())
		typ.Links[info.Name] = info
	}


	var maxTag uint16 = 0;
	var missing = make([]*linkInfo, 0)

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
		var key = encodeTaggedKey([]uint16{ldb_LINK_INFO_TAG, vt.Tag}, info.Name)
		enc, _ := spack.EncodeToBytes(info, store.linkInfoSpec)
		fmt.Printf("Updating link: %s::%s (%d)\n", typ.Name, info.Name, info.Tag)
		var err = store.db.Put(writeOptions, key, enc)
		if err != nil {
			panic(fmt.Sprintf("Write error: %v\n", err))
		}
	}

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

func encodeIndexKey(prefix []byte, target string, source string) []byte {
	var buf = make([]byte, 0, len(prefix) + len(target) + len(source))
	buf = append(buf, prefix...)
	buf = append(buf, []byte(target)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(source)...)
	return buf
}

// -----------------------------------------------
// Prefix iterator
// -----------------------------------------------

type prefixIterator struct {
	Prefix []byte
	Iterator *levigo.Iterator
	Finished bool
}

func (store *levelDBStore) iteratePrefix(prefix []byte) *prefixIterator {
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

