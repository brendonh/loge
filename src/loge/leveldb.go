package loge

import (
	"fmt"
	"bytes"
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

	writeQueue chan *levelDBContext
	flushed bool
}

type levelDBResultSet struct {
	it *prefixIterator
	prefixLen int
	next string
	limit int
	count int
	closed bool
}

type levelDBContext struct {
	ldbStore *levelDBStore
	snapshot *levigo.Snapshot
	readOptions *levigo.ReadOptions
	batch []levelDBWriteEntry
	result chan error
}

type levelDBWriteEntry struct {
	Key []byte
	Val []byte
	Delete bool
}

var defaultWriteOptions = levigo.NewWriteOptions()
var defaultReadOptions = levigo.NewReadOptions()

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
		
		writeQueue: make(chan *levelDBContext),
		flushed: false,
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
	store.tagVersions(typ)

	var vt = typ.SpackType

	if (!vt.Dirty) {
		return
	}

	fmt.Printf("Updating type info: %s (%d)\n", typ.Name, typ.Version)

	var typeType = store.types.Type("_type")
	var keyVal = typeType.EncodeKey(vt.Name)
	var typeVal, err = typeType.EncodeObj(vt)

	if err != nil {
		panic(fmt.Sprintf("Error encoding type %s: %v", vt.Name, err))
	}

	err = store.db.Put(defaultWriteOptions, keyVal, typeVal)
	
	if err != nil {
		panic(fmt.Sprintf("Couldn't write type metadata: %v\n", err))
	}

	vt.Dirty = false
}

func (store *levelDBStore) getSpackType(name string) *spack.VersionedType {
	return store.types.RegisterType(name)
}


func (store *levelDBStore) get(ref objRef) []byte {
	val, err := store.db.Get(defaultReadOptions, []byte(ref.CacheKey))

	if err != nil {
		panic(fmt.Sprintf("Read error: %v\n", err))
	}

	return val
}

// -----------------------------------------------
// Search
// -----------------------------------------------

func (rs *levelDBResultSet) Valid() bool {
	return !rs.closed
}

func (rs *levelDBResultSet) Next() LogeKey {
	if rs.closed {
		return ""
	}
	var next = rs.next
	rs.it.Next()
	rs.count++

	if rs.it.Valid() {
		rs.next = string(rs.it.Key()[rs.prefixLen:])
		if rs.limit >= 0 && rs.count >= rs.limit {
			rs.Close()
		}
	} else {
		rs.Close()
	} 

	return LogeKey(next)
}

func (rs *levelDBResultSet) All() []LogeKey {
	var keys = make([]LogeKey, 0)
	for rs.Valid() {
		keys = append(keys, rs.Next())
	}
	return keys
}

func (rs *levelDBResultSet) Close() {
	rs.it.Close()
	rs.closed = true
}


// -----------------------------------------------
// Transaction Contexts
// -----------------------------------------------

func (store *levelDBStore) newContext() transactionContext {
	var snapshot = store.db.NewSnapshot()
	var options = levigo.NewReadOptions()
	options.SetSnapshot(snapshot)
	return &levelDBContext{
		ldbStore: store,
		readOptions: options,
		snapshot: snapshot,
		batch: make([]levelDBWriteEntry, 0),
		result: make(chan error),
	}
}

func (store *levelDBStore) writer() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	for context := range store.writeQueue {
		if context == nil {
			break
		}
		context.result<- context.Write()
	}
	store.flushed = true
}



func (context *levelDBContext) commit() error {
	context.ldbStore.writeQueue <- context
	var err = <-context.result
	context.cleanup()
	return err
}

func (context *levelDBContext) rollback() {
	context.cleanup()
}

func (context *levelDBContext) cleanup() {
	context.ldbStore.db.ReleaseSnapshot(context.snapshot)
	context.readOptions.Close()
}

func (context *levelDBContext) Write() error {
	var wb = levigo.NewWriteBatch()
	defer wb.Close()
	for _, entry := range context.batch {
		if entry.Delete {
			wb.Delete(entry.Key)
		} else {
			wb.Put(entry.Key, entry.Val)
		}
	}

	return context.ldbStore.db.Write(defaultWriteOptions, wb)
}


// -----------------------------------------------
// transactionContext API
// -----------------------------------------------

func (context *levelDBContext) get(ref objRef) []byte {
	return context.ldbStore.get(ref)
}

func (context *levelDBContext) store(ref objRef, enc []byte) error {
	var key = []byte(ref.CacheKey)

	if len(enc) == 0 {
		context.delete(key)
		return nil
	}
	
	context.put(key, enc)

	return nil
}

func (context *levelDBContext) addIndex(ref objRef, source LogeKey) {
	var key = encodeIndexKey(ref, source)
	context.put(key, []byte{})
}

func (context *levelDBContext) remIndex(ref objRef, source LogeKey) {
	var key = encodeIndexKey(ref, source)
	context.delete(key)
}

func (context *levelDBContext) find(ref objRef) ResultSet {
	return context.findSlice(ref, "", -1)
}

func (context *levelDBContext) findSlice(ref objRef, from LogeKey, limit int) ResultSet {
	if limit == 0 {
		return &levelDBResultSet {
			closed: true,
		}
	}

	var prefix = append(
		encodeLDBKey(ldb_INDEX_TAG, ref),
		0)

	var it = context.ldbStore.iteratePrefix(prefix, []byte(from), context.readOptions)
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
		limit: limit,
		count: 0,
	}
}


// -----------------------------------------------
// Helpers
// -----------------------------------------------

func (context *levelDBContext) put(key []byte, val []byte) error {
	context.batch = append(context.batch, levelDBWriteEntry{ key, val, false })
	return nil
}

func (context *levelDBContext) delete(key []byte) error {
	context.batch = append(context.batch, levelDBWriteEntry{ key, nil, true })
	return nil
}

// -----------------------------------------------
// Internals
// -----------------------------------------------

func (store *levelDBStore) loadTypeMetadata() {
	var typeType = store.types.Type("_type")
	var tag = typeType.EncodeTag()
	var it = store.iteratePrefix(tag, []byte{}, defaultReadOptions)
	defer it.Close()

	for it = it; it.Valid(); it.Next() {
		var typeInfo, _, err = typeType.DecodeObj(it.Value())

		if err != nil {
			panic(fmt.Sprintf("Error loading type info: %v", err))
		}

		store.types.LoadType(typeInfo.(*spack.VersionedType))
	}
}

func (store *levelDBStore) tagVersions(typ *logeType) {
	var vt = typ.SpackType
	var prefix = encodeTaggedKey([]uint16{ldb_LINK_INFO_TAG, vt.Tag}, "")
	var it = store.iteratePrefix(prefix, []byte{}, defaultReadOptions)
	defer it.Close()

	for it = it; it.Valid(); it.Next() {
		var info = &linkInfo{}
		spack.DecodeFromBytes(info, linkInfoSpec, it.Value())
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
		enc, _ := spack.EncodeToBytes(info, linkInfoSpec)
		fmt.Printf("Updating link: %s::%s (%d)\n", typ.Name, info.Name, info.Tag)
		var err = store.db.Put(defaultWriteOptions, key, enc)
		if err != nil {
			panic(fmt.Sprintf("Write error: %v\n", err))
		}
	}

}

// -----------------------------------------------
// Key encoding
// -----------------------------------------------

func encodeLDBKey(typeTag uint16, ref objRef) []byte {
	var keyBytes = []byte(ref.CacheKey)
	var buf = bytes.NewBuffer(make([]byte, 0, len(keyBytes) + 2))
	binary.Write(buf, binary.BigEndian, typeTag)
	buf.Write(keyBytes)
	return buf.Bytes()
}

func encodeTaggedKey(tags []uint16, key string) []byte {
	var keyBytes = []byte(key)
	var buf = bytes.NewBuffer(make([]byte, 0, len(keyBytes) + (2 * len(tags))))
	for _, tag := range tags {
		binary.Write(buf, binary.BigEndian, tag)
	}
	buf.Write(keyBytes)
	return buf.Bytes()
}

func encodeIndexKey(ref objRef, target LogeKey) []byte {
	var targetBytes = []byte(ref.CacheKey)
	var sourceBytes = []byte(target)
	var buf = bytes.NewBuffer(make([]byte, 0, 3 + len(targetBytes) + len(sourceBytes)))
	binary.Write(buf, binary.BigEndian, ldb_INDEX_TAG)
	buf.Write(targetBytes)
	buf.Write([]byte{0})
	buf.Write(sourceBytes)
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

func (store *levelDBStore) iteratePrefix(prefix []byte, from []byte, readOptions *levigo.ReadOptions) *prefixIterator {
	var it = store.db.NewIterator(readOptions)
	var seekPrefix = append(prefix, from...)
	it.Seek(seekPrefix)

	if len(from) > 0 && it.Valid() {
		it.Next()
	}

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

