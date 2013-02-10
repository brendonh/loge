package loge

import (
	"github.com/brendonh/spack"
)

type LogeStore interface {
	storeContext
	close()
	registerType(*logeType)
	getSpackType(name string) *spack.VersionedType
	newContext() transactionContext
}

type ResultSet interface {
	All() []LogeKey
	Next() LogeKey
	Valid() bool
	Close()
}

type storeContext interface {
	get(objRef) []byte
	store(objRef, []byte) error

	addIndex(objRef, LogeKey)
	remIndex(objRef, LogeKey)

	find(objRef, LogeKey) ResultSet
	findSlice(objRef, LogeKey, LogeKey, int) ResultSet
}

type transactionContext interface {
	storeContext
	commit() error
	rollback()
}

type objectMap map[string][]byte


type memStore struct {
	objects objectMap
	lock spinLock
	spackTypes *spack.TypeSet
}

type memContext struct {
	mstore *memStore
	writes []memWriteEntry
}

type memWriteEntry struct {
	CacheKey string
	Value []byte
}

func NewMemStore() LogeStore {
	return &memStore{
		objects: make(objectMap),
		spackTypes: spack.NewTypeSet(),
	}
}

func (store *memStore) close() {
}

func (store *memStore) registerType(typ *logeType) {
	store.spackTypes.RegisterType(typ.Name)
}

func (store *memStore) getSpackType(name string) *spack.VersionedType {
	return store.spackTypes.RegisterType(name)
}

func (store *memStore) get(ref objRef) []byte {
	enc, ok := store.objects[ref.CacheKey]
	if !ok {
		return nil
	}
	return enc
}

func (store *memStore) addIndex(ref objRef, key LogeKey) {
}

func (store *memStore) remIndex(ref objRef, key LogeKey) {
}

func (store *memStore) find(ref objRef, key LogeKey) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (store *memStore) findSlice(ref objRef, key LogeKey, from LogeKey, limit int) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (store *memStore) store(ref objRef, enc []byte) error {
	store.lock.SpinLock()
	defer store.lock.Unlock()
	if len(enc) == 0 {
		delete(store.objects, ref.CacheKey)
	} else {
		store.objects[ref.CacheKey] = enc
	}
	return nil
}

func (store *memStore) newContext() transactionContext {
	return &memContext{
		mstore: store,
	}
}

func (context *memContext) get(ref objRef) []byte {
	return context.mstore.get(ref)
}

func (context *memContext) store(ref objRef, enc []byte) error {
	context.writes = append(
		context.writes,
		memWriteEntry{ 
		CacheKey: ref.CacheKey,
		Value: enc,
	})
	return nil
}


func (context *memContext) addIndex(ref objRef, key LogeKey) {
}

func (context *memContext) remIndex(ref objRef, key LogeKey) {
}
func (context *memContext) find(ref objRef, key LogeKey) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (context *memContext) findSlice(ref objRef, key LogeKey, from LogeKey, limit int) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (context *memContext) commit() error {
	var store = context.mstore
	store.lock.SpinLock()
	defer store.lock.Unlock()
	for _, entry := range context.writes {
		if len(entry.Value) == 0 {
			delete(store.objects, entry.CacheKey)
		} else {
			store.objects[entry.CacheKey] = entry.Value
		}
	}
	return nil
}

func (context *memContext) rollback() {
}