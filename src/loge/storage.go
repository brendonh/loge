package loge

import (
	"github.com/brendonh/spack"
)

type LogeStore interface {
	get(objRef) []byte
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

type transactionContext interface {
	get(objRef) []byte
	store(objRef, []byte) error

	addIndex(objRef, LogeKey)
	remIndex(objRef, LogeKey)

	find(objRef) ResultSet
	findSlice(objRef, LogeKey, int) ResultSet

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
func (context *memContext) find(ref objRef) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (context *memContext) findSlice(ref objRef, from LogeKey, limit int) ResultSet {
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