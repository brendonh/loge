package loge

import (
	"github.com/brendonh/spack"
)

type LogeStore interface {
	close()
	registerType(*logeType)
	getSpackType(name string) *spack.VersionedType
	newContext(uint64) transactionContext
}

type ResultSet interface {
	All() []LogeKey
	Next() LogeKey
	Valid() bool
	Close()
}

type transactionContext interface {
	getSnapshotID() uint64

	get(objRef) []byte
	store(objRef, []byte) error

	addIndex(objRef, LogeKey)
	remIndex(objRef, LogeKey)

	find(objRef) ResultSet
	findSlice(objRef, LogeKey, int) ResultSet

	commit(uint64) error
	rollback()
}

type memVersion struct {
	snapshotID uint64
	blob []byte
}

type memVersionHistory []memVersion

func (mvh memVersionHistory) findPrevious(sID uint64) []byte {
	for i := len(mvh)-1; i >= 0; i-- {
		if mvh[i].snapshotID <= sID {
			return mvh[i].blob
		}
	}
	return nil
}

type objectMap map[string]memVersionHistory

type memStore struct {
	objects objectMap
	lock spinLock
	spackTypes *spack.TypeSet
}

type memContext struct {
	mstore *memStore
	snapshotID uint64
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


func (store *memStore) newContext(sID uint64) transactionContext {
	return &memContext{
		mstore: store,
		snapshotID: sID,
	}
}

func (context *memContext) getSnapshotID() uint64 {
	return context.snapshotID
}


func (context *memContext) get(ref objRef) []byte {
	mvh, ok := context.mstore.objects[ref.CacheKey]
	if !ok {
		return nil
	}
	return mvh.findPrevious(context.snapshotID)
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

func (context *memContext) commit(sID uint64) error {
	var store = context.mstore
	store.lock.SpinLock()
	defer store.lock.Unlock()
	for _, entry := range context.writes {
		var mv = memVersion{ sID, entry.Value }
		var mvh = store.objects[entry.CacheKey]
		store.objects[entry.CacheKey] = append(mvh, mv)
	}
	return nil
}

func (context *memContext) rollback() {
}