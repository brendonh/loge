package loge

import (
	"fmt"
	"time"
	"math/rand"
)

type TransactionState int

const (
	ACTIVE = iota
	CANCELLED
	COMMITTING
	FINISHED
	ABORTED
	ERROR
)


type liveVersion struct {
	version *objectVersion
	object interface{}
	dirty bool
}


type Transaction struct {
	db *LogeDB
	context transactionContext
	versions map[string]*liveVersion
	state TransactionState
	snapshotID uint64
	cancelled bool
	giveJSON bool
}

func NewTransaction(db *LogeDB, sID uint64) *Transaction {
	return &Transaction{
		db: db,
		context: db.store.newContext(sID),
		versions: make(map[string]*liveVersion),
		state: ACTIVE,
		snapshotID: sID,
	}
}


func (t *Transaction) String() string {
	return fmt.Sprintf("Transaction<%s>", t.state.String())
}

func (t *Transaction) GetState() TransactionState {
	return t.state
}

func (t *Transaction) Exists(typeName string, key LogeKey) bool {
	var lv = t.getVersion(t.db.makeObjRef(typeName, key), false, true)
	return lv.version.LogeObj.hasValue(lv.object)
}


func (t *Transaction) Read(typeName string, key LogeKey) interface{} {
	return t.getVersion(t.db.makeObjRef(typeName, key), false, true).object
}


func (t *Transaction) Write(typeName string, key LogeKey) interface{} {
	return t.getVersion(t.db.makeObjRef(typeName, key), true, true).object
}


func (t *Transaction) Set(typeName string, key LogeKey, obj interface{}) {
	var version = t.getVersion(t.db.makeObjRef(typeName, key), true, false)
	version.object = obj
}


func (t *Transaction) Delete(typeName string, key LogeKey) {
	var version = t.getVersion(t.db.makeObjRef(typeName, key), true, true)
	version.object = version.version.LogeObj.Type.NilValue()
}


func (t *Transaction) ReadLinks(typeName string, linkName string, key LogeKey) []string {
	return t.getLink(t.db.makeLinkRef(typeName, linkName, key), false, true).ReadKeys()
}

func (t *Transaction) HasLink(typeName string, linkName string, key LogeKey, target LogeKey) bool {
	return t.getLink(t.db.makeLinkRef(typeName, linkName, key), false, true).Has(string(target))
}

func (t *Transaction) AddLink(typeName string, linkName string, key LogeKey, target LogeKey) {
	t.getLink(t.db.makeLinkRef(typeName, linkName, key), true, true).Add(string(target))
}

func (t *Transaction) RemoveLink(typeName string, linkName string, key LogeKey, target LogeKey) {
	t.getLink(t.db.makeLinkRef(typeName, linkName, key), true, true).Remove(string(target))
}

func (t *Transaction) SetLinks(typeName string, linkName string, key LogeKey, targets []LogeKey) {
	// XXX BGH: Yargh
	var stringTargets = make([]string, 0, len(targets))
	for _, key := range targets {
		stringTargets = append(stringTargets, string(key))
	}
	t.getLink(t.db.makeLinkRef(typeName, linkName, key), true, true).Set(stringTargets)
}

func (t *Transaction) Find(typeName string, linkName string, target LogeKey) ResultSet {
	return t.context.find(t.db.makeLinkRef(typeName, linkName, target))
}

func (t *Transaction) FindSlice(typeName string, linkName string, target LogeKey, from LogeKey, limit int) ResultSet {	
	return t.context.findSlice(t.db.makeLinkRef(typeName, linkName, target), from, limit)
}

func (t *Transaction) ListSlice(typeName string, from LogeKey, limit int) ResultSet {	
	typ, ok := t.db.types[typeName]
	if !ok {
		panic(fmt.Sprintf("No such type %s\n", typeName))
	}
	var prefix = typePrefix(typ)
	return t.context.listSlice(prefix, from, limit)
}

// -----------------------------------------------
// Internals
// -----------------------------------------------

func (t *Transaction) getLink(ref objRef, forWrite bool, load bool) *linkSet {
	var version = t.getVersion(ref, forWrite, load)
	return version.object.(*linkSet)
}

func (t *Transaction) getVersion(ref objRef, forWrite bool, load bool) *liveVersion {

	if t.state != ACTIVE {
		panic(fmt.Sprintf("GetObj from inactive transaction %s\n", t))
	}

	var objKey = ref.CacheKey

	lv, ok := t.versions[objKey]

	if ok {
		if forWrite {
			lv.dirty = true
		}
		return lv
	}

	var version = t.db.acquireVersion(ref, t.context, load)

	object, upgraded := version.getObject(t.giveJSON)

	lv = &liveVersion{
		version: version,
		object: object,
		dirty: forWrite || upgraded,
	}

	t.versions[objKey] = lv
	return lv
}


const t_BACKOFF_EXPONENT = 1.05

func (t *Transaction) Cancel() {
	if (t.state != ACTIVE) {
		panic(fmt.Sprintf("Cancel on transaction %s\n", t))
	}

	t.state = CANCELLED
}

func (t *Transaction) Commit() bool {
	if (t.state == CANCELLED) {
		return false
	}

	if (t.state != ACTIVE) {
		panic(fmt.Sprintf("Commit on transaction %s\n", t))
	}

	t.state = COMMITTING

	var versions = make([]*liveVersion, 0, len(t.versions))
	for _, v := range t.versions {
		versions = append(versions, v)
	}
	
	var delayFact = 10.0
	for {
		if t.tryCommit(versions) {
			break
		}
		var delay = time.Duration(delayFact - float64(rand.Intn(10)))
		time.Sleep(delay * time.Millisecond)
		delayFact *= t_BACKOFF_EXPONENT
	}

	t.db.releaseVersions(versions)

	return t.state == FINISHED
}

func (t *Transaction) tryCommit(versions []*liveVersion) bool {
	for _, lv := range versions {
		var obj = lv.version.LogeObj

		if !obj.Lock.TryLock() {
			return false
		}
		defer obj.Lock.Unlock()

		if obj.Current.snapshotID > t.snapshotID {
			t.state = ABORTED
			return true
		}
	}

	var context = t.context
	var sID = t.db.newSnapshotID()

	for _, lv := range versions {
		if lv.dirty {
			var obj = lv.version.LogeObj
			obj.applyVersion(lv.object, context, sID)
		}
	}

	var err = context.commit(sID)
	if err != nil {
		t.state = ERROR
		fmt.Printf("Commit error: %v\n", err)
	}

	t.state = FINISHED
	return true
}


func (ts TransactionState) String() string {
	switch ts {
	case ACTIVE: 
		return "ACTIVE"
	case CANCELLED:
		return "CANCELLED"
	case COMMITTING: 
		return "COMMITTING"
	case FINISHED: 
		return "FINISHED"
	case ABORTED: 
		return "ABORTED"
	case ERROR: 
		return "ERROR"
	}
	return "UNKNOWN STATE"
}