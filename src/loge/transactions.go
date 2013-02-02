package loge

import (
	"fmt"
	"time"
	"math/rand"
)

type TransactionState int

const (
	ACTIVE = iota
	COMMITTING
	FINISHED
	ABORTED
	ERROR
)


type Transaction struct {
	DB *LogeDB
	Versions map[string]*LogeObjectVersion
	State TransactionState
}


func NewTransaction(db *LogeDB) *Transaction {
	return &Transaction{
		DB: db,
		Versions: make(map[string]*LogeObjectVersion),
		State: ACTIVE,
	}
}


func (t *Transaction) String() string {
	return fmt.Sprintf("Transaction<%s>", t.StateString())
}


func (t *Transaction) Exists(typeName string, key LogeKey) bool {
	var version = t.getObj(MakeObjRef(typeName, key), false)
	return version.HasValue()
}


func (t *Transaction) ReadObj(typeName string, key LogeKey) interface{} {
	return t.getObj(MakeObjRef(typeName, key), false).Object
}


func (t *Transaction) WriteObj(typeName string, key LogeKey) interface{} {
	return t.getObj(MakeObjRef(typeName, key), true).Object
}


func (t *Transaction) SetObj(typeName string, key LogeKey, obj interface{}) {
	var version = t.getObj(MakeObjRef(typeName, key), true)
	version.Object = obj
}


func (t *Transaction) DeleteObj(typeName string, key LogeKey) {
	var version = t.getObj(MakeObjRef(typeName, key), true)
	version.Object = version.LogeObj.Type.NilValue()
}


func (t *Transaction) ReadLinks(typeName string, linkName string, key LogeKey) []string {
	return t.getLink(MakeLinkRef(typeName, linkName, key), false).ReadKeys()
}

func (t *Transaction) HasLink(typeName string, linkName string, key LogeKey, target LogeKey) bool {
	return t.getLink(MakeLinkRef(typeName, linkName, key), false).Has(string(target))
}

func (t *Transaction) AddLink(typeName string, linkName string, key LogeKey, target LogeKey) {
	t.getLink(MakeLinkRef(typeName, linkName, key), true).Add(string(target))
}

func (t *Transaction) getLink(objRef ObjRef, forWrite bool) *LinkSet {
	var version = t.getObj(objRef, forWrite)
	return version.Object.(*LinkSet)
}

func (t *Transaction) getObj(objRef ObjRef, forWrite bool) *LogeObjectVersion {

	if t.State != ACTIVE {
		panic(fmt.Sprintf("GetObj from inactive transaction %s\n", t))
	}

	var objKey = objRef.String()

	version, ok := t.Versions[objKey]

	if ok {
		if forWrite {
			if !version.Dirty {
				version = version.LogeObj.NewVersion()
				t.Versions[objKey] = version
			}
		}
		return version
	}

	var logeObj = t.DB.EnsureObj(objRef)

	logeObj.SpinLock()
	defer logeObj.Unlock()

	logeObj.RefCount++

	if forWrite {
		version = logeObj.NewVersion()
	} else {
		version = logeObj.Current
	}

	t.Versions[objKey] = version

	return version
}


const BACKOFF_EXPONENT = 1.05

func (t *Transaction) Commit() bool {
	
	if (t.State != ACTIVE) {
		panic(fmt.Sprintf("Commit on transaction %s\n", t))
	}

	t.State = COMMITTING
	
	var delayFact = 10.0
	for {
		if t.tryCommit() {
			break
		}
		var delay = time.Duration(delayFact - float64(rand.Intn(10)))
		time.Sleep(delay * time.Millisecond)
		delayFact *= BACKOFF_EXPONENT
	}

	return t.State == FINISHED
}

func (t *Transaction) tryCommit() bool {
	//fmt.Printf("-------------\n")
	for _, version := range t.Versions {
		var obj = version.LogeObj

		if !obj.TryLock() {
			return false
		}
		defer obj.Unlock()

		var expectedVersion int
		if version.Dirty {
			expectedVersion = obj.Current.Version + 1
		} else {
			expectedVersion = obj.Current.Version
		}

		if version.Version != expectedVersion {
			t.State = ABORTED
			return true
		}
	}
	
	for _, version := range t.Versions {
		//fmt.Printf("Version %v\n", version)
		version.LogeObj.RefCount--
		if version.Dirty {
			version.LogeObj.ApplyVersion(version)
		}
	}

	t.State = FINISHED
	return true
}


func (t *Transaction) StateString() string {
	switch t.State {
	case ACTIVE: 
		return "ACTIVE"
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