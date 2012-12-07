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
	Objs map[string]*InvolvedObject
	State TransactionState
}


type InvolvedObject struct {
	Obj *LogeObject
	Version *LogeObjectVersion
	FromVersion int
	Dirty bool
}


func NewTransaction(db *LogeDB) *Transaction {
	return &Transaction{
		DB: db,
		Objs: make(map[string]*InvolvedObject),
		State: ACTIVE,
	}
}


func (t *Transaction) String() string {
	return fmt.Sprintf("Transaction<%s>", t.StateString())
}


func (t *Transaction) Exists(typeName string, key string) bool {
	var obj = t.getObj(typeName, key, false, false)
	return obj.Version.HasValue()
}


func (t *Transaction) ReadObj(typeName string, key string) interface{} {
	return t.getObj(typeName, key, false, true).Version.Object
}


func (t *Transaction) WriteObj(typeName string, key string) interface{} {
	return t.getObj(typeName, key, true, true).Version.Object
}


func (t *Transaction) SetObj(typeName string, key string, obj interface{}) {
	var involved = t.getObj(typeName, key, true, true)
	involved.Version.Object = obj
}


func (t *Transaction) DeleteObj(typeName string, key string) {
	var involved = t.getObj(typeName, key, true, false)
	involved.Version.Object = involved.Obj.Type.NilValue()
}


func (t *Transaction) getObj(typeName string, key string, update bool, create bool) *InvolvedObject {

	if t.State != ACTIVE {
		panic(fmt.Sprintf("GetObj from transaction %s\n", t))
	}

	involved, ok := t.Objs[key]

	if ok {
		if update {
			involved.Dirty = true
		}
		return involved
	}

	var logeObj = t.DB.GetObj(typeName, key)
	if logeObj == nil {
		logeObj = t.DB.CreateObj(typeName, key)
	}

	logeObj.SpinLock()
	defer logeObj.Unlock()

	var fromVersion = logeObj.Current.Version

	involved = &InvolvedObject{
		Obj: logeObj,
		Version: logeObj.NewVersion(),
		FromVersion: fromVersion,
		Dirty: update,
	}

	t.Objs[key] = involved

	return involved
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
	var writeList = make([]*InvolvedObject, 0, len(t.Objs))
	
	for _, involved := range t.Objs {
		if involved == nil {
			continue
		}

		var obj = involved.Obj.Ensure()

		if !obj.TryLock() {
			return false
		}
		defer obj.Unlock()

		if !obj.Applicable(involved.Version) {
			// fmt.Printf("Version mismatch on %s: %d vs %d\n",
			// 	obj.Key, involved.FromVersion, involved.Obj.Current.Version)
			t.State = ABORTED
			return true
		}

		if involved.Dirty {
			writeList = append(writeList, involved)
		}
	}
	
	for _, involved := range writeList {
		involved.Obj.ApplyVersion(involved.Version)
		//fmt.Printf("Wrote %d: %v\n", involved.Version.Version, involved.Obj.Current.Object)
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