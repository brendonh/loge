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


func (t *Transaction) ReadObj(key string) interface{} {
	return t.GetObj(key, false)
}


func (t *Transaction) WriteObj(key string) interface{} {
	return t.GetObj(key, true)
}


func (t *Transaction) GetObj(key string, update bool) interface{} {

	if t.State != ACTIVE {
		panic(fmt.Sprintf("GetObj from transaction %s\n", t))
	}

	involved, ok := t.Objs[key]
	if ok {
		if update {
			involved.Dirty = true
		}
		return involved.Version.Object
	}

	var logeObj = t.DB.GetObj(key)
	if logeObj == nil {
		return nil
	}

	var fromVersion = logeObj.Current.Version

	involved = &InvolvedObject{
		Obj: logeObj,
		Version: logeObj.NewVersion(),
		FromVersion: fromVersion,
		Dirty: update,
	}

	t.Objs[key] = involved

	return involved.Version.Object
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
		if !involved.Obj.TryLock() {
			return false
		}
		defer involved.Obj.Unlock()		

		if involved.FromVersion != involved.Obj.Current.Version {
			// fmt.Printf("Version mismatch on %s: %d vs %d\n",
			// 	key, involved.FromVersion, involved.Obj.Current.Version)
			t.State = ABORTED
			return true
		}

		if involved.Dirty {
			writeList = append(writeList, involved)
		}
	}
	
	for _, involved := range writeList {
		involved.Obj.ApplyVersion(involved.Version)
		//fmt.Printf("Writing %v: %v\n", involved.Obj.Key, success)
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