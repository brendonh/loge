package loge

import (
	"sync/atomic"
	"runtime"
)

type lockToken struct{}

var _lock_token struct{}

type spinLock struct {
	lock int32
}

func (lock *spinLock) TryLock() bool {
	return atomic.CompareAndSwapInt32(
		&lock.lock, UNLOCKED, LOCKED)
}

func (lock *spinLock) SpinLock() {
	for {
		if lock.TryLock() {
			return
		}
		runtime.Gosched()
	}
}

func (lock *spinLock) Unlock() {
	lock.lock = UNLOCKED
}
