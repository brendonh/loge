package loge

import (
	"sync/atomic"
	"runtime"
)

type lockToken struct{}

var _lock_token struct{}

type SpinLock struct {
	lock int32
}

func (lock *SpinLock) TryLock() bool {
	return atomic.CompareAndSwapInt32(
		&lock.lock, UNLOCKED, LOCKED)
}

func (lock *SpinLock) SpinLock() {
	for {
		if lock.TryLock() {
			return
		}
		runtime.Gosched()
	}
}

func (lock *SpinLock) Unlock() {
	lock.lock = UNLOCKED
}
