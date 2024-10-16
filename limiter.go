package aerospike

import (
	"sync"
)

type (
	reteLimiter struct {
		limiter map[string]*limiter
		sync.RWMutex
	}
	limiter chan bool
)

func (l *limiter) acquire() {
	*l <- true
}

func (l *limiter) release() {
	<-*l
}

func (r *reteLimiter) getLimiter(key string, size int) *limiter {
	if size == 0 {
		return nil
	}
	r.RLock()
	limiterPtr, ok := r.limiter[key]
	r.RUnlock()
	if ok {
		return limiterPtr
	}
	r.Lock()
	defer r.Unlock()
	limiterPtr, ok = r.limiter[key]
	if ok {
		return limiterPtr
	}
	rateLimit := make(limiter, size)
	limiterPtr = &rateLimit
	r.limiter[key] = limiterPtr
	return limiterPtr
}

var writeLimiter = newRateLimiter()

func newRateLimiter() *reteLimiter {
	return &reteLimiter{
		limiter: make(map[string]*limiter),
	}
}
