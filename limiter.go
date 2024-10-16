package aerospike

import (
	"fmt"
	"os"
	"strconv"
	"sync"
)

func maxConcurrentWrite() (int, bool, error) {
	value := os.Getenv("AEROSPIKE_CONCURRENT_WRITE")
	if value == "" {
		return 0, false, nil
	}
	ret, err := strconv.Atoi(value)
	if err != nil {
		return 0, true, fmt.Errorf("invalid AEROSPIKE_CONCURRENT_WRITE: '%v' %w", value, err)
	}
	return ret, err == nil, err
}

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
