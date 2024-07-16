package sql

import (
	"fmt"
	"sync"
)

type (
	//Registry represents a set registry
	Registry struct {
		mux   sync.RWMutex
		types map[string]*set
	}
)

// Register registers a set
func (r *Registry) Register(aSet *set) error {
	if aSet == nil {
		return fmt.Errorf("unable to register set: set is nil")
	}
	r.register(aSet)
	return nil
}

func (r *Registry) register(aSet *set) {
	r.mux.RLock()
	key := aSet.xType.Key() //TODO check if not nil
	//prev, ok := r.types[key] //TODO: check if we need to keep this
	r.mux.RUnlock()
	//if !ok {
	//	r.mux.Lock()
	//	r.types[key] = aSet
	//	r.mux.Unlock()
	//	return
	//}
	//if prev.Scn >= aSet.Scn {
	//	return
	//}
	r.mux.Lock()
	r.types[key] = aSet
	r.mux.Unlock()
}

// Lookup returns a set by name
func (r *Registry) Lookup(name string) *set {
	r.mux.RLock()
	aSet, _ := r.types[name]
	r.mux.RUnlock()
	return aSet
}

// NewRegistry creates a registry
func NewRegistry() *Registry {
	ret := &Registry{types: make(map[string]*set)}
	return ret
}

// Merge merges registry
func (r *Registry) Merge(registry *Registry) {
	for _, aSet := range registry.types {
		r.register(aSet)
	}
}
