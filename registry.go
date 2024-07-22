package aerospike

import (
	"fmt"
	"sync"
)

type (
	//registry represents a set registry
	registry struct {
		mux   sync.RWMutex
		types map[string]*set
	}
)

// Register registers a set
func (r *registry) Register(aSet *set) error {
	if aSet == nil {
		return fmt.Errorf("unable to register set: set is nil")
	}
	r.register(aSet)
	return nil
}
func (r *registry) clear() {
	r.mux.Lock()
	r.types = make(map[string]*set)
	r.mux.Unlock()

}

func (r *registry) sets() []string {
	r.mux.RLock()
	keys := make([]string, 0, len(r.types))
	for key := range r.types {
		keys = append(keys, key)
	}
	r.mux.RUnlock()
	return keys
}

func (r *registry) register(aSet *set) {
	r.mux.RLock()
	key := aSet.xType.Name //TODO check if not nil
	r.mux.RUnlock()

	r.mux.Lock()
	r.types[key] = aSet
	r.mux.Unlock()
}

// Lookup returns a set by name
func (r *registry) Lookup(name string) *set {
	r.mux.RLock()
	aSet, _ := r.types[name]
	r.mux.RUnlock()
	return aSet
}

// newRegistry creates a registry
func newRegistry() *registry {
	ret := &registry{types: make(map[string]*set)}
	return ret
}

// Merge merges registry
func (r *registry) Merge(registry *registry) {
	for _, aSet := range registry.types {
		r.register(aSet)
	}
}
