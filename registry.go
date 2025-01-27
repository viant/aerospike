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
	return r.register(aSet)
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

func (r *registry) register(aSet *set) error {
	if aSet == nil {
		return fmt.Errorf("unable to register set: set is nil")
	}
	if aSet.xType.Name == "" {
		return fmt.Errorf("unable to register set: set name is empty")
	}

	r.mux.Lock()
	defer r.mux.Unlock()

	key := aSet.xType.Name
	if _, exists := r.types[key]; exists {
		return nil
	}
	r.types[key] = aSet
	return nil
}

// Lookup returns a set by name
func (r *registry) Lookup(name string) *set {
	r.mux.RLock()
	aSet, _ := r.types[name]
	r.mux.RUnlock()
	return aSet
}

// Has returns true if set is registered
func (r *registry) Has(name string) bool {
	return r.Lookup(name) != nil
}

// newRegistry creates a registry
func newRegistry() *registry {
	ret := &registry{types: make(map[string]*set)}
	return ret
}

// Merge merges registry
func (r *registry) Merge(registry *registry) error {
	registry.mux.RLock()
	defer registry.mux.RUnlock()
	for _, aSet := range registry.types {
		if err := r.Register(aSet); err != nil {
			return err
		}
	}
	return nil
}
