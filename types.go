package aerospike

import "github.com/viant/x"

var globalSets = newRegistry()

func registerSet(aSet *set) error {
	return globalSets.Register(aSet)
}

// RegisterSet register set
func RegisterSet(xType *x.Type, options ...Option) error {
	aSet := &set{xType: xType}
	for _, option := range options {
		option(aSet)
	}
	return globalSets.Register(aSet)
}
