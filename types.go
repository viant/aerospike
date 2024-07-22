package aerospike

import "github.com/viant/x"

var globalSets = newRegistry()

func registerSet(aSet *set) error {
	return globalSets.Register(aSet)
}

// RegisterSet register set
func RegisterSet(xType *x.Type) error {
	return globalSets.Register(&set{xType: xType})
}
