package aerospike

var globalSets = NewRegistry()

func RegisterGlobalSet(aSet *set) error {
	return globalSets.Register(aSet)
}
