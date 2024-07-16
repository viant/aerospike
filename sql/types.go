package sql

var globalSets = NewRegistry()

func RegisterGlobalSet(aSet *set) error {
	return globalSets.Register(aSet)
}
