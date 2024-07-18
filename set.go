package aerospike

import "github.com/viant/x"

type set struct {
	xType           *x.Type
	typeBasedMapper *mapper
	queryMapper     map[string]*mapper
	ttlSec          uint32
}

func (s *set) lookupQueryMapper(query string) *mapper {
	if len(s.queryMapper) == 0 {
		return nil
	}
	return s.queryMapper[query]
}

func (s *set) registerQueryMapper(query string, aMapper *mapper) {
	if len(s.queryMapper) == 0 {
		s.queryMapper = make(map[string]*mapper)
	}
	s.queryMapper[query] = aMapper
}
