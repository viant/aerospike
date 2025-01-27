package aerospike

import (
	"github.com/viant/x"
	"sync"
)

type set struct {
	xType           *x.Type
	typeBasedMapper *mapper
	queryMapper     map[string]*mapper
	ttlSec          uint32
	mux             sync.RWMutex
}

func (s *set) lookupQueryMapper(query string) *mapper {
	if len(s.queryMapper) == 0 {
		return nil
	}
	s.mux.RLock()
	defer s.mux.RUnlock()
	return s.queryMapper[query]
}

func (s *set) registerQueryMapper(query string, aMapper *mapper) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if len(s.queryMapper) == 0 {
		s.queryMapper = make(map[string]*mapper)
	}
	s.queryMapper[query] = aMapper
}
