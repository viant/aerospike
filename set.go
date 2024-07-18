package aerospike

import "github.com/viant/x"

type set struct {
	xType           *x.Type
	typeBasedMapper *mapper
	queryMapper     *mapper
	ttlSec          uint32
}
