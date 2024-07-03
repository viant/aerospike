package sql

import (
	"fmt"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

type (
	field struct {
		tag *Tag
		*xunsafe.Field
		index int
	}
	mapper struct {
		fields []field
		byName map[string]int
	}
)

func (m *mapper) lookup(name string) *xunsafe.Field {
	pos, ok := m.byName[name]
	if !ok {
		name = strings.ReplaceAll(strings.ToLower(name), "_", "")
		pos, ok = m.byName[name]
	}
	if !ok {
		return nil
	}
	return m.fields[pos].Field
}

func newMapper(recordType reflect.Type, list query.List) (*mapper, error) {
	m := &mapper{fields: make([]field, 0), byName: make(map[string]int)}
	for i := 0; i < recordType.NumField(); i++ {
		aField := recordType.Field(i)
		tag, err := ParseTag(aField.Tag.Get("aerospike"))
		if err != nil {
			return nil, err
		}
		if tag.Name == "" {
			tag.Name = aField.Name
		}
		if tag.Ignore {
			continue
		}
		idx := len(m.fields)
		m.fields = append(m.fields, field{index: idx, Field: xunsafe.NewField(aField), tag: tag})
		fuzzName := strings.ReplaceAll(strings.ToLower(tag.Name), "_", "")
		m.byName[tag.Name] = idx
		m.byName[fuzzName] = idx
	}

	if list.IsStarExpr() {
		return m, nil
	}

	listMapper := &mapper{fields: make([]field, 0), byName: make(map[string]int)}

	for i := 0; i < len(list); i++ {
		item := list[i]

		switch actual := item.Expr.(type) {
		case *expr.Ident:
			pos, ok := m.byName[actual.Name]
			fuzzName := strings.ReplaceAll(strings.ToLower(actual.Name), "_", "")
			if !ok {
				pos, ok = m.byName[fuzzName]
			}
			if !ok {
				return nil, fmt.Errorf("unable to match column: %v in type: %s", actual.Name, recordType.Name())
			}
			idx := len(listMapper.fields)
			listMapper.fields = append(listMapper.fields, m.fields[pos])
			listMapper.byName[actual.Name] = idx
			listMapper.byName[fuzzName] = idx
		}
	}
	return listMapper, nil
}
