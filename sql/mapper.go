package sql

import (
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/structology"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})
var timePtrType = reflect.TypeOf(&time.Time{})

type (
	field struct {
		tag    *Tag
		setter structology.Setter
		*xunsafe.Field
		index int
	}

	mapper struct {
		fields        []field
		collectionBin string
		pk            []*field
		key           []*field
		byName        map[string]int
	}
)

func (f *field) ensureValidValueType(value interface{}) (interface{}, error) {
	valueType := reflect.TypeOf(value)
	if valueType.Kind() == f.Type.Kind() {
		if valueType == timeType {
			v, ok := value.(time.Time)
			if !ok {
				return nil, fmt.Errorf("unable to ensure valid value type - invalid type %T expected %T", value, v)
			}
			if f.tag.UnixSec {
				value = v.Unix()
			}
		}
		return value, nil
	}

	if valueType.AssignableTo(f.Type) {
		value = reflect.ValueOf(value).Convert(f.Type).Interface()
	} else {
		// TODO add extra converson logic
		// TODO check pointers
		if f.Type == timeType && valueType.Kind() == reflect.String {
			v, err := time.Parse(time.RFC3339, value.(string))
			if err != nil {
				return nil, fmt.Errorf("unable to ensure valid value type due to: %w", err)
			}

			if f.tag.UnixSec {
				value = v.Unix()
			}
		}
	}
	return value, nil
}

func (f *field) Column() string {
	if f.tag != nil {
		return f.tag.Name
	}
	return f.Field.Name
}
func (t *mapper) lookup(name string) *xunsafe.Field {
	field := t.getField(name)
	if field == nil {
		return nil
	}
	return field.Field
}

func (t *mapper) getField(name string) *field {
	pos, ok := t.byName[name]
	if !ok {
		name = strings.ReplaceAll(strings.ToLower(name), "_", "")
		pos, ok = t.byName[name]
	}
	if !ok {
		return nil
	}
	return &t.fields[pos]
}

func (t *mapper) addField(aField reflect.StructField, tag *Tag) *field {
	idx := len(t.fields)
	t.fields = append(t.fields, field{index: idx, Field: xunsafe.NewField(aField), tag: tag})
	mapperField := &t.fields[idx]
	if tag.IsMapKey {
		t.key = append(t.key, mapperField)
	}
	fuzzName := strings.ReplaceAll(strings.ToLower(tag.Name), "_", "")
	t.byName[tag.Name] = idx
	t.byName[fuzzName] = idx
	return mapperField
}

func newQueryMapper(recordType reflect.Type, list query.List) (*mapper, error) {
	typeMapper, err := newTypeBaseMapper(recordType)
	if err != nil {
		return nil, err
	}
	if list.IsStarExpr() {
		return typeMapper, nil
	}
	result := &mapper{fields: make([]field, 0), byName: make(map[string]int)}
	for i := 0; i < len(list); i++ {
		item := list[i]
		switch actual := item.Expr.(type) {
		case *expr.Ident, *expr.Selector:
			name := sqlparser.Stringify(actual)
			if index := strings.LastIndex(name, "."); index != -1 {
				name = name[index+1:]
			}
			pos, ok := typeMapper.byName[name]
			fuzzName := strings.ReplaceAll(strings.ToLower(name), "_", "")
			if !ok {
				pos, ok = typeMapper.byName[fuzzName]
			}
			if !ok {
				return nil, fmt.Errorf("unable to match column: %v in type: %s", name, recordType.Name())
			}
			idx := len(result.fields)
			result.fields = append(result.fields, typeMapper.fields[pos])
			result.byName[name] = idx
			result.byName[fuzzName] = idx
		default:
			return nil, fmt.Errorf("newmapper: unsupported expression type: %T", actual)
		}
	}
	return result, nil
}

func newTypeBaseMapper(recordType reflect.Type) (*mapper, error) {
	typeMapper := &mapper{fields: make([]field, 0), byName: make(map[string]int)}
	var idIndex *int
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

		idx := len(typeMapper.fields)
		if idIndex == nil {
			if strings.ToLower(tag.Name) == "id" || strings.ToLower(tag.Name) == "pk" {
				idIndex = &idx
			}
		}
		mapperField := typeMapper.addField(aField, tag)
		if tag.IsPK {
			if typeMapper.pk != nil {
				return nil, fmt.Errorf("multiple PK tags detected in %T", recordType)
			}
			typeMapper.pk = append(typeMapper.pk, mapperField)
		}
	}
	if typeMapper.pk == nil && idIndex != nil {
		typeMapper.pk = append(typeMapper.pk, &typeMapper.fields[*idIndex])
		typeMapper.pk[0].tag.IsPK = true
	}
	return typeMapper, nil
}
