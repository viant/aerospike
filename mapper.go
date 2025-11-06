package aerospike

import (
	"encoding/json"
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/structology"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})
var timePtrType = reflect.TypeOf(&time.Time{})
var timePtr = &time.Time{}
var timeDoublePtrType = reflect.TypeOf(&timePtr)

type (
	field struct {
		tag    *Tag
		setter structology.Setter
		*xunsafe.Field
		index    int
		isPseudo bool
		isFunc   bool
		value    interface{}
	}

	mapper struct {
		fields           []field
		pk               []*field
		mapKey           []*field
		arrayIndex       *field
		secondaryIndex   *field
		component        *field
		arraySize        int
		byName           map[string]int
		columnList       map[string]bool
		pseudoColumns    map[string]interface{}
		aggregateColumn  map[string]*expr.Call
		columnZeroValues map[string]interface{}
		groupBy          []int
		zeroMux          sync.RWMutex
	}
)

func (f *field) ensureValidValueType(value interface{}) (interface{}, error) {

	if iFacePtr, ok := value.(*interface{}); ok && iFacePtr != nil {
		value = *iFacePtr
	}
	valueType := reflect.TypeOf(value)
	if valueType == nil {
		valueType = f.Type
	}
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
		if valueType == timePtrType {
			v, ok := value.(*time.Time)
			if !ok {
				return nil, fmt.Errorf("unable to ensure valid value type - invalid type %T expected %T", value, v)
			}
			if f.tag.UnixSec {
				value = v.Unix()
			}
		}
		if valueType == timeDoublePtrType {
			v, ok := value.(**time.Time)
			if !ok {
				return nil, fmt.Errorf("unable to ensure valid value type - invalid type %T expected %T", value, v)
			}
			if f.tag.UnixSec {
				vv := *v
				value = vv.Unix()
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
		} else if valueType.Kind() == reflect.Ptr {
			var err error
			value, err = extractValue(value)
			if err != nil {
				return nil, fmt.Errorf("unable to ensure valid value type due to: %w", err)
			}
		}

		basicType := baseType(f.Type)
		valueType = reflect.TypeOf(value)
		if basicType.Kind() != valueType.Kind() {
			b := basicType.Kind()

			switch t := valueType.Kind(); t {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				switch b {
				case reflect.Float32:
					value = float32(value.(int))
				case reflect.Float64:
					value = float64(value.(int))
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				switch b {
				case reflect.Float32:
					value = float32(value.(uint))
				case reflect.Float64:
					value = float64(value.(uint))
				}
			case reflect.Float32, reflect.Float64:
				return nil, fmt.Errorf("unable to ensure valid type: can't convert value %v (%v) to type %v", value, t, b)
			}
		}
	}

	return value, nil
}

func extractValue(value interface{}) (interface{}, error) {
	switch actual := value.(type) {
	case *int64:
		if actual == nil {
			return nil, nil
		}
		return int(*actual), nil
	case *int32:
		if actual == nil {
			return nil, nil
		}
		return int(*actual), nil
	case *float64:
		if actual == nil {
			return nil, nil
		}
		return int(*actual), nil
	case *float32:
		if actual == nil {
			return nil, nil
		}
		return int(*actual), nil
	case *int16:
		if actual == nil {
			return nil, nil
		}
		return int(*actual), nil
	case *int:
		if actual == nil {
			return nil, nil
		}
		return *actual, nil
	case *string:
		if actual == nil {
			return nil, nil
		}
		return *actual, nil
	case **time.Time:
		if actual == nil || *actual == nil {
			return nil, nil
		}
		tValue := *(*actual)
		return tValue.Format(time.RFC3339), nil
	case *time.Time:
		if actual == nil {
			return nil, nil
		}
		tValue := *actual
		return tValue.Format(time.RFC3339), nil
	case *[]string:
		if actual == nil {
			return nil, nil
		}
		return *actual, nil
	case *[]byte:
		if actual == nil {
			return nil, nil
		}
		return *actual, nil
	case *json.RawMessage:
		if actual == nil {
			return nil, nil
		}
		// store as []byte to ensure Aerospike client treats it as a blob
		return []byte(*actual), nil
	case *[]int:
		if actual == nil {
			return nil, nil
		}
		return *actual, nil
	case *interface{}:
		if actual == nil {
			return nil, nil
		}
		return *actual, nil
	case *bool:
		if actual == nil {
			return nil, nil
		}
		return *actual, nil
	default:
		return nil, fmt.Errorf("extractValue: unsupported type %T", actual)
	}
}

func extractKeyValue(value interface{}) (interface{}, error) {
	switch actual := value.(type) {
	case *string:
		if actual == nil {
			return "", nil
		}
		return *actual, nil
	}
	return value, nil
}

func (f *field) Column() string {
	if f.tag != nil {
		return f.tag.Name
	}
	return f.Field.Name
}
func (m *mapper) lookup(name string) *xunsafe.Field {
	field := m.getField(name)
	if field == nil {
		return nil
	}
	return field.Field
}

func (m *mapper) expandBins(extraBins ...string) []string {
	bins := make([]string, 0, len(m.fields))
	unique := map[string]bool{}
	for _, field := range m.fields {
		bins = append(bins, field.Column())
		unique[field.Column()] = true
	}
	for _, extraBin := range extraBins {
		if extraBin == "" {
			continue
		}
		if _, found := unique[extraBin]; !found {
			bins = append(bins, extraBin)
		}
	}
	return bins
}

func (m *mapper) getField(name string) *field {
	pos, ok := m.byName[name]
	if !ok {
		name = strings.ReplaceAll(strings.ToLower(name), "_", "")
		pos, ok = m.byName[name]
	}
	if !ok {
		return nil
	}
	return &m.fields[pos]
}

func (m *mapper) addField(aField reflect.StructField, tag *Tag) *field {
	idx := len(m.fields)
	m.fields = append(m.fields, field{index: idx, Field: xunsafe.NewField(aField), tag: tag})
	mapperField := &m.fields[idx]
	if tag.IsMapKey {
		m.mapKey = append(m.mapKey, mapperField)
	}
	if tag.IsArrayIndex {
		m.arrayIndex = mapperField
	}

	if tag.IsSecondaryIndex {
		m.secondaryIndex = mapperField
	}

	if tag.IsComponent {
		m.component = mapperField
	}
	if tag.ArraySize > 0 {
		m.arraySize = tag.ArraySize
	}

	fuzzName := strings.ReplaceAll(strings.ToLower(tag.Name), "_", "")
	m.byName[tag.Name] = idx
	m.byName[fuzzName] = idx
	return mapperField
}

func newQueryMapper(recordType reflect.Type, aQuery *query.Select, typeMapper *mapper) (*mapper, error) {
	if typeMapper == nil {
		return nil, fmt.Errorf("newquerymapper: typeMapper is nil")
	}
	list := aQuery.List
	if list.IsStarExpr() {
		return typeMapper, nil
	}

	ret := &mapper{
		fields:          make([]field, 0),
		byName:          make(map[string]int),
		arrayIndex:      typeMapper.arrayIndex,
		arraySize:       typeMapper.arraySize,
		secondaryIndex:  typeMapper.secondaryIndex,
		component:       typeMapper.component,
		mapKey:          typeMapper.mapKey,
		pk:              typeMapper.pk,
		pseudoColumns:   make(map[string]interface{}),
		aggregateColumn: make(map[string]*expr.Call),
		groupBy:         []int{},
	}
	for i := 0; i < len(list); i++ {
		item := list[i]
		switch actual := item.Expr.(type) {
		case *expr.Literal:
			var rType reflect.Type
			switch actual.Kind {
			case "string":
				ret.pseudoColumns[item.Alias] = strings.Trim(actual.Value, "'")
				rType = reflect.TypeOf((*string)(nil)).Elem()
			case "int":

				value, err := strconv.Atoi(actual.Value)
				if err != nil {
					return nil, fmt.Errorf("unable to convert %v to int", actual.Value)
				}
				ret.pseudoColumns[item.Alias] = value
				rType = reflect.TypeOf((*string)(nil)).Elem()
			case "numeric":
				value, err := strconv.ParseFloat(actual.Value, 64)
				if err != nil {
					return nil, fmt.Errorf("unable to convert %v to float", actual.Value)
				}
				ret.pseudoColumns[item.Alias] = value
				rType = reflect.TypeOf((*float64)(nil)).Elem()
			}
			if err := ret.appendField(recordType, item.Alias, typeMapper, true, false, rType); err != nil {
				return nil, err
			}
		case *expr.Ident, *expr.Selector:
			name := sqlparser.Stringify(actual)
			if err := ret.appendField(recordType, name, typeMapper, false, false, nil); err != nil {
				return nil, err
			}
		case *expr.Call:
			funName := sqlparser.Stringify(actual.X)
			switch strings.ToLower(funName) {
			case "count":
				if item.Alias == "" {
					item.Alias = "t" + strconv.Itoa(i)
				}
				ret.aggregateColumn[item.Alias] = actual
			}
			if err := ret.appendField(recordType, item.Alias, typeMapper, false, true, reflect.TypeOf(0)); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("newmapper: unsupported expression type: %T", actual)
		}
	}

	if aQuery.GroupBy != nil {
		for i := 0; i < len(aQuery.GroupBy); i++ {
			aGroup := aQuery.GroupBy[i]
			switch actual := aGroup.Expr.(type) {
			case *expr.Literal:
				if actual.Kind != "int" {
					return nil, fmt.Errorf("unsupported group by literal kind: %v", actual.Kind)
				}
				idx, _ := strconv.Atoi(actual.Value)
				ret.groupBy = append(ret.groupBy, idx-1)
			case *expr.Ident, *expr.Selector:
				return nil, fmt.Errorf("unsupported group by expression type: %T, only position based group by is supported", actual)
			}
		}

		if len(ret.groupBy) != 1 {
			return nil, fmt.Errorf("unsupported group by expression type: %T, only single group by is supported", aQuery.GroupBy)
		}
		groupColumn := ret.fields[ret.groupBy[0]]
		if groupColumn.Column() != ret.pk[0].Column() {
			return nil, fmt.Errorf("group by column: %v must match primary key column: %v", groupColumn.Column(), ret.pk[0].Column())
		}
	}
	return ret, nil
}

func (m *mapper) appendField(recordType reflect.Type, name string, typeMapper *mapper, pseudo bool, fun bool, rType reflect.Type) error {
	if index := strings.LastIndex(name, "."); index != -1 {
		name = name[index+1:]
	}

	pos, ok := typeMapper.byName[name]
	fuzzName := strings.ReplaceAll(strings.ToLower(name), "_", "")
	if pseudo {
		m.fields = append(m.fields,
			field{
				Field: &xunsafe.Field{
					Name: name,
					Type: rType,
				},
				tag:      &Tag{Name: name},
				isPseudo: true,
			})
		idx := len(m.fields)
		m.byName[name] = idx
		m.byName[fuzzName] = idx
		return nil
	} else if fun {
		m.fields = append(m.fields,
			field{
				Field: &xunsafe.Field{
					Name: name,
					Type: rType,
				},
				tag:    &Tag{Name: name},
				isFunc: fun,
			})
		idx := len(m.fields)
		m.byName[name] = idx
		m.byName[fuzzName] = idx
		return nil
	}

	if !ok {
		pos, ok = typeMapper.byName[fuzzName]
	}
	if !ok {
		return fmt.Errorf("unable to match column: %v in type: %s", name, recordType.Name())
	}
	idx := len(m.fields)
	m.fields = append(m.fields, typeMapper.fields[pos])
	m.byName[name] = idx
	m.byName[fuzzName] = idx
	return nil
}

func newTypeBasedMapper(recordType reflect.Type) (*mapper, error) {
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

	typeMapper.columnZeroValues = make(map[string]interface{})

	return typeMapper, nil
}

func baseType(rType reflect.Type) reflect.Type {
	switch rType.Kind() {
	case reflect.Ptr, reflect.Array, reflect.Chan, reflect.Map, reflect.Slice:
		return baseType(rType.Elem())
	default:
		return rType
	}
}

func (m *mapper) columnZeroValue(name string) interface{} {
	m.zeroMux.RLock()
	value, ok := m.columnZeroValues[name]
	m.zeroMux.RUnlock()

	if ok {
		return value
	}

	var zeroValue interface{}
	xfield := m.lookup(name)

	t := baseType(xfield.Type)
	switch t.Kind() {
	case reflect.Float32, reflect.Float64:
		zeroValue = 0.0
	case reflect.String:
		zeroValue = ""
	case reflect.Bool:
		zeroValue = false
	default:
		zeroValue = 0
	}

	m.zeroMux.Lock()
	m.columnZeroValues[name] = zeroValue
	m.zeroMux.Unlock()

	return zeroValue
}

func (m *mapper) newSlice() any {
	sliceType := reflect.SliceOf(m.component.Type)
	slice := reflect.MakeSlice(sliceType, m.arraySize, m.arraySize)
	return slice.Interface()
}
