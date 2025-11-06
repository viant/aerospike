package aerospike

import (
	"context"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"reflect"
)

func (s *Statement) prepareUpdate(sql string) error {
	var err error
	if s.update, err = sqlparser.ParseUpdate(sql); err != nil {
		return err
	}
	s.setSet(sqlparser.Stringify(s.update.Target.X))
	return nil
}

func (s *Statement) handleUpdate(ctx context.Context, args []driver.NamedValue) error {
	if s.update == nil {
		return fmt.Errorf("update statement is not initialized")
	}

	s.affected = 1
	if s.writeLimiter != nil {
		defer s.writeLimiter.release()
		s.writeLimiter.acquire()
	}
	var operates []*as.Operation
	j := 0
	var putBins = map[string]interface{}{}
	var addBins = map[string]interface{}{}
	var subBins = map[string]interface{}{}

	for _, item := range s.update.Set {
		column := sqlparser.Stringify(item.Column)
		aField := s.mapper.getField(column)
		if aField == nil {
			return fmt.Errorf("unable to find field %v in type %s", column, s.recordType.String())
		}
		var value interface{}
		if item.IsExpr() {
			binary := item.Expr.(*expr.Binary)
			switch binary.Op {
			case "+", "-":
				values, err := binary.Values()
				if err != nil {
					return err
				}
				if len(values.X) != 1 {
					return fmt.Errorf("invalid value  length: %v in %v", values, binary.Op)
				}
				addValue := values.X[0].Value
				if values.X[0].Placeholder {
					addValue = args[j].Value
					j++
				}
				addValue, err = aField.ensureValidValueType(addValue)
				if err != nil {
					return err
				}
				if binary.Op == "+" {
					addBins[aField.Column()] = addValue
				} else {
					subBins[aField.Column()] = addValue
				}
			default:
				return fmt.Errorf("unsupported update column operator: %s, supported(+,-)", binary.Op)
			}
		} else {
			itemValue, err := item.Value()
			if err != nil {
				return err
			}
			if itemValue.Placeholder {
				value = args[j].Value
				j++
			} else {
				value = itemValue.Value
			}
			value, err = aField.ensureValidValueType(value)
			if err != nil {
				return err
			}
			// Normalize custom []byte-like types (e.g., json.RawMessage) to []byte so Aerospike stores blob
			if value != nil {
				v := reflect.ValueOf(value)
				if v.IsValid() && v.Type().Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
					base := reflect.TypeOf([]byte(nil))
					if v.Type() != base && v.Type().ConvertibleTo(base) {
						value = v.Convert(base).Interface()
					}
				}
			}
			putBins[aField.Column()] = value
		}
	}

	args = args[j:]
	if err := s.updateCriteria(s.update.Qualify, args, false); err != nil {
		return err
	}

	if s.collectionType.IsMap() {
		if len(s.mapKeyValues) != 1 {
			return fmt.Errorf("update statement map must have one map mapKey")
		}
		mapPolicy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
		// ensure map key is not a pointer type
		mk := s.mapKeyValues[0]
		if rv := reflect.ValueOf(mk); rv.IsValid() && rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				mk = nil
			} else {
				mk = rv.Elem().Interface()
			}
		}
		binKey := as.CtxMapKey(as.NewValue(mk))
		// If the update only sets the payload column and no add/sub, replace entire entry value
		payload := findPayloadColumn(s.mapper)
		if payload != "" && len(addBins) == 0 && len(subBins) == 0 && len(putBins) == 1 {
			if v, ok := putBins[payload]; ok {
				// key already normalized in mk
				operates = append(operates, as.MapPutOp(mapPolicy, s.collectionBin, as.NewValue(mk), v))
			} else {
				// fall back to nested ops
				for key, value := range putBins {
					operates = append(operates, as.MapPutOp(mapPolicy, s.collectionBin, key, value, binKey))
				}
			}
			// add/sub bins remain empty in this branch
		} else {
			for key, value := range addBins {
				operates = append(operates, as.MapIncrementOp(mapPolicy, s.collectionBin, key, value, binKey))
			}
			for key, value := range subBins {
				operates = append(operates, as.MapDecrementOp(mapPolicy, s.collectionBin, key, value, binKey))
			}
			for key, value := range putBins {
				operates = append(operates, as.MapPutOp(mapPolicy, s.collectionBin, key, value, binKey))
			}
		}
	} else if s.collectionType.IsArray() || s.collectionType == "" {
		for key, value := range addBins {
			operates = append(operates, as.AddOp(as.NewBin(key, value)))
		}
		for key, value := range subBins {
			operates = append(operates, as.AddOp(as.NewBin(key, negate(value))))
		}
		for key, value := range putBins {
			operates = append(operates, as.PutOp(as.NewBin(key, value)))
		}
	}

	keys, err := s.buildKeys()
	if err != nil {
		return err
	}
	if len(keys) != 1 {
		return fmt.Errorf("update statement must have one pk")
	}

	if isDryRun("update") {
		return nil
	}

	aSet, err := s.lookupSet()
	if err != nil {
		return err
	}

	writePolicy := s.writePolicy(aSet, false)
	for _, key := range keys {
		if _, err = s.operateWithCtx(ctx, writePolicy, key, operates); err != nil {
			return err
		}
	}
	return nil
}

func negate(value interface{}) interface{} {
	switch v := value.(type) {
	case int:
		return -v
	case int32:
		return -v
	case int64:
		return -v
	case float32:
		return -v
	case float64:
		return -v
	}
	return value
}
