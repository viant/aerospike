package sql

import (
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v4"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
)

func (s *Statement) prepareUpdate(sql string) error {
	var err error
	if s.update, err = sqlparser.ParseUpdate(sql); err != nil {
		return err
	}
	s.setSet(sqlparser.Stringify(s.update.Target.X))
	return nil
}

func (s *Statement) handleUpdate(args []driver.NamedValue) error {
	if s.update == nil {
		return fmt.Errorf("insert statement is not initialized")
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
			return fmt.Errorf("unable to find field %v in type %T", column, s.recordType)
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
				addValue = aField.ensureValidValueType(addValue)
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
			value = aField.ensureValidValueType(value)
			putBins[aField.Column()] = value
		}
	}

	args = args[j:]
	if err := s.updateCriteria(s.update.Qualify, args, false); err != nil {
		return err
	}

	if s.mapBin != "" {
		if len(s.keyValues) != 1 {
			return fmt.Errorf("update statement map must have one map key")
		}
		binKey := as.CtxMapKey(as.NewValue(s.keyValues[0]))
		for key, value := range addBins {
			operates = append(operates, as.MapIncrementOp(nil, s.mapBin, key, value, binKey))
		}
		for key, value := range subBins {
			operates = append(operates, as.MapDecrementOp(nil, s.mapBin, key, value, binKey))
		}
		for key, value := range putBins {
			operates = append(operates, as.MapPutOp(nil, s.mapBin, key, value, binKey))
		}
	} else {
		for key, value := range addBins {
			operates = append(operates, as.AddOp(as.NewBin(key, value)))
		}

		for key, value := range subBins {
			operates = append(operates, as.AddOp(as.NewBin(key, value)))
		}
		for key, value := range putBins {
			operates = append(operates, as.PutOp(as.NewBin(key, negate(value))))
		}
	}

	keys, err := s.buildKeys()
	if err != nil {
		return err
	}
	if len(keys) != 1 {
		return fmt.Errorf("update statement must have one pk")
	}
	writePolicy := as.NewWritePolicy(0, 0)
	for _, key := range keys {
		if _, err = s.client.Operate(writePolicy, key, operates...); err != nil {
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