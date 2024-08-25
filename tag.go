package aerospike

import (
	"fmt"
	"github.com/viant/tagly/tags"
	"strconv"
	"strings"
)

type Tag struct {
	Name             string
	IsPK             bool
	IsMapKey         bool
	IsSecondaryIndex bool
	IsArrayIndex     bool
	Ignore           bool
	UnixSec          bool
	ArraySize        int
	IsComponent      bool
}

func (t *Tag) updateTagKey(key, value string) error {
	var err error

	switch strings.ToLower(key) {
	case "-":
		t.Ignore = true
	case "name":
		t.Name = value
	case "arraysize":
		if t.ArraySize, err = strconv.Atoi(strings.TrimSpace(value)); err != nil {
			return err
		}
	case "arrayindex":
		if value == "" {
			t.IsArrayIndex = true
		} else if t.IsArrayIndex, err = strconv.ParseBool(value); err != nil {
			return err
		}

	case "component":
		t.IsComponent = true

	case "pk":
		if value == "" {
			t.IsPK = true
		} else {
			if t.IsPK, err = strconv.ParseBool(value); err != nil {
				return err
			}
		}
	case "index", "secondaryindex":
		if value == "" {
			t.IsSecondaryIndex = true
		} else {
			if t.IsSecondaryIndex, err = strconv.ParseBool(value); err != nil {
				return err
			}
		}
	case "mapkey":
		if value == "" {
			t.IsMapKey = true
		} else if t.IsMapKey, err = strconv.ParseBool(value); err != nil {
			return err
		}
	case "unixsec":
		if value == "" {
			t.UnixSec = true
		} else if t.UnixSec, err = strconv.ParseBool(value); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported tag: %s", key)
	}
	return nil
}

func ParseTag(tagString string) (*Tag, error) {
	tag := &Tag{}
	values := tags.Values(tagString)
	name, values := values.Name()
	if name == "-" {
		tag.Ignore = true
	} else {
		tag.Name = name
	}
	err := values.MatchPairs(tag.updateTagKey)
	return tag, err
}
