package sql

import (
	"fmt"
	"github.com/viant/tagly/tags"
	"strconv"
	"strings"
)

type Tag struct {
	Name      string
	IsPK      bool
	IsMapKey  bool
	IsListKey bool
	Ignore    bool
	UnixSec   bool
}

func (t *Tag) updateTagKey(key, value string) error {
	var err error
	switch strings.ToLower(key) {
	case "-":
		t.Ignore = true
	case "name":
		t.Name = value
	case "pk":
		if value == "" {
			t.IsPK = true
		} else {
			if t.IsPK, err = strconv.ParseBool(value); err != nil {
				return err
			}
		}
	case "listkey":
		if value == "" {
			t.IsListKey = true
		} else if t.IsListKey, err = strconv.ParseBool(value); err != nil {
			return err
		}
	case "key":
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
	tag.Name = name
	err := values.MatchPairs(tag.updateTagKey)
	return tag, err
}
