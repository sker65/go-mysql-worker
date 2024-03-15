package main

import (
	"reflect"
	"testing"

	"gotest.tools/v3/assert"
)

func TestStringToAnyList(t *testing.T) {
	a1 := []string{"Abc", "Xyz", "Mno"}
	list := StringToAnyList(a1)
	assert.Equal(t, reflect.TypeOf(list).String(), "[]interface {}", "Should be []interface{} / []any")
	assert.Equal(t, len(list), 3, "len(list) should be 3")
	assert.Equal(t, list[0], "Abc", "list[0] should be Abc")
}
