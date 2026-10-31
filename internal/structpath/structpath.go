package structpath

import (
	"fmt"
	"reflect"
	"strings"
)

func Lookup(target interface{}, path []string) (s string, ok bool) {
	currentValue := reflect.ValueOf(target)

	for _, pathElement := range path {
		pathElement := pathElement

		if currentValue.Kind() != reflect.Struct {
			return "", false
		}

		field, found := currentValue.Type().FieldByNameFunc(func(fieldName string) bool {
			return strings.EqualFold(fieldName, pathElement)
		})
		if !found {
			return "", false
		}

		// A promoted field may traverse a nil embedded pointer.
		value, err := currentValue.FieldByIndexErr(field.Index)
		if err != nil {
			return "", false
		}
		currentValue = value
	}

	if !currentValue.IsValid() || !currentValue.CanInterface() {
		return "", false
	}
	if (currentValue.Kind() == reflect.Pointer || currentValue.Kind() == reflect.Interface) && currentValue.IsNil() {
		return "", false
	}

	result, ok := currentValue.Interface().(string)
	if ok {
		return result, true
	}

	stringerResult, ok := currentValue.Interface().(fmt.Stringer)
	if ok {
		return stringerResult.String(), true
	}

	return "", false
}
