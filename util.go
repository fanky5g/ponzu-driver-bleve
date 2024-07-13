package search

import (
    "reflect"
)

func fieldByJSONTagName(structType interface{}, jsonTagName string) reflect.Value {
	v := reflect.ValueOf(structType)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	for i := 0; i < v.NumField(); i++ {
		typeField := v.Type().Field(i)
		tag := typeField.Tag

		if jsonTag, ok := tag.Lookup("json"); ok {
			if jsonTag == jsonTagName {
				return v.FieldByName(typeField.Name)
			}
		}
	}

	return reflect.Value{}
}
