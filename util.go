package search

import (
    "reflect"
    "fmt"
)

// getSearchableFields returns fields that are supported for search
func getSearchableFields(entity interface{}) ([]string, error) {
	v := reflect.Indirect(reflect.ValueOf(entity))
	t := v.Type()

	var searchableFields []string
	searchableAttributes, ok := entity.(CustomizableSearchAttributes)
	if ok {
		for attribute, attributeType := range searchableAttributes.GetSearchableAttributes() {
			if attributeType.Kind() != reflect.String {
				return nil, fmt.Errorf("%s is not supported for search", attributeType.Kind())
			}

			field := v.FieldByName(attribute)
			if !field.IsValid() {
				field = fieldByJSONTagName(entity, attribute)
			}

			if !field.IsValid() {
				return nil, fmt.Errorf("invalid field %s", attribute)
			}

			searchableFields = append(searchableFields, attribute)
		}
		return searchableFields, nil
	}

	for i := 0; i < v.NumField(); i++ {
		structField := t.Field(i)
		field := v.Field(i)

		if field.Kind() == reflect.String {
			fieldName := structField.Name
			if jsonTag, ok := structField.Tag.Lookup("json"); ok && jsonTag != "-" {
				fieldName = jsonTag
			}

			searchableFields = append(searchableFields, fieldName)
		}
	}

	return searchableFields, nil
}

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
