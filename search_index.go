package search

import (
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/fanky5g/ponzu/content"
	"github.com/fanky5g/ponzu/driver"
	"github.com/fanky5g/ponzu/util"
	"reflect"
	"strings"
)

type Index struct {
	Name                 string
	idx                  bleve.Index
	contentRepository    driver.Repository
	searchableAttributes []string
}

func (index *Index) Key(id string) string {
	if !strings.HasPrefix(id, index.Name) {
		id = fmt.Sprintf("%s:%s", index.Name, id)
	}

	return id
}

func (index *Index) Update(id string, data interface{}) error {
	if !searchable(data) {
		return nil
	}

	doc := make(map[string]string)
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = reflect.Indirect(v)
	}

	doc[TypeField] = index.Name
	for _, fieldName := range index.searchableAttributes {
		var field reflect.Value

		fieldByName := v.FieldByName(fieldName)
		if fieldByName.IsValid() {
			field = fieldByName
		} else {
			field = util.FieldByJSONTagName(data, fieldName)
		}

		if !field.IsValid() {
			return fmt.Errorf("invalid field %s", fieldName)
		}

		if field.Kind() != reflect.String {
			return fmt.Errorf("%s type %s is not supported in search", fieldName, field.Kind())
		}

		if !field.IsZero() {
			doc[fieldName] = field.String()
		}
	}

	return index.idx.Index(index.Key(id), doc)
}

func (index *Index) Delete(id string) error {
	return index.idx.Delete(index.Key(id))
}

func (index *Index) SearchWithPagination(query string, count, offset int) ([]interface{}, int, error) {
	q := bleve.NewQueryStringQuery(fmt.Sprintf("+%s:%s +%s", TypeField, index.Name, query))
	searchRequest := bleve.NewSearchRequestOptions(q, count, offset, false)
	res, err := index.idx.Search(searchRequest)
	if err != nil {
		return nil, 0, err
	}

	var results []interface{}
	for _, hit := range res.Hits {
		var entity interface{}
		entity, err = index.contentRepository.FindOneById(hit.ID)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to find entity: %v", err)
		}

		if entity != nil {
			results = append(results, entity)
		}
	}

	// since we only index searchable fields. We need to fetch the original entities
	return results, res.Size(), nil
}

func (index *Index) Search(query string, count, offset int) ([]interface{}, error) {
	results, _, err := index.SearchWithPagination(query, count, offset)
	return results, err
}

func NewSearchIndex(entity content.Entity, index bleve.Index, repo driver.Repository) (driver.SearchInterface, error) {
	searchableAttributes, err := getSearchableFields(entity)
	if err != nil {
		return nil, err
	}

	return &Index{
		Name:                 entity.EntityName(),
		idx:                  index,
		contentRepository:    repo,
		searchableAttributes: searchableAttributes,
	}, nil
}
