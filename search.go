package search

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/blevesearch/bleve"
	"github.com/pkg/errors"
)

var (
	TypeField              = "type"
	IndexSuffix            = ".index"
	ErrInvalidSearchEntity = errors.New("invalid search entity")
)

type Client struct {
	searchPath string
	indexes    map[string]*Index
}

func New(dataDir string) (*Client, error) {
	var err error

	searchPath := filepath.Join(dataDir, "search")
	if err = os.MkdirAll(searchPath, os.ModeDir|os.ModePerm); err != nil {
		return nil, err
	}

	c := &Client{
		indexes:    make(map[string]*Index),
		searchPath: searchPath,
	}

	return c, err
}

func (c *Client) Update(id string, data interface{}) error {
	entity, ok := data.(Entity)
	if !ok {
		return ErrInvalidSearchEntity
	}

	index, err := c.index(entity)
	if err != nil {
		return err
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
			field = fieldByJSONTagName(data, fieldName)
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

	return index.idx.Index(id, doc)
}

func (c *Client) Delete(entityName, entityId string) error {
	index, ok := c.indexes[entityName]
	if !ok {
		return nil
	}

	return index.idx.Delete(entityId)
}

func (c *Client) SearchWithPagination(entity interface{}, query string, count, offset int) ([]interface{}, int, error) {
	index, err := c.index(entity)
	if err != nil {
		return nil, 0, err
	}

	q := bleve.NewQueryStringQuery(fmt.Sprintf("+%s:%s +%s", TypeField, index.Name, query))
	searchRequest := bleve.NewSearchRequestOptions(q, count, offset, false)
	res, err := index.idx.Search(searchRequest)
	if err != nil {
		return nil, 0, err
	}

	results := make([]interface{}, len(res.Hits)) 
	for i, doc := range res.Hits {
		results[i] = Hit{doc: doc}
	}

	return results, int(res.Total), nil
}

func (c *Client) Search(entity interface{}, query string, count, offset int) ([]interface{}, error) {
	results, _, err := c.SearchWithPagination(entity, query, count, offset)
	return results, err
}

func (c *Client) index(e interface{}) (*Index, error) {
	entity, ok := e.(Entity)
	if !ok {
		return nil, ErrInvalidSearchEntity
	}

	index, ok := c.indexes[entity.EntityName()]
	if !ok {
		bleveIndex, err := c.getBleveIndex(entity)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get bleve index")
		}

		index, err = NewSearchIndex(entity, bleveIndex)
		if err != nil {
			return nil, errors.Wrap(err, "failed to initialize search index")
		}

		c.indexes[entity.EntityName()] = index
	}

	return index, nil
}
