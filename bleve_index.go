package search

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/token/keyword"
	"github.com/blevesearch/bleve/mapping"
)

var (
	ErrIndexNotFound = errors.New("index not found")
)

func (c *Client) getBleveIndex(entity Entity) (bleve.Index, error) {
	idxName := fmt.Sprintf("%s%s", entity.EntityName(), IndexSuffix)
	idxPath := filepath.Join(c.searchPath, idxName)

	index, err := c.getExistingIndex(idxPath)
	if err != nil {
		if err == ErrIndexNotFound {
			indexMapping, err := c.getIndexMapping(entity)
			if err != nil {
				return nil, errors.Wrap(err, "failed to get index mapping")
			}

			index, err = c.persistIndex(idxPath, indexMapping)
			if err != nil {
				return nil, errors.Wrap(err, "failed to persist new index")
			}
		}
	}

    return index, nil
}

func (c *Client) getExistingIndex(indexPath string) (bleve.Index, error) {
	entityName := strings.TrimSuffix(path.Base(indexPath), IndexSuffix)

	var index bleve.Index
	index, err := bleve.Open(indexPath)
	if err != nil {
		return nil, ErrIndexNotFound
	}

	index.SetName(entityName)
	return index, nil
}

func (c *Client) persistIndex(indexPath string, mapping *mapping.IndexMappingImpl) (bleve.Index, error) {
	mapping.TypeField = TypeField
	_, err := os.Stat(indexPath)
	if err == nil {
		if err = os.RemoveAll(indexPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing index: %v", err)
		}
	}

	return bleve.New(indexPath, mapping)
}

func (c *Client) getIndexMapping(entity Entity) (*mapping.IndexMappingImpl, error) {
	typeField := bleve.NewTextFieldMapping()
	typeField.Analyzer = keyword.Name

	entityMapping := bleve.NewDocumentMapping()
	entityMapping.AddFieldMappingsAt(TypeField, typeField)
	searchableAttributes, err := getSearchableFields(entity)
	if err != nil {
		return nil, err
	}

	for _, fieldName := range searchableAttributes {
		fieldMapping := bleve.NewTextFieldMapping()
		entityMapping.AddFieldMappingsAt(fieldName, fieldMapping)
	}

	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultMapping = entityMapping

	return indexMapping, nil
}
