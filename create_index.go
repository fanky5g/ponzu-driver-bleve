package search

import (
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/fanky5g/ponzu/content"
	"github.com/fanky5g/ponzu/driver"
	"log"
	"path"
	"path/filepath"
	"strings"
)

func (c *client) getExistingIndex(indexPath string, failOnMissingIndex bool) (driver.SearchInterface, error) {
	entityName := strings.TrimSuffix(path.Base(indexPath), IndexSuffix)
	if searchIndex, ok := c.indexes[entityName]; ok {
		return searchIndex, nil
	}

	entity, ok := c.entities[entityName]
	if !ok {
		return nil, fmt.Errorf("entity for %s not found", entityName)
	}

	var index bleve.Index
	index, err := bleve.Open(indexPath)
	if err != nil {
		if failOnMissingIndex {
			log.Printf("Invalid index in search path: %v\n", err)
			return nil, err
		}

		return nil, nil
	}

	index.SetName(entityName)
	var searchIndex driver.SearchInterface
	searchIndex, err = NewSearchIndex(entity, index, c.repository(entityName))
	if err != nil {
		return nil, fmt.Errorf("failed to create search index: %v", err)
	}

	return searchIndex, nil
}

func (c *client) createIndex(entity content.Entity, overwrite bool) error {
	if !searchable(entity) {
		return nil
	}

	idxName := fmt.Sprintf("%s%s", entity.EntityName(), IndexSuffix)
	idxPath := filepath.Join(c.searchPath, idxName)

	existingIndex, err := c.getExistingIndex(idxPath, overwrite)
	if err != nil {
		if !overwrite {
			return err
		}
	}

	if existingIndex != nil && !overwrite {
		c.indexes[idxName] = existingIndex
		return nil
	}

	typeField := bleve.NewTextFieldMapping()
	typeField.Analyzer = keyword.Name

	entityMapping := bleve.NewDocumentMapping()
	entityMapping.AddFieldMappingsAt(TypeField, typeField)
	searchableAttributes, err := getSearchableFields(entity)
	if err != nil {
		return err
	}

	for _, fieldName := range searchableAttributes {
		fieldMapping := bleve.NewTextFieldMapping()
		entityMapping.AddFieldMappingsAt(fieldName, fieldMapping)
	}

	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultMapping = entityMapping

	index, err := c.persistIndex(idxPath, indexMapping)
	if err != nil {
		return fmt.Errorf("failed to build index: %v", err)
	}

	searchIndex, err := NewSearchIndex(entity, index, c.repository(entity.EntityName()))
	if err != nil {
		return err
	}

	c.indexes[entity.EntityName()] = searchIndex
	return nil
}

func (c *client) CreateIndex(entityName string, entityType interface{}) error {
	return c.createIndex(entityType.(content.Entity), false)
}
