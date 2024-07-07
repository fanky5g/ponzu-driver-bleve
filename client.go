package search

import (
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/mapping"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func (c *client) GetIndex(entityName string) (Search, error) {
	if index, ok := c.indexes[entityName]; ok {
		return index, nil
	}

	return nil, fmt.Errorf("index for %s not implemented", entityName)
}

func (c *client) CreateIndex(entityName string, entityType interface{}) error {
	return c.createIndex(entityType.(Entity), false)
}

func (c *client) repository(entityType string) Repository {
	repository := c.database.GetRepositoryByToken(entityType)
	if repository == nil {
		log.Panicf("Failed to get repository for: %v", entityType)
	}

	return repository.(Repository)
}

func (c *client) getExistingIndex(indexPath string, failOnMissingIndex bool) (Search, error) {
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
	var searchIndex Search
	searchIndex, err = NewSearchIndex(entity, index, c.repository(entityName))
	if err != nil {
		return nil, fmt.Errorf("failed to create search index: %v", err)
	}

	return searchIndex, nil
}

func (c *client) createIndex(entity Entity, overwrite bool) error {
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

func (c *client) persistIndex(indexPath string, mapping *mapping.IndexMappingImpl) (bleve.Index, error) {
	mapping.TypeField = TypeField
	_, err := os.Stat(indexPath)
	if err == nil {
		if err = os.RemoveAll(indexPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing index: %v", err)
		}
	}

	return bleve.New(indexPath, mapping)
}

func searchable(entity interface{}) bool {
	indexableInterface, ok := entity.(SearchIndexable)
	if ok {
		return indexableInterface.IndexContent()
	}

	return true
}
