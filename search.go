package search

import (
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/fanky5g/ponzu/config"
	ponzuContent "github.com/fanky5g/ponzu/content"
	"github.com/fanky5g/ponzu/driver"
	"github.com/fanky5g/ponzu/entities"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var TypeField = "type"
var IndexSuffix = ".index"

type client struct {
	searchPath   string
	indexes      map[string]driver.SearchInterface
	repositories map[string]driver.Repository
	entities     map[string]ponzuContent.Entity
}

func searchable(entity interface{}) bool {
	indexableInterface, ok := entity.(entities.SearchIndexable)
	if ok {
		return indexableInterface.IndexContent()
	}

	return true
}

func (c *client) repository(entityType string) driver.Repository {
	repository := c.repositories[entityType]
	if repository == nil {
		log.Panicf("Failed to get repository for: %v", entityType)
	}

	return repository.(driver.Repository)
}

func (c *client) GetIndex(entityName string) (driver.SearchInterface, error) {
	if index, ok := c.indexes[entityName]; ok {
		return index, nil
	}

	return nil, fmt.Errorf("index for %s not implemented", entityName)
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

func New(
	contentTypes map[string]ponzuContent.Builder,
	database driver.Database,
) (driver.SearchClientInterface, error) {
	cfg, err := config.New()
	if err != nil {
		return nil, fmt.Errorf("failed to get config: %v", err)
	}

	searchPath := filepath.Join(cfg.Paths.DataDir, "search")

	if err = os.MkdirAll(searchPath, os.ModeDir|os.ModePerm); err != nil {
		return nil, err
	}

	repos := make(map[string]driver.Repository)
	contentEntities := make(map[string]ponzuContent.Entity)
	for entityName, entityConstructor := range contentTypes {
		entity := entityConstructor()
		persistable, ok := entity.(entities.Persistable)
		if !ok {
			return nil, fmt.Errorf("entity %s does not implement Persistable", entityName)
		}

		repository := database.GetRepositoryByToken(persistable.GetRepositoryToken())
		if repository == nil {
			return nil, fmt.Errorf("content repository for %s not implemented", entityName)
		}

		repos[entityName] = repository.(driver.Repository)
		contentEntities[entityName] = entity.(ponzuContent.Entity)
	}

	c := &client{
		indexes:      make(map[string]driver.SearchInterface),
		searchPath:   searchPath,
		repositories: repos,
		entities:     contentEntities,
	}

	if contentTypes != nil {
		// Load existing types
		searchDirItems, err := os.ReadDir(searchPath)
		if err != nil {
			return nil, err
		}

		if len(searchDirItems) > 0 {
			for _, searchDirItem := range searchDirItems {
				if searchDirItem.IsDir() {
					entityName := strings.TrimSuffix(searchDirItem.Name(), IndexSuffix)
					if _, ok := contentTypes[entityName]; ok {
						searchIndex, err := c.getExistingIndex(path.Join(searchPath, searchDirItem.Name()), false)
						if err != nil {
							return nil, err
						}

						if searchIndex != nil {
							log.Printf("Search index %s initialized\n", entityName)
							c.indexes[entityName] = searchIndex
						}
					}

				}
			}
		}
	}

	return c, nil
}
