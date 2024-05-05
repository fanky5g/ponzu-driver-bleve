package search

import (
	"errors"
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/fanky5g/ponzu/config"
	ponzuContent "github.com/fanky5g/ponzu/content"
	"github.com/fanky5g/ponzu/content/item"
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
	indexes      map[string]driver.SearchIndexInterface
	repositories map[string]driver.Repository
}

func (c *client) repository(entityType string) driver.Repository {
	repository := c.repositories[entityType]
	if repository == nil {
		log.Panicf("Failed to get repository for: %v", entityType)
	}

	return repository.(driver.Repository)
}

// UpdateIndex TODO: only call when an item structure updates (via manual command)
func (c *client) UpdateIndex(entityName string, entityType interface{}) error {
	if err := c.createIndex(entityName, entityType, true); err != nil {
		return err
	}

	searchIndex, err := c.GetIndex(entityName)
	if err != nil {
		return err
	}

	if searchIndex == nil {
		return errors.New("failed to update index")
	}

	go func() {
		entities, err := c.repository(entityName).FindAll()
		if err != nil {
			log.Fatalf("Failed to re-index namespace: %s. Error: %v", entityName, err)
		}

		for _, entity := range entities {
			if err = searchIndex.Update(entity.(item.Identifiable).ItemID(), entity); err != nil {
				log.Fatalf("Failed to index entity: %v", entity)
			}
		}
	}()

	return nil
}

func (c *client) Indexes() (map[string]driver.SearchIndexInterface, error) {
	return c.indexes, nil
}

func (c *client) GetIndex(entityName string) (driver.SearchIndexInterface, error) {
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
	}

	c := &client{
		indexes:      make(map[string]driver.SearchIndexInterface),
		searchPath:   searchPath,
		repositories: repos,
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
