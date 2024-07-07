package search

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var TypeField = "type"
var IndexSuffix = ".index"

type client struct {
	searchPath string
	indexes    map[string]Search
	database   Database
	entities   map[string]Entity
}

func New(config SearchConfiguration) (SearchClient, error) {
	var err error
	contentTypes := config.GetContentTypes()
	database := config.GetDatabase()

	searchPath := filepath.Join(config.GetPonzuConfig().GetDataDirectory(), "search")
	if err = os.MkdirAll(searchPath, os.ModeDir|os.ModePerm); err != nil {
		return nil, err
	}

	repos := make(map[string]Repository)
	contentEntities := make(map[string]Entity)
	for entityName, entityConstructor := range contentTypes {
		entity := entityConstructor()
		persistable, ok := entity.(Persistable)
		if !ok {
			return nil, fmt.Errorf("entity %s does not implement Persistable", entityName)
		}

		repository := database.GetRepositoryByToken(persistable.GetRepositoryToken())
		if repository == nil {
			return nil, fmt.Errorf("content repository for %s not implemented", entityName)
		}

		repos[entityName] = repository.(Repository)
		contentEntities[entityName] = entity.(Entity)
	}

	c := &client{
		indexes:    make(map[string]Search),
		searchPath: searchPath,
		database:   database,
		entities:   contentEntities,
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
