package search

import (
    "reflect"
)

type PonzuConfiguration interface {
    GetDataDirectory() string
}

type SearchConfiguration interface {
    GetContentTypes() map[string]func() interface{}
    GetDatabase() Database
    GetPonzuConfig() PonzuConfiguration
}

type Search interface {
	Update(id string, data interface{}) error
	Delete(id string) error
	Search(query string, count, offset int) ([]interface{}, error)
	SearchWithPagination(query string, count, offset int) ([]interface{}, int, error)
}

type SearchIndexable interface {
	IndexContent() bool
}

type SearchClient interface {
	CreateIndex(entityName string, entityType interface{}) error
	GetIndex(entityName string) (Search, error)
}

type CustomizableSearchAttributes interface {
	GetSearchableAttributes() map[string]reflect.Type
}

type Entity interface {
	EntityName() string
}

type Repository interface {
	FindOneById(id string) (interface{}, error)
}

type Persistable interface {
    GetRepositoryToken() string
}

type Database interface {
	GetRepositoryByToken(token string) Repository
	Close() error
}
