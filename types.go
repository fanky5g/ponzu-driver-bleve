package search

import (
	"reflect"

	"github.com/blevesearch/bleve/search"
)

type CustomizableSearchAttributes interface {
	GetSearchableAttributes() map[string]reflect.Type
}

type Entity interface {
	EntityName() string
}

type Hit struct {
    doc  *search.DocumentMatch
}

func (h *Hit) GetID() string {
    return h.doc.ID
}
