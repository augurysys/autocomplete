// Package autocomplete provides a library for building auto complete services
// with a Redis backend.
//
// it uses http://github.com/garyburd/redigo/redis as it's Redis driver.
//
// it borrows ideas from:
//
// 	http://oldblog.antirez.com/post/autocomplete-with-redis.html
//
// 	http://patshaughnessy.net/2011/11/29/two-ways-of-using-redis-to-build-a-nosql-autocomplete-search-index
//
// 	http://engineering.getglue.com/post/36667374830/autocomplete-search-with-redis
//
// the implementation is using transactions and LUA scripts for optimizations.
package autocomplete

import (
	"errors"

	"github.com/garyburd/redigo/redis"
)

const (
	// PrefixesIndexing index the combination of each substring for every word
	// in a new ZSET and use ZRANGE/ZRANGEBYSCORE for querying.
	//
	// it also does intersections between multiple words in a search term.
	//
	// Redis dataset and queries complexity:
	// memory complexity - O(N*M) with N being the number of distinct words
	// across all search terms and M the word's length.
	//
	// time complexity - O(log(N)+N) with N being the number of terms with the
	// same prefix.
	PrefixesIndexing = 0

	// TermsIndexing index entire terms in a single ZSET and use ZRANGEBYLEX
	// for querying.
	//
	// Redis dataset and queries complextity:
	// memory complexity - O(N) with N being the number of search terms.
	//
	// time complexity - O(log(N)+N) with N being the number of indexed terms.
	TermsIndexing = 1
)

// Error objects
var (
	ErrInvalidIndexType = errors.New("invalid index type")
)

// Autocomplete service
type Autocomplete struct {
	pool      *redis.Pool
	prefix    string
	indexType int
}

// New returns a pointer to a new Autocomplete service
func New(pool *redis.Pool, prefix string, indexType int) *Autocomplete {
	return &Autocomplete{
		pool:      pool,
		prefix:    prefix,
		indexType: indexType,
	}
}
