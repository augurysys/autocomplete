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
// for implementing the auto-complete functionality and the implementation
// is using transactions and LUA scripts for optimizations.
package autocomplete

import (
	"github.com/garyburd/redigo/redis"
)

// Autocomplete service
type Autocomplete struct {
	pool   *redis.Pool
	prefix string
}

// New returns a pointer to a new Autocomplete service
func New(pool *redis.Pool, prefix string) *Autocomplete {
	return &Autocomplete{
		pool:   pool,
		prefix: prefix,
	}
}
