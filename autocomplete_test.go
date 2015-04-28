package autocomplete

import (
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

func ExampleNew() {
	pool := &redis.Pool{
		MaxIdle:     3,
		MaxActive:   20,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "localhost:6379")
			if err != nil {
				return nil, err
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	defer pool.Close()

	New(pool, "ac", PrefixesIndexing)
}

func TestNew(t *testing.T) {
	p := &redis.Pool{}
	prefix := "test_prefix"

	s := New(p, prefix, PrefixesIndexing)

	if s.pool != p || s.prefix != prefix || s.indexType != PrefixesIndexing {
		t.Fail()
	}
}
