package autocomplete

import (
	"encoding/json"
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

func ExampleSearch() {
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

	a := New(pool, "ac")

	results, err := a.Search("cars", "mer")
	if err != nil {
		log.Fatal(err)
	}

	var docs []doc
	for _, b := range results {
		var d doc
		if err := json.Unmarshal(b, &d); err != nil {
			log.Fatal(err)
		}

		docs = append(docs, d)
	}
}
