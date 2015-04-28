package autocomplete

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

func ExampleAutocomplete_Index() {
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

	a := New(pool, "ac", TermsIndexing)

	docs := []doc{
		{
			DocID: "1",
			Name:  "Mercedes S500",
		},
		{
			DocID: "2",
			Name:  "Mercedes E250",
		},
		{
			DocID: "3",
			Name:  "Toyota Prius",
		},
	}

	for _, d := range docs {
		if err := a.Index("cars", d, 0); err != nil {
			log.Fatal(err)
		}
	}
}
