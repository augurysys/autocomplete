package autocomplete

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

func ExampleIndex() {
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

	docs := []doc{
		{
			id:   "1",
			term: "Mercedes S500",
		},
		{
			id:   "2",
			term: "Mercedes E250",
		},
		{
			id:   "3",
			term: "Toyota Prius",
		},
	}

	for _, d := range docs {
		if err := a.Index("cars", d); err != nil {
			log.Fatal(err)
		}
	}
}
