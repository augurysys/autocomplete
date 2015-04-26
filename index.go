package autocomplete

import (
	"encoding/json"
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// Index indexes a document for autocomplete search
func (a *Autocomplete) Index(index string, d Document) error {
	conn := a.pool.Get()
	defer conn.Close()

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	if err := conn.Send("MULTI"); err != nil {
		return err
	}

	docKey := key(d)
	for _, p := range prefixes(d) {
		if err := conn.Send("ZADD", a.prefix+":"+index+":"+p,
			0, docKey); err != nil {

			return err
		}
	}

	if err := conn.Send(
		"HSET", a.prefix+":$"+index, docKey, string(b)); err != nil {

		return err
	}

	if _, err := conn.Do("EXEC"); err != nil {
		return err
	}

	return nil
}

// RemoveDocument removes a document from the autocomplete search index
func (a *Autocomplete) RemoveDocument(index string, d Document) error {
	conn := a.pool.Get()
	defer conn.Close()

	if err := conn.Send("MULTI"); err != nil {
		return err
	}

	docKey := key(d)
	for _, p := range prefixes(d) {
		if err := conn.Send(
			"ZREM", a.prefix+":"+index+":"+p, docKey); err != nil {

			return err
		}
	}

	if err := conn.Send(
		"HDEL", a.prefix+":"+"$"+index, docKey); err != nil {
		return err
	}

	if _, err := conn.Do("EXEC"); err != nil {
		return err
	}

	return nil
}

// UpdateDocument updates a document in the autocomplete search index
func (a *Autocomplete) UpdateDocument(index string, d Document) error {
	conn := a.pool.Get()
	defer conn.Close()

	docKey := key(d)

	exists, err := redis.Bool(conn.Do("HEXISTS", a.prefix+":$"+index, docKey))
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%s does not contain %s", a.prefix+":$"+index, docKey)
	}

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	if _, err := conn.Do(
		"HSET", a.prefix+":$"+index, docKey, string(b)); err != nil {

		return err
	}

	return nil
}
