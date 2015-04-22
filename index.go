package autocomplete

import (
	"encoding/json"
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
