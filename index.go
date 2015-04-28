package autocomplete

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/garyburd/redigo/redis"
)

// Index indexes a document for autocomplete search
func (a *Autocomplete) Index(index string, d Document, score uint64) error {
	conn := a.pool.Get()
	defer conn.Close()

	docKey := key(d)
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	if err := conn.Send("MULTI"); err != nil {
		return err
	}

	switch a.indexType {
	case PrefixesIndexing:
		for _, p := range prefixes(d) {
			if err := conn.Send("ZADD", a.prefix+":"+index+":"+p,
				score, docKey); err != nil {

				return err
			}
		}

	case TermsIndexing:
		scoreStr, err := scoreString(score)
		if err != nil {
			return err
		}

		val := strings.ToLower(d.Term()) + "::" + scoreStr + "::" + docKey
		if err := conn.Send("ZADD", a.prefix+":$$"+index, 0, val); err != nil {
			return err
		}

	default:
		return ErrInvalidIndexType
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

	docKey := key(d)

	switch a.indexType {
	case PrefixesIndexing:
		for _, p := range prefixes(d) {
			if err := conn.Send("MULTI"); err != nil {
				return err
			}

			if err := conn.Send(
				"ZREM", a.prefix+":"+index+":"+p, docKey); err != nil {

				return err
			}
		}

	case TermsIndexing:
		script, ok := a.scripts["removeDocument"]
		if !ok {
			return fmt.Errorf("initialization error")
		}

		zmember, err := redis.String(script.Do(conn, a.prefix+":$$"+index, docKey))
		if err != nil {
			return err
		}

		if err := conn.Send("MULTI"); err != nil {
			return err
		}

		if err := conn.Send(
			"ZREM", a.prefix+":$$"+index, zmember); err != nil {

			return err
		}

	default:
		return ErrInvalidIndexType
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

// UpdateDocument updates a document in the autocomplete search index,
// only the document's data can be updated because the key is generated from
// a combination of the document id and name.
//
// if one of those is changed, the document should be removed and re-indexed
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

// UpdateScore updates the score of a document
func (a *Autocomplete) UpdateScore(index string, d Document, score uint64) error {
	conn := a.pool.Get()
	defer conn.Close()

	docKey := key(d)

	switch a.indexType {
	case PrefixesIndexing:
		if err := conn.Send("MULTI"); err != nil {
			return err
		}

		for _, p := range prefixes(d) {
			if err := conn.Send("ZADD", a.prefix+":"+index+":"+p,
				score, docKey); err != nil {

				return err
			}
		}

		if _, err := conn.Do("EXEC"); err != nil {
			return err
		}

	case TermsIndexing:
		script, ok := a.scripts["updateScore"]
		if !ok {
			return fmt.Errorf("initialization error")
		}

		scoreStr, err := scoreString(score)
		if err != nil {
			return err
		}

		val := strings.ToLower(d.Term()) + "::" + scoreStr + "::" + docKey
		if _, err := script.Do(conn, a.prefix+":$$"+index, docKey, val); err != nil {
			return err
		}

	default:
		return ErrInvalidIndexType
	}

	return nil
}

func scoreString(score uint64) (string, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, score); err != nil {
		return "", err
	}

	return base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}
