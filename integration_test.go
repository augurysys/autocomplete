// +build integration

package autocomplete

import (
	"encoding/json"
	"flag"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

var pool *redis.Pool
var autocomplete *Autocomplete

var redisURL string
var redisPassword string
var prefix string

func init() {
	url := flag.String("redis_url", "localhost:6379", "Redis URL")
	password := flag.String("redis_password", "", "Redis password")
	prx := flag.String("prefix", "ac", "Prefix of Redis keys")

	flag.Parse()

	prefix = *prx
	redisURL = *url
	redisPassword = *password
}

type TestStruct interface {
	Fatal(args ...interface{})
}

func flushall(t TestStruct) {
	conn := pool.Get()
	defer conn.Close()

	if _, err := conn.Do("FLUSHALL"); err != nil {
		t.Fatal(err)
	}
}

func setUp(t TestStruct, indexType int) {
	pool = &redis.Pool{
		MaxIdle:     3,
		MaxActive:   10,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisURL)
			if err != nil {
				return nil, err
			}

			if redisPassword != "" {
				if _, err := c.Do("AUTH", redisPassword); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	flushall(t)

	autocomplete = New(pool, prefix, indexType)
}

func tearDown(t TestStruct) {
	flushall(t)

	pool.Close()
	autocomplete = nil
}

func TestIndexAndSearchPrefixesIndexing(t *testing.T) {
	setUp(t, PrefixesIndexing)
	defer tearDown(t)

	d1 := doc{
		DocID: "123",
		Name:  "Test SEARCH term!",
	}

	d2 := doc{
		DocID: "345",
		Name:  "Another search TERM",
	}

	if err := autocomplete.Index("test_index", d1, 100); err != nil {
		t.Fatal(err)
	}

	if err := autocomplete.Index("test_index", d2, 200); err != nil {
		t.Fatal(err)
	}

	results, err := autocomplete.Search("test_index", "x", SortLexicographical)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Fail()
	}

	// search with lexicographical order
	results, err = autocomplete.Search("test_index", "se", SortLexicographical)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fail()
	}

	docs := []doc{}
	for _, r := range results {
		var d doc
		if err := json.Unmarshal(r, &d); err != nil {
			t.Fatal(err)
		}

		docs = append(docs, d)
	}

	if !reflect.DeepEqual(docs[0], d2) {
		t.Fail()
	}

	if !reflect.DeepEqual(docs[1], d1) {
		t.Fail()
	}

	// search with reverse lexicographical order
	results, err = autocomplete.Search("test_index", "se", SortRevLexicographical)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fail()
	}

	docs = []doc{}
	for _, r := range results {
		var d doc
		if err := json.Unmarshal(r, &d); err != nil {
			t.Fatal(err)
		}

		docs = append(docs, d)
	}

	if !reflect.DeepEqual(docs[0], d1) {
		t.Fail()
	}

	if !reflect.DeepEqual(docs[1], d2) {
		t.Fail()
	}

	// search with score order
	results, err = autocomplete.Search("test_index", "se", SortScore)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fail()
	}

	docs = []doc{}
	for _, r := range results {
		var d doc
		if err := json.Unmarshal(r, &d); err != nil {
			t.Fatal(err)
		}

		docs = append(docs, d)
	}

	if !reflect.DeepEqual(docs[0], d1) {
		t.Fail()
	}

	if !reflect.DeepEqual(docs[1], d2) {
		t.Fail()
	}

	// search with reverse score order
	results, err = autocomplete.Search("test_index", "se", SortRevScore)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fail()
	}

	docs = []doc{}
	for _, r := range results {
		var d doc
		if err := json.Unmarshal(r, &d); err != nil {
			t.Fatal(err)
		}

		docs = append(docs, d)
	}

	if !reflect.DeepEqual(docs[0], d2) {
		t.Fail()
	}

	if !reflect.DeepEqual(docs[1], d1) {
		t.Fail()
	}

	// multi term search
	results, err = autocomplete.Search("test_index", "se term!", SortRevScore)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fail()
	}

	var d doc
	if err := json.Unmarshal(results[0], &d); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(d, d1) {
		t.Fail()
	}
}

func TestIndexAndSearchTermsIndexing(t *testing.T) {
	setUp(t, TermsIndexing)
	defer tearDown(t)

	d1 := doc{
		DocID: "123",
		Name:  "Test SEARCH term!",
	}

	d2 := doc{
		DocID: "345",
		Name:  "Test another SEARCH term 2",
	}

	if err := autocomplete.Index("test_index", d1, 100); err != nil {
		t.Fatal(err)
	}

	if err := autocomplete.Index("test_index", d2, 200); err != nil {
		t.Fatal(err)
	}

	results, err := autocomplete.Search("test_index", "x", SortLexicographical)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Fail()
	}

	// search with lexicographical order
	results, err = autocomplete.Search("test_index", "test", SortLexicographical)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fail()
	}

	docs := []doc{}
	for _, r := range results {
		var d doc
		if err := json.Unmarshal(r, &d); err != nil {
			t.Fatal(err)
		}

		docs = append(docs, d)
	}

	if !reflect.DeepEqual(docs[0], d2) {
		t.Fail()
	}

	if !reflect.DeepEqual(docs[1], d1) {
		t.Fail()
	}

	// search with reverse lexicographical order
	results, err = autocomplete.Search("test_index", "test", SortRevLexicographical)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fail()
	}

	docs = []doc{}
	for _, r := range results {
		var d doc
		if err := json.Unmarshal(r, &d); err != nil {
			t.Fatal(err)
		}

		docs = append(docs, d)
	}

	if !reflect.DeepEqual(docs[0], d1) {
		t.Fail()
	}

	if !reflect.DeepEqual(docs[1], d2) {
		t.Fail()
	}

	// search with score order
	results, err = autocomplete.Search("test_index", "test", SortScore)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fail()
	}

	docs = []doc{}
	for _, r := range results {
		var d doc
		if err := json.Unmarshal(r, &d); err != nil {
			t.Fatal(err)
		}

		docs = append(docs, d)
	}

	if !reflect.DeepEqual(docs[0], d1) {
		t.Fail()
	}

	if !reflect.DeepEqual(docs[1], d2) {
		t.Fail()
	}

	// search with reverse score order
	results, err = autocomplete.Search("test_index", "test", SortRevScore)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fail()
	}

	docs = []doc{}
	for _, r := range results {
		var d doc
		if err := json.Unmarshal(r, &d); err != nil {
			t.Fatal(err)
		}

		docs = append(docs, d)
	}

	if !reflect.DeepEqual(docs[0], d2) {
		t.Fail()
	}

	if !reflect.DeepEqual(docs[1], d1) {
		t.Fail()
	}
}

func BenchmarkIndexPrefixesIndexing(b *testing.B) {
	b.StopTimer()
	setUp(b, PrefixesIndexing)
	defer tearDown(b)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := strconv.Itoa(i)

		d := doc{
			DocID: s,
			Name:  s + " " + "test_string" + s,
		}

		b.StartTimer()
		if err := autocomplete.Index("test_index", d, 100); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func BenchmarkIndexTermsIndexing(b *testing.B) {
	b.StopTimer()
	setUp(b, TermsIndexing)
	defer tearDown(b)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := strconv.Itoa(i)
		d := doc{
			DocID: s,
			Name:  s + " " + "test_string" + s,
		}

		b.StartTimer()
		if err := autocomplete.Index("test_index", d, 100); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func BenchmarkSearchPrefixesIndexingLexicographicalSort(b *testing.B) {
	b.StopTimer()
	setUp(b, PrefixesIndexing)
	defer tearDown(b)

	for i := 0; i < 100000; i++ {
		s := strconv.Itoa(i)

		d := doc{
			DocID: s,
			Name:  s + " " + "test_string" + s,
		}

		if err := autocomplete.Index("test_index", d, 100); err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := strconv.Itoa(i)

		b.StartTimer()
		if _, err := autocomplete.Search("test_index",
			s+" t", SortLexicographical); err != nil {

			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func BenchmarkSearchPrefixesIndexingScoreSort(b *testing.B) {
	b.StopTimer()
	setUp(b, PrefixesIndexing)
	defer tearDown(b)

	for i := 0; i < 100000; i++ {
		s := strconv.Itoa(i)

		d := doc{
			DocID: s,
			Name:  s + " " + "test_string" + s,
		}

		if err := autocomplete.Index("test_index", d, 100); err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := strconv.Itoa(i)

		b.StartTimer()
		if _, err := autocomplete.Search("test_index",
			s+" t", SortScore); err != nil {

			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func BenchmarkSearchTermsIndexingLexicographicalSort(b *testing.B) {
	b.StopTimer()
	setUp(b, TermsIndexing)
	defer tearDown(b)

	for i := 0; i < 100000; i++ {
		s := strconv.Itoa(i)
		d := doc{
			DocID: s,
			Name:  s + " " + "test_string" + s,
		}

		if err := autocomplete.Index("test_index", d, 100); err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := strconv.Itoa(i)

		b.StartTimer()
		if _, err := autocomplete.Search("test_index",
			s, SortLexicographical); err != nil {

			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func BenchmarkSearchTermsIndexingScoreSort(b *testing.B) {
	b.StopTimer()
	setUp(b, TermsIndexing)
	defer tearDown(b)

	for i := 0; i < 100000; i++ {
		s := strconv.Itoa(i)

		d := doc{
			DocID: s,
			Name:  s + " " + "test_string" + s,
		}

		if err := autocomplete.Index("test_index", d, 100); err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := strconv.Itoa(i)

		b.StartTimer()
		if _, err := autocomplete.Search("test_index",
			s, SortScore); err != nil {

			b.Fatal(err)
		}
	}

	b.StopTimer()
}
