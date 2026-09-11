package tinydb

import (
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	db := New()
	if value, ok := db.Get("missing"); ok || value != "" {
		t.Fatalf("Get(missing) = (%q, %v), want (empty, false)", value, ok)
	}

	putForTest(t, db, "language", "Go")
	if value, ok := db.Get("language"); !ok || value != "Go" {
		t.Fatalf("Get(language) = (%q, %v), want (Go, true)", value, ok)
	}

	putForTest(t, db, "language", "Python")
	if value, ok := db.Get("language"); !ok || value != "Python" {
		t.Fatalf("Get(language) after overwrite = (%q, %v), want (Python, true)", value, ok)
	}
}

func TestEmptyKeyAndValue(t *testing.T) {
	db := New()
	putForTest(t, db, "", "empty key")
	putForTest(t, db, "empty value", "")

	if value, ok := db.Get(""); !ok || value != "empty key" {
		t.Fatalf("Get(empty key) = (%q, %v), want (empty key, true)", value, ok)
	}
	if value, ok := db.Get("empty value"); !ok || value != "" {
		t.Fatalf("Get(empty value) = (%q, %v), want (empty, true)", value, ok)
	}
	if value, ok := db.Get("missing"); ok || value != "" {
		t.Fatalf("Get(missing) = (%q, %v), want (empty, false)", value, ok)
	}
}

func TestDelete(t *testing.T) {
	db := New()
	if deleted, err := db.Delete("missing"); deleted || err != nil {
		t.Fatalf("Delete(missing) = (%v, %v), want (false, nil)", deleted, err)
	}

	putForTest(t, db, "remove", "")
	putForTest(t, db, "keep", "value")
	if deleted, err := db.Delete("remove"); !deleted || err != nil {
		t.Fatalf("Delete(existing key with empty value) = (%v, %v), want (true, nil)", deleted, err)
	}
	if value, ok := db.Get("remove"); ok || value != "" {
		t.Fatalf("Get(deleted key) = (%q, %v), want (empty, false)", value, ok)
	}
	if deleted, err := db.Delete("remove"); deleted || err != nil {
		t.Fatalf("Delete(already deleted key) = (%v, %v), want (false, nil)", deleted, err)
	}
	if value, ok := db.Get("keep"); !ok || value != "value" {
		t.Fatalf("Get(other key) = (%q, %v), want (value, true)", value, ok)
	}
}

func TestZeroValue(t *testing.T) {
	var db DB
	if value, ok := db.Get("key"); ok || value != "" {
		t.Fatalf("zero-value Get(key) = (%q, %v), want (empty, false)", value, ok)
	}
	if deleted, err := db.Delete("key"); deleted || err != nil {
		t.Fatalf("zero-value Delete(key) = (%v, %v), want (false, nil)", deleted, err)
	}
	putForTest(t, &db, "key", "value")
	if value, ok := db.Get("key"); !ok || value != "value" {
		t.Fatalf("Get(key) after first Put = (%q, %v), want (value, true)", value, ok)
	}
	if deleted, err := db.Delete("key"); !deleted || err != nil {
		t.Fatalf("Delete(key) after first Put = (%v, %v), want (true, nil)", deleted, err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	// Each worker owns its key, making the results deterministic while operations
	// on the shared map overlap. Run with go test -race to check synchronization.
	var db DB
	var workers sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			key := strconv.Itoa(id)
			<-start
			for iteration := 0; iteration < 100; iteration++ {
				if err := db.Put(key, "value"); err != nil {
					t.Errorf("worker %d: Put failed: %v", id, err)
					return
				}
				if value, ok := db.Get(key); !ok || value != "value" {
					t.Errorf("worker %d: Get = (%q, %v), want (value, true)", id, value, ok)
					return
				}
				if deleted, err := db.Delete(key); !deleted || err != nil {
					t.Errorf("worker %d: Delete = (%v, %v), want (true, nil)", id, deleted, err)
					return
				}
				if _, ok := db.Get(key); ok {
					t.Errorf("worker %d: deleted key still present", id)
					return
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
}

// Both benchmarks cycle through a fixed, preloaded set of 10,000 keys and
// 128-byte string values. Key creation and loading are outside the timed loop.
// Put overwrites existing keys, so memory use stays bounded across long runs.
const benchmarkKeyCount = 10_000

func benchmarkData(b *testing.B) (*DB, []string, string) {
	b.Helper()
	db := New()
	keys := make([]string, benchmarkKeyCount)
	value := strings.Repeat("v", 128)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
		putForTest(b, db, keys[i], value)
	}
	return db, keys, value
}

var benchmarkValue string
var benchmarkFound bool

func BenchmarkGet_10KKeys_128BValues(b *testing.B) {
	db, keys, _ := benchmarkData(b)
	var value string
	var found bool
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, found = db.Get(keys[i%len(keys)])
	}
	b.StopTimer()
	benchmarkValue, benchmarkFound = value, found
}

func BenchmarkPutOverwrite_10KKeys_128BValues(b *testing.B) {
	db, keys, value := benchmarkData(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(keys[i%len(keys)], value); err != nil {
			b.Fatal(err)
		}
	}
}

func putForTest(t testing.TB, db *DB, key, value string) {
	t.Helper()
	if err := db.Put(key, value); err != nil {
		t.Fatalf("Put(%q) failed: %v", key, err)
	}
}
