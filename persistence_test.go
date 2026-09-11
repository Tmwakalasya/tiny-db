package tinydb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestPersistentRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roundtrip.log")
	db := openForTest(t, path)
	want := map[string]string{
		"":                       "",
		"empty value":            "",
		"language":               "Go",
		"quotes\"\\\t\n\x00\xff": "value\"\\\t\r\n\x00\xfe",
		"unicode":                "Hello, 世界 🌍",
		"large":                  strings.Repeat("x", 128*1024),
	}
	for key, value := range want {
		putForTest(t, db, key, value)
	}
	putForTest(t, db, "language", "Python")
	want["language"] = "Python"
	putForTest(t, db, "removed", "old")
	if deleted, err := db.Delete("removed"); !deleted || err != nil {
		t.Fatalf("Delete(removed) = (%v, %v), want (true, nil)", deleted, err)
	}
	if deleted, err := db.Delete("missing"); deleted || err != nil {
		t.Fatalf("Delete(missing) = (%v, %v), want (false, nil)", deleted, err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	closeForTest(t, db)

	db = openForTest(t, path)
	checkValues(t, db, want)
	if _, ok := db.Get("removed"); ok {
		t.Fatal("deleted key reappeared after reopening")
	}
	if _, ok := db.Get("missing"); ok {
		t.Fatal("deleting a missing key created it")
	}
	// Reopened files must append after existing records, including later deletes.
	putForTest(t, db, "after reopen", "new value")
	want["after reopen"] = "new value"
	putForTest(t, db, "language", "C")
	want["language"] = "C"
	if deleted, err := db.Delete("empty value"); !deleted || err != nil {
		t.Fatalf("Delete(empty value) = (%v, %v), want (true, nil)", deleted, err)
	}
	delete(want, "empty value")
	closeForTest(t, db)

	db = openForTest(t, path)
	checkValues(t, db, want)
	if _, ok := db.Get("empty value"); ok {
		t.Fatal("key deleted after reopening reappeared")
	}
	closeForTest(t, db)
}

func TestLogFormatAndExistingLogReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "format.log")
	db := openForTest(t, path)
	putForTest(t, db, "hello", "world")
	putForTest(t, db, "tab\tkey", "line\n\"quoted\"")
	if deleted, err := db.Delete("hello"); !deleted || err != nil {
		t.Fatalf("Delete(hello) = (%v, %v), want (true, nil)", deleted, err)
	}
	closeForTest(t, db)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "tinydb-v1\nput\t\"hello\"\t\"world\"\nput\t\"tab\\tkey\"\t\"line\\n\\\"quoted\\\"\"\ndelete\t\"hello\"\n"
	if string(got) != want {
		t.Fatalf("log = %q, want %q", got, want)
	}

	// Replay also accepts a complete log supplied independently of Put/Delete.
	existingPath := filepath.Join(t.TempDir(), "existing.log")
	existing := "tinydb-v1\nput\t\"key\"\t\"first\"\nput\t\"key\"\t\"latest\"\nput\t\"removed\"\t\"value\"\ndelete\t\"removed\"\ndelete\t\"missing\"\n"
	if err := os.WriteFile(existingPath, []byte(existing), 0600); err != nil {
		t.Fatal(err)
	}
	db = openForTest(t, existingPath)
	checkValues(t, db, map[string]string{"key": "latest"})
	for _, key := range []string{"removed", "missing"} {
		if _, ok := db.Get(key); ok {
			t.Errorf("%q should be absent after replay", key)
		}
	}
	closeForTest(t, db)
}

func TestOpenRejectsCorruptLogsWithoutChangingBytes(t *testing.T) {
	for _, tc := range []struct {
		name string
		log  string
	}{
		{"wrong header", "tinydb-v2\n"},
		{"truncated header", "tinydb-v1"},
		{"missing header", "put\t\"key\"\t\"value\"\n"},
		{"unknown operation", "tinydb-v1\nupdate\t\"key\"\t\"value\"\n"},
		{"missing put value", "tinydb-v1\nput\t\"key\"\n"},
		{"extra put field", "tinydb-v1\nput\t\"key\"\t\"value\"\t\"extra\"\n"},
		{"unquoted key", "tinydb-v1\nput\tkey\t\"value\"\n"},
		{"invalid escape", "tinydb-v1\nput\t\"key\"\t\"\\q\"\n"},
		{"unclosed value", "tinydb-v1\nput\t\"key\"\t\"value\n"},
		{"missing delete key", "tinydb-v1\ndelete\n"},
		{"extra delete field", "tinydb-v1\ndelete\t\"key\"\t\"extra\"\n"},
		{"blank record", "tinydb-v1\n\n"},
		{"truncated final put", "tinydb-v1\nput\t\"key\"\t\"value\""},
		{"truncated final delete", "tinydb-v1\ndelete\t\"key\""},
		{"valid prefix then corrupt tail", "tinydb-v1\nput\t\"good\"\t\"value\"\nput\t\"bad"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "corrupt.log")
			original := []byte(tc.log)
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			db, err := Open(path)
			if db != nil {
				_ = db.Close()
				t.Error("Open returned a usable DB for a corrupt file")
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Errorf("Open error = %v, want ErrCorrupt", err)
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !bytes.Equal(after, original) {
				t.Errorf("corrupt file changed: got %q, want %q", after, original)
			}
		})
	}
}

func TestOpenMissingParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "db.log")
	if db, err := Open(path); err == nil {
		_ = db.Close()
		t.Fatal("Open with nonexistent parent succeeded")
	}
}

func TestCloseAndSync(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) *DB
	}{
		{"memory", func(t *testing.T) *DB { return New() }},
		{"zero value", func(t *testing.T) *DB { return &DB{} }},
		{"persistent", func(t *testing.T) *DB {
			return openForTest(t, filepath.Join(t.TempDir(), "lifecycle.log"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := tc.open(t)
			if err := db.Sync(); err != nil {
				t.Fatalf("initial Sync failed: %v", err)
			}
			putForTest(t, db, "key", "snapshot")
			closeForTest(t, db)
			closeForTest(t, db)
			if err := db.Put("key", "changed"); !errors.Is(err, ErrClosed) {
				t.Errorf("Put after Close = %v, want ErrClosed", err)
			}
			for _, key := range []string{"key", "missing"} {
				if deleted, err := db.Delete(key); deleted || !errors.Is(err, ErrClosed) {
					t.Errorf("Delete(%q) after Close = (%v, %v), want (false, ErrClosed)", key, deleted, err)
				}
			}
			if err := db.Sync(); !errors.Is(err, ErrClosed) {
				t.Errorf("Sync after Close = %v, want ErrClosed", err)
			}
			checkValues(t, db, map[string]string{"key": "snapshot"})
			if _, ok := db.Get("missing"); ok {
				t.Error("Get(missing) after Close found a key")
			}
		})
	}
}

func TestIOFailurePreservesMemoryAndStopsMutations(t *testing.T) {
	for _, operation := range []string{"Put", "Delete", "Sync"} {
		t.Run(operation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "failure.log")
			db := openForTest(t, path)
			putForTest(t, db, "key", "original")
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			// Force a real file error without relying on platform permissions,
			// disk exhaustion, timing, or a simulated hardware crash.
			if err := db.log.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			switch operation {
			case "Put":
				err = db.Put("key", "changed")
			case "Delete":
				var deleted bool
				deleted, err = db.Delete("key")
				if deleted {
					t.Error("failed Delete reported success")
				}
			case "Sync":
				err = db.Sync()
			}
			if err == nil {
				t.Fatalf("%s on a closed file succeeded", operation)
			}
			checkValues(t, db, map[string]string{"key": "original"})
			// Restore a usable descriptor: later writes must still fail because
			// the DB remembers the error, rather than retrying the append.
			db.log, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Put("later", "value"); err == nil {
				t.Error("Put after I/O failure succeeded")
			}
			if deleted, err := db.Delete("key"); deleted || err == nil {
				t.Errorf("Delete after I/O failure = (%v, %v), want (false, error)", deleted, err)
			}
			if err := db.Sync(); err == nil {
				t.Error("Sync did not report the earlier I/O failure")
			}
			if _, ok := db.Get("later"); ok {
				t.Error("failed Put changed the in-memory snapshot")
			}
			checkValues(t, db, map[string]string{"key": "original"})
			_ = db.Close() // Closing can report the earlier I/O error.
			reopened := openForTest(t, path)
			checkValues(t, reopened, map[string]string{"key": "original"})
			if _, ok := reopened.Get("later"); ok {
				t.Error("failed Put appeared on disk")
			}
			closeForTest(t, reopened)
		})
	}
}

func TestPersistentConcurrentAccessAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.log")
	db := openForTest(t, path)
	var workers sync.WaitGroup
	start := make(chan struct{})
	const workerCount = 8
	for id := 0; id < workerCount; id++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			key := "worker:" + strconv.Itoa(id)
			<-start
			for iteration := 0; iteration < 20; iteration++ {
				if err := db.Put(key, "temporary"); err != nil {
					t.Errorf("worker %d Put: %v", id, err)
					return
				}
				if value, ok := db.Get(key); !ok || value != "temporary" {
					t.Errorf("worker %d Get = (%q, %v)", id, value, ok)
					return
				}
				if deleted, err := db.Delete(key); !deleted || err != nil {
					t.Errorf("worker %d Delete = (%v, %v)", id, deleted, err)
					return
				}
			}
			if err := db.Put(key, "final"); err != nil {
				t.Errorf("worker %d final Put: %v", id, err)
			}
		}(id)
	}
	close(start)
	workers.Wait()
	closeForTest(t, db)
	db = openForTest(t, path)
	for id := 0; id < workerCount; id++ {
		key := "worker:" + strconv.Itoa(id)
		checkValues(t, db, map[string]string{key: "final"})
	}
	closeForTest(t, db)
}

func openForTest(t *testing.T, path string) *DB {
	t.Helper()
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func closeForTest(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func checkValues(t *testing.T, db *DB, want map[string]string) {
	t.Helper()
	for key, expected := range want {
		if value, ok := db.Get(key); !ok || value != expected {
			t.Errorf("Get(%q): present=%v, value length=%d; want present=true and exact value of length %d", key, ok, len(value), len(expected))
		}
	}
}
