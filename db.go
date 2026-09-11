// Package tinydb is an educational key-value database with optional persistence.
package tinydb

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// ErrClosed is returned when a closed database is asked to write or sync.
var ErrClosed = errors.New("tinydb: database is closed")

// DB stores string keys and values in memory, optionally backed by a log file.
// Its zero value is ready to use as an in-memory database.
// A DB can be shared by goroutines, but must not be copied after first use.
type DB struct {
	mu       sync.RWMutex
	data     map[string]string
	log      *os.File
	closed   bool
	writeErr error
	closeErr error
}

// New returns an empty in-memory database. Use Open to store changes in a file.
func New() *DB {
	return &DB{data: make(map[string]string)}
}

// Put inserts a key or replaces its existing value.
// A persistent DB appends to its log before updating memory. Call Sync or Close
// to request that the OS flush the log to storage.
func (db *DB) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.writable(); err != nil {
		return err
	}
	if err := db.appendRecord("put", key, value); err != nil {
		return err
	}

	// Initialize lazily so that "var db DB" also works.
	if db.data == nil {
		db.data = make(map[string]string)
	}
	db.data[key] = value
	return nil
}

// Get returns the value and whether the key exists.
// The boolean distinguishes a missing key from a stored empty string.
func (db *DB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	value, found := db.data[key]
	return value, found
}

// Delete removes a key and reports whether it existed.
// An error leaves the in-memory value unchanged.
func (db *DB) Delete(key string) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.writable(); err != nil {
		return false, err
	}
	_, found := db.data[key]
	if !found {
		return false, nil
	}
	if err := db.appendRecord("delete", key, ""); err != nil {
		return false, err
	}
	delete(db.data, key)
	return true, nil
}

// Sync asks the OS to flush the log to storage. It is a no-op for an open
// in-memory DB. A failed flush stops later mutations until the DB is reopened.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.writable(); err != nil {
		return err
	}
	if db.log == nil {
		return nil
	}
	if err := db.log.Sync(); err != nil {
		db.writeErr = fmt.Errorf("tinydb: sync log: %w", err)
		return db.writeErr
	}
	return nil
}

// Close flushes and closes the log and prevents further mutations.
// Get can still read the final in-memory snapshot. Repeated calls return the
// same result. File errors must be checked even when all earlier writes worked.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return db.closeErr
	}
	db.closed = true
	if db.log != nil {
		syncErr := db.log.Sync()
		closeErr := db.log.Close()
		db.closeErr = errors.Join(db.writeErr, syncErr, closeErr)
		db.log = nil
	}
	return db.closeErr
}

// writable is called with the write lock held. Continuing after a partial file
// write could append valid records behind damaged data, so file errors stop writes.
func (db *DB) writable() error {
	if db.closed {
		return ErrClosed
	}
	if db.writeErr != nil {
		return fmt.Errorf("tinydb: writes stopped; close and inspect the log before reopening: %w", db.writeErr)
	}
	return nil
}
