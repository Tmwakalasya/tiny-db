package tinydb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const logHeader = "tinydb-v1\n"

// ErrCorrupt means the log is malformed or ends with an incomplete record.
// Open reports the error without changing the file; automatic repair is a later
// milestone, so the original bytes remain available for inspection.
var ErrCorrupt = errors.New("tinydb: corrupt log")

// Open creates a log file or rebuilds the in-memory map from an existing log.
// The parent directory must already exist. Only one DB handle/process may use
// a file at a time; this first format does not yet implement file locking.
func Open(path string) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("tinydb: open log: %w", err)
	}
	db := New()
	db.log = file
	if err := db.loadLog(); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return db, nil
}

func (db *DB) loadLog() error {
	info, err := db.log.Stat()
	if err != nil {
		return fmt.Errorf("tinydb: stat log: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("tinydb: log must be a regular file")
	}
	if info.Size() == 0 {
		return db.writeLog([]byte(logHeader))
	}
	reader := bufio.NewReader(db.log)
	header, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("tinydb: read log header: %w", err)
	}
	if err != nil || header != logHeader {
		return fmt.Errorf("%w: missing or unsupported header", ErrCorrupt)
	}
	for lineNumber := 2; ; lineNumber++ {
		// ReadString handles large values without Scanner's default token limit.
		line, err := reader.ReadString('\n')
		if errors.Is(err, io.EOF) {
			if len(line) == 0 {
				return nil
			}
			return fmt.Errorf("%w: incomplete record at line %d", ErrCorrupt, lineNumber)
		}
		if err != nil {
			return fmt.Errorf("tinydb: read log at line %d: %w", lineNumber, err)
		}
		if err := db.replayRecord(strings.TrimSuffix(line, "\n")); err != nil {
			return fmt.Errorf("%w at line %d", err, lineNumber)
		}
	}
}

// Quoting escapes tabs, newlines, and arbitrary bytes, leaving one record per
// line. Unlike JSON string encoding, Go quoting also preserves invalid UTF-8.
func (db *DB) appendRecord(operation, key, value string) error {
	if db.log == nil {
		return nil
	}
	record := append([]byte(operation), '\t')
	record = strconv.AppendQuote(record, key)
	if operation == "put" {
		record = append(record, '\t')
		record = strconv.AppendQuote(record, value)
	}
	record = append(record, '\n')
	return db.writeLog(record)
}

func (db *DB) writeLog(record []byte) error {
	n, err := db.log.Write(record)
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}
	if err != nil {
		db.writeErr = fmt.Errorf("tinydb: append log: %w", err)
		return db.writeErr
	}
	return nil
}

func (db *DB) replayRecord(line string) error {
	fields := strings.Split(line, "\t")
	if len(fields) < 2 {
		return fmt.Errorf("%w: missing record fields", ErrCorrupt)
	}
	key, err := unquoteField(fields[1])
	if err != nil {
		return fmt.Errorf("%w: invalid quoted key", ErrCorrupt)
	}
	switch fields[0] {
	case "put":
		if len(fields) != 3 {
			return fmt.Errorf("%w: put needs key and value", ErrCorrupt)
		}
		value, err := unquoteField(fields[2])
		if err != nil {
			return fmt.Errorf("%w: invalid quoted value", ErrCorrupt)
		}
		db.data[key] = value
	case "delete":
		if len(fields) != 2 {
			return fmt.Errorf("%w: delete needs one key", ErrCorrupt)
		}
		delete(db.data, key)
	default:
		return fmt.Errorf("%w: unknown operation", ErrCorrupt)
	}
	return nil
}

func unquoteField(field string) (string, error) {
	if len(field) < 2 || field[0] != '"' {
		return "", errors.New("expected a double-quoted string")
	}
	return strconv.Unquote(field)
}
