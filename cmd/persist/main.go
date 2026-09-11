// Command persist demonstrates rebuilding TinyDB from its append-only log.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/Tmwakalasya/tiny-db"
)

const counterKey = "demo:launch-count"

func main() {
	path := flag.String("path", "./tinydb.log", "path to the persistent database log")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "persist: unexpected positional arguments; use -help for options")
		os.Exit(1)
	}
	if err := run(*path); err != nil {
		fmt.Fprintln(os.Stderr, "persist:", err)
		os.Exit(1)
	}
}

func run(path string) (err error) {
	db, err := tinydb.Open(path)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	// Close also runs on early returns. Its error must reach main, where it
	// produces a nonzero exit status. A second Close is safe after success below.
	defer func() {
		if closeErr := db.Close(); closeErr != nil && !errors.Is(err, closeErr) {
			err = errors.Join(err, fmt.Errorf("close database: %w", closeErr))
		}
	}()

	var previous uint64
	if stored, found := db.Get(counterKey); found {
		previous, err = strconv.ParseUint(stored, 10, 64)
		if err != nil {
			return fmt.Errorf("stored %q value %q is not a valid launch count: %w", counterKey, stored, err)
		}
		fmt.Printf("Restored launch count from %s: %d\n", path, previous)
	} else {
		fmt.Printf("No saved launch count in %s; starting at 0.\n", path)
	}
	if previous == ^uint64(0) {
		return fmt.Errorf("stored %q has reached the largest supported launch count", counterKey)
	}

	next := previous + 1
	if err := db.Put(counterKey, strconv.FormatUint(next, 10)); err != nil {
		return fmt.Errorf("save launch count: %w", err)
	}
	// Put appends to the OS file buffer. Close requests a flush to storage and
	// releases the file; only report success after checking its error.
	if err := db.Close(); err != nil {
		return fmt.Errorf("close database: %w", err)
	}
	fmt.Printf("Saved launch count: %d (log flushed and closed).\n", next)
	fmt.Println("Run the same command again to restore this value in a new process.")
	return nil
}
