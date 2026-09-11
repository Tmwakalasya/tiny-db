# Learning tiny-kube

[Back to the project README](../README.md)

A database we can build and understand one step at a time, using Go.

## Start here

Open Terminal and run:

```sh
git clone https://github.com/Tmwakalasya/tiny-db.git
cd tiny-db
go run ./cmd/demo
```

The demo stores a sample weather API response, reads it, replaces it, and deletes
it. It uses example data and makes no network requests. Install Go 1.24 or newer
before starting; this project uses only the standard library.

## Milestone 1: an in-memory baseline

We have three operations:

```go
db := tinydb.New()
if err := db.Put("name", "Ada"); err != nil {
    return err
}
value, found := db.Get("name")    // "Ada", true
deleted, err := db.Delete("name") // true, nil
if err != nil {
    return err
}
fmt.Printf("value=%q found=%t deleted=%t\n", value, found, deleted)
return db.Close()
```

This snippet belongs inside a function that returns an error, with `fmt` and
`github.com/Tmwakalasya/tiny-db` imported. `Put` and `Delete` return errors
because persistent writes can fail; check them in both modes so switching from
`New()` to `Open(path)` keeps failures visible.

Read [db.go](../db.go) first. The engine is a Go map protected by a read/write
lock. The map handles the initial indexing, and the lock allows goroutines to share the
database safely. Multiple readers may hold the read lock together; a writer
needs exclusive access. String values keep the first API simple.

An empty value is valid. `Get` returns a boolean so callers can distinguish an
empty value from a missing key. `Delete` also reports whether a key existed.

With `New()`, data lives only in RAM and disappears when the program exits.
This mode gives us a performance baseline to compare with the file-backed
version opened by `Open(path)`. A persistent cache for Python scripts is the
eventual example application; Python integration comes after the storage engine.

## Milestone 2: survive a restart

Run this command twice from the project folder:

```sh
go run ./cmd/persist -path ./tinydb.log
go run ./cmd/persist -path ./tinydb.log
```

With a new log, the first process saves a launch count of `1`. The second process
restores `1` from the file and saves `2`. Each later run increments the saved
count. This demo changes only the key `demo:launch-count`; an invalid stored
counter produces an error instead of silently resetting it.

Read [cmd/persist/main.go](../cmd/persist/main.go) for the complete example,
including error handling.
`tinydb.Open(path)` opens an existing log or creates one, then reconstructs the
database in memory. `tinydb.New()` still creates a database that uses only RAM.

### How the log works

For a persistent database, `Put` and `Delete` append a record to the file **before
updating the map**. If writing fails, the map stays unchanged. Startup reads the
records in order: a later put replaces the earlier value, and a delete removes
it. This is how we find the newest value without rewriting the old record.

Inspect the file with `cat tinydb.log`. A new file starts with `tinydb-v1`, then
uses one record per line. Fields are separated by tabs; keys and values use Go's
quoted-string escapes so tabs, newlines, and arbitrary string bytes round-trip.
After the two demo runs, the records will look like:

```text
tinydb-v1
put	"demo:launch-count"	"1"
put	"demo:launch-count"	"2"
```

`Put` returning successfully means the record was handed to the operating
system. Call `Sync()` when you want to request a flush to storage. `Close()` also
requests a flush and closes the file; **check its error**. The persistent demo
reports a saved count only after `Close()` succeeds. After closing, mutations
and `Sync()` return `ErrClosed`; `Get` can still read the in-memory snapshot.
Repeated `Close()` calls are safe.

This version supports one open handle or process per log file; it does not yet
lock files against another opener. Malformed or incomplete records cause
`Open` to return `ErrCorrupt`, rather than silently ignoring data. Automatic
repair is a later step. All current values still fit in RAM, and old records
remain in the file until we implement compaction.

`Put` and `Delete` return errors, so check them before treating a change as
successful. After a file write or flush error, later mutations stop; the log
can be inspected before reopening. A successful append followed by a failed
flush has an uncertain persistence outcome. This format has no checksums yet,
so syntactically valid changes to the file are not detected as corruption.

## See performance metrics

From the project folder, run:

```sh
go run ./cmd/metrics
```

This prints two tables for inserting new keys, reading existing keys, overwriting
values, and deleting existing keys:

| Metric | Meaning |
| --- | --- |
| Ops/sec | Operations completed per second; higher is faster. |
| Mean ns/op | Average time per operation over the entire bulk run; lower is faster. One nanosecond is one billionth of a second. |
| B/op | Heap bytes allocated per operation during the bulk run. |
| Allocs/op | Heap allocations per operation during the bulk run. |
| p50 / p95 / p99 ns | Time at or below which 50%, 95%, or 99% of separately sampled operations finished. |

Below the tables, you will see the logical size of the keys and values, the
change in live Go heap memory after loading them, and the process's total live
Go heap. Zero allocations per read means a read did not allocate new heap
memory; the database still occupies memory.

Change the workload or save a report with:

```sh
go run ./cmd/metrics -keys 100000 -ops 5000000 -value-size 256
go run ./cmd/metrics -json > metrics-latest.json
go run ./cmd/metrics -help
```

The JSON includes the workload settings, timestamp, Go version, platform,
operation counts, timings, allocations, and memory figures for later comparison.
It also retains the measurement notes. Each run starts with a fresh in-memory
database using `New()`; this command does not measure the persistent log yet.
The export command creates or replaces `metrics-latest.json` on your machine;
generated reports are not committed to the repository.

### What is being measured

Defaults are 10,000 keys, 128-byte values, one million reads, one million
overwrites, and one worker. Insert and delete each visit all keys once. Reads and
overwrites cycle through the keys in order. Keys and values are prepared before
timing, and each initial nonempty value has its own backing memory. Overwrite
assigns a prebuilt replacement string and reuses that replacement on later
cycles. The live-heap measurement includes the database, prepared input strings
and slices, and runtime changes; it is an approximate process-wide difference
after garbage collection, rather than an exact database footprint or OS memory
usage. Allocation counters also cover the whole process during each bulk run.

The latency table uses a **separate pass**, with 10,000 samples per operation by
default. `-samples` changes this count; insert and delete are capped at the key
count to preserve successful new inserts and deletions. Samples include the
timer's overhead and resolution. Very short samples can round to zero or advance
in coarse clock ticks, so use the **bulk mean** as the primary measure for these
tiny operations. The empty timer/callback median provides context and is never
subtracted from measurements.

All timings include the runner's loop, key selection, function call, and success
counting overhead. Go shares immutable string bytes, so these results exclude
payload construction and copying. They measure in-memory operations with no
disk or network work. For steadier comparisons, repeat runs with identical
flags, increase `-ops` for reads/overwrites, and avoid other heavy workloads.
Insert and delete runs can be short at small key counts. The original Go
microbenchmarks below use one shared value and a tighter loop, so their numbers
will differ. They remain useful for comparing changes to the small API.

## Run the checks and benchmarks

```sh
go test ./...
go test -race ./...
go test -run '^$' -bench . -benchmem -count=3
```

The benchmark results include:

- `ns/op`: average nanoseconds per operation; lower means less time.
- `B/op`: average bytes allocated per operation on the heap.
- `allocs/op`: average number of heap allocations per operation.

The initial benchmarks read and overwrite 10,000 existing keys with 128-byte
values. Setup happens outside the timed work. These measurements cover the
in-memory API, its locking, and some benchmark loop overhead. They do not measure
disk access, new-key insertion, network requests, or copying 128 bytes into and
out of storage. Go string assignment shares immutable string data.

Use these results to compare changes on the same machine. They are not yet a
fair comparison with a persistent database, which does additional work. Run
performance benchmarks without `-race`; the race detector adds overhead.

Record your own baseline with the commands above. Results depend on hardware,
Go version, workload settings, and background activity; they are measurements
of a particular run rather than fixed performance guarantees.

## What we will build next

1. Measure persistent writes, including the cost of flushing every write versus batches.
2. Store file positions in the index so values can live on disk.
3. Add a deliberate recovery policy for incomplete writes.
4. Compact old records, then profile and improve measured bottlenecks.

We will implement these in small steps, with an explanation and a check at each
step. The next design question: if the log stores every old value, how can we
reclaim space while keeping the latest value for each key?

## Files

- [db.go](../db.go): the database implementation.
- [log.go](../log.go): log encoding, replay, and file handling.
- [db_test.go](../db_test.go): behavior checks and performance benchmarks.
- [persistence_test.go](../persistence_test.go): replay, file-error, and persistence checks.
- [cmd/demo/main.go](../cmd/demo/main.go): a runnable in-memory example.
- [cmd/persist/main.go](../cmd/persist/main.go): a launch counter that survives process restarts.
- [cmd/metrics/main.go](../cmd/metrics/main.go): the performance report command, with optional JSON output.
- [cmd/metrics/main_test.go](../cmd/metrics/main_test.go): checks for the report and its calculations.
- [go.mod](../go.mod): the local Go module definition.
