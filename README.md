# tiny-db

```text
 _   _                       _   _
| |_(_)_ __  _   _        __| | | |__
| __| | '_ \| | | | ___  / _` | | '_ \
| |_| | | | | |_| ||___|| (_| | | |_) |
 \__|_|_| |_|\__, |      \__,_| |_.__/
             |___/
```

An educational, embedded key-value database written in Go. tiny-db combines
an in-memory map with an optional append-only log so storage, recovery, and
performance tradeoffs are easy to inspect and measure.

The repository is `tiny-db`; the Go package is `tinydb`. The implementation uses
only the standard library and is a learning project with a small, evolving API.

## Quick start

Requires **Go 1.24 or newer** and Git.

```sh
git clone https://github.com/Tmwakalasya/tiny-db.git
cd tiny-db
go run ./cmd/demo
```

The in-memory demo writes, reads, overwrites, and deletes an example API
response. To keep data between processes, run the persistent demo twice:

```sh
go run ./cmd/persist -path ./tinydb.log
go run ./cmd/persist -path ./tinydb.log
```

With a new file, the first run saves launch count `1`; the next restores `1` and
saves `2`. The demo updates only `demo:launch-count` and checks write and close
errors. Its log is a local generated file.

```sh
go run ./cmd/metrics
```

The metrics command reports in-memory throughput, latency, allocations, and
heap usage; add `-disk` to compare persistent write policies. See
[Performance measurement](#performance-measurement) for its
workload and limits, or follow the [learning guide](docs/learning-guide.md) for
a step-by-step explanation of the engine.

## Use as a library

From an existing Go module:

```sh
go get github.com/Tmwakalasya/tiny-db
```

This complete example opens a persistent database, stores a value, reads it,
and reports errors from both the operation and cleanup:

```go
package main

import (
    "errors"
    "fmt"
    "log"

    tinydb "github.com/Tmwakalasya/tiny-db"
)

func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}

func run() (err error) {
    db, err := tinydb.Open("example.log")
    if err != nil {
        return err
    }
    defer func() {
        err = errors.Join(err, db.Close())
    }()

    if err := db.Put("language", "Go"); err != nil {
        return err
    }
    value, found := db.Get("language")
    fmt.Printf("found=%t value=%q\n", found, value)
    return nil
}
```

The named return value lets deferred cleanup return a `Close` error to `main`.
`Close` requests a flush to storage before releasing the file. Use
`db := tinydb.New()` when persistence is unnecessary.

### API

| Function or method | Behavior |
| --- | --- |
| `New() *DB` | Create an in-memory database. The zero value of `DB` is also usable. |
| `Open(path string) (*DB, error)` | Create a log or replay an existing one. Its parent directory must exist. |
| `Put(key, value string) error` | Insert or replace a value; append first when persistent. |
| `Get(key string) (string, bool)` | Read from memory; the boolean distinguishes a missing key from an empty value. |
| `Delete(key string) (bool, error)` | Remove an existing key and report whether it was present. Missing keys need no log record. |
| `Sync() error` | Request a file flush. No-op for an open in-memory database. |
| `Close() error` | Flush and close, preventing later mutations. Repeated calls return the same result. |

Keys and values are Go strings; empty strings are valid. A `DB` can be shared
between goroutines but must not be copied after use. After `Close`, `Get` can
read the final snapshot; mutations and `Sync` return `ErrClosed`. Use
`errors.Is(err, tinydb.ErrCorrupt)` to recognize malformed log data.

## Storage design

- **Index:** a `map[string]string` protected by `sync.RWMutex`. Reads use a
  shared lock; writes use an exclusive lock.
- **Write path:** encode a record, append it to the log, then change the map.
  A failed append leaves the in-memory value unchanged and stops later writes.
- **Recovery:** replay records in order during `Open`. Later puts replace
  earlier values; delete records remove them.
- **Format:** a `tinydb-v1` header followed by newline-terminated records with
  tab-separated fields. Go string quoting preserves tabs, newlines, and arbitrary
  bytes inside keys and values. The format is readable with a text viewer.

Read [db.go](db.go) for the API and synchronization, then [log.go](log.go) for
encoding, file I/O, and recovery. All current keys and values remain in memory;
opening a database scans its entire log.

### Durability and current limits

For a persistent database, `Put` and deletion of an existing key append log
records. A successful append hands bytes to the operating system without
requesting a storage flush. Call `Sync` at a chosen boundary or check `Close`,
which also requests a flush. A write or flush failure stops further mutations;
inspect the log before reopening. A failed flush leaves the persistence outcome
uncertain.

Only **one open database handle or process per log file** is supported. There
is no file locking to enforce that restriction. Multiple goroutines may share
that single handle.

Malformed or incomplete records cause `Open` to fail with `ErrCorrupt`, keeping
the existing log bytes available for inspection. There is no checksum,
automatic repair, or compaction. Syntactically valid corruption can go
undetected, and overwritten or deleted data remains in the growing log.
There are no transactions, SQL queries, or network server.

## Performance measurement

```sh
go run ./cmd/metrics
go run ./cmd/metrics -keys 100000 -ops 5000000 -value-size 256 -samples 10000
go run ./cmd/metrics -json > metrics-latest.json
go run ./cmd/metrics -help
```

JSON output records workload settings, timestamp, Go version, platform,
measurements, and interpretation notes. Generated reports stay local; create
one on your machine to establish a baseline.

| Metric | What it describes |
| --- | --- |
| Ops/sec | Completed operations per second during the bulk run. |
| Mean ns/op | Bulk elapsed time divided by completed operations. |
| B/op and Allocs/op | Process-wide heap allocation deltas per bulk operation. |
| p50, p95, p99 | Percentiles from a separate pass of individually timed operations. |
| Live heap change | Process-wide change after loading and garbage collection, including input fixtures and runtime changes. |

Defaults use one worker, 10,000 keys, and 128-byte values. Inserts and deletes
visit each key once; reads and overwrites each run one million operations,
cycling through existing keys. Inputs are prepared before timing. Overwrites
assign a prebuilt replacement string and reuse it on later cycles.

These measurements use `New()` and cover **in-memory operations only**. Go
shares immutable string bytes, so they exclude payload construction and copying.
Bulk timings include runner overhead. Sampled percentiles include timer overhead
and clock resolution; very short samples can round to zero. Use the bulk mean
as the primary timing for this small engine. Heap figures are neither an exact
database footprint nor operating-system RSS.

For comparisons, repeat runs with the same settings and Go version on the same
machine. Increase `-ops` for longer read/overwrite runs; insert/delete duration
depends on `-keys`. Avoid other heavy workloads. The original Go benchmarks use
a tighter loop and share one value across keys, so their numbers differ from
the metrics command:

```sh
go test -run '^$' -bench . -benchmem -count=3
```

Run performance measurements without `-race`; the race detector adds overhead.

### Compare persistent write policies

```sh
go run ./cmd/metrics -disk
go run ./cmd/metrics -disk -disk-ops 1000 -sync-every 100 -disk-dir . -value-size 128
go run ./cmd/metrics -disk -json > metrics-disk.json
```

Disk mode uses one worker and inserts distinct keys into three fresh log files,
running these policies in order:

| Policy | Timed flush requests |
| --- | --- |
| `sync-each` | `Sync` after every write. |
| `sync-batch` | `Sync` every `-sync-every` writes and after any partial final batch. |
| `sync-end` | One `Sync` after all writes. |

Defaults are `-disk-ops 1000` writes per policy, `-sync-every 100`, and
`-value-size 128` bytes. Every policy's final flush is inside the write timer.
Header creation and its initial `Sync`, the extra flush from `Close`, replay,
validation, and cleanup are outside that timer. Each policy reports throughput,
mean time per write, timed `Sync` call count, process-wide allocation deltas,
log bytes, replay duration, and verified key count.

`-disk-dir` selects the filesystem parent for temporary scratch directories;
it defaults to `.`. Each run removes its scratch data without changing existing
user logs. Each file is closed and immediately reopened, then every key/value
is checked. Replay therefore measures startup with a warm filesystem cache;
it does not simulate cold storage or establish survival after power loss.

`-keys`, `-ops`, and `-samples` are memory-only and are rejected with `-disk`.
`-disk-ops`, `-sync-every`, and `-disk-dir` require `-disk`; `-value-size` and
`-json` work in both modes. Batching changes flush frequency only: each `Put`
still encodes and writes its own record, and the batch is not atomic. See the
[flush lesson](docs/learning-guide.md#milestone-3-measure-the-cost-of-flushing)
for the tradeoff between throughput and writes awaiting a flush.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```

Tests cover the in-memory API, concurrent use of a shared handle, log replay,
corrupt records, file failures, and metrics calculations. The
[learning guide](docs/learning-guide.md) connects these behaviors to the design
choices behind them.

## Roadmap

- Available: persistent write comparisons with per-write, batched, and final flushes.
- Index file offsets so values can live on disk.
- Define recovery for incomplete writes and add integrity checks.
- Compact old records to reclaim space.
- Profile representative workloads and improve measured bottlenecks.
