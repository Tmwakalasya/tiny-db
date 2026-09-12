package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Tmwakalasya/tiny-db"
)

type diskConfig struct {
	Writes    int    `json:"writes_per_policy"`
	ValueSize int    `json:"value_bytes"`
	SyncEvery int    `json:"batch_sync_every_writes"`
	Directory string `json:"directory"`
}

type diskResult struct {
	Policy              string  `json:"policy"`
	Writes              int     `json:"writes"`
	SyncEvery           int     `json:"sync_every_writes"`
	SyncCalls           int     `json:"timed_sync_calls"`
	ElapsedNS           int64   `json:"write_elapsed_ns"`
	OpsPerSecond        float64 `json:"writes_per_second"`
	MeanNS              float64 `json:"mean_ns_per_write"`
	AllocatedBytesPerOp float64 `json:"allocated_bytes_per_write"`
	AllocationsPerOp    float64 `json:"allocations_per_write"`
	LogBytes            int64   `json:"log_bytes"`
	ReplayNS            int64   `json:"reopen_elapsed_ns"`
	VerifiedKeys        int     `json:"verified_keys"`
}

type diskReport struct {
	Storage    string       `json:"storage"`
	MeasuredAt time.Time    `json:"measured_at"`
	GoVersion  string       `json:"go_version"`
	Platform   string       `json:"platform"`
	Directory  string       `json:"storage_directory"`
	Workers    int          `json:"workers"`
	Config     diskConfig   `json:"config"`
	Results    []diskResult `json:"results"`
	Notes      []string     `json:"notes"`
}

func (c diskConfig) validate() error {
	if c.Writes <= 0 || c.SyncEvery <= 0 {
		return errors.New("-disk-ops and -sync-every must be greater than zero")
	}
	if c.ValueSize < 0 {
		return errors.New("-value-size must be zero or greater")
	}
	if c.Directory == "" {
		return errors.New("-disk-dir must name an existing directory")
	}
	return nil
}

func collectDisk(c diskConfig) (r diskReport, err error) {
	if err := c.validate(); err != nil {
		return r, err
	}
	directory, err := filepath.Abs(c.Directory)
	if err != nil {
		return r, fmt.Errorf("resolve benchmark directory: %w", err)
	}
	// A unique child directory keeps existing database files out of the workload.
	scratch, err := os.MkdirTemp(directory, ".tiny-kube-bench-")
	if err != nil {
		return r, fmt.Errorf("create benchmark directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(scratch); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("remove benchmark directory: %w", cleanupErr))
		}
	}()
	r = diskReport{
		Storage: "disk", MeasuredAt: time.Now().UTC(), GoVersion: runtime.Version(),
		Platform: runtime.GOOS + "/" + runtime.GOARCH, Directory: directory, Workers: 1, Config: c,
		Notes: []string{
			"Each policy inserts the same distinct keys and prebuilt ASCII value into a fresh log, using one worker.",
			"Write timing includes record encoding, appends, map updates, and every scheduled Sync, including the final partial batch.",
			"File/header creation and initial Sync, Close's extra Sync, reopen, verification, and cleanup are outside write timing.",
			"Sync batches flush requests; each Put still writes its own record. Batches are not atomic transactions.",
			"Sync requests the OS flush. These runs do not simulate power loss or establish hardware durability guarantees.",
			"Reopen timing measures immediate startup with a warm OS file cache. Every saved key and value is checked afterward.",
			"B/write and allocations/write are process-wide allocation deltas during the timed write workload; input generation is excluded.",
			"Policies run in fixed order on the chosen filesystem. Repeat runs for comparison; temporary benchmark files are removed.",
		},
	}
	keys := make([]string, c.Writes)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
	}
	value := strings.Repeat("v", c.ValueSize)
	for _, policy := range []struct {
		name  string
		every int
	}{
		{"sync-each", 1},
		{"sync-batch", c.SyncEvery},
		{"sync-end", c.Writes},
	} {
		measurement, err := measureDiskPolicy(filepath.Join(scratch, policy.name+".log"), policy.name, keys, value, policy.every)
		if err != nil {
			return r, fmt.Errorf("%s: %w", policy.name, err)
		}
		r.Results = append(r.Results, measurement)
	}
	return r, nil
}

type diskWriter interface {
	Put(key, value string) error
	Sync() error
}

// writeWithSync includes the last partial batch so every policy completes with
// an explicit flush. Counting only those calls makes the policy visible in output.
func writeWithSync(db diskWriter, keys []string, value string, every int) (syncCalls int, err error) {
	if every <= 0 || len(keys) == 0 {
		return 0, errors.New("write workload needs keys and a positive flush interval")
	}
	for i, key := range keys {
		if err := db.Put(key, value); err != nil {
			return syncCalls, fmt.Errorf("write %d: %w", i+1, err)
		}
		if (i+1)%every == 0 {
			if err := db.Sync(); err != nil {
				return syncCalls, fmt.Errorf("flush after write %d: %w", i+1, err)
			}
			syncCalls++
		}
	}
	if len(keys)%every != 0 {
		if err := db.Sync(); err != nil {
			return syncCalls, fmt.Errorf("flush final batch: %w", err)
		}
		syncCalls++
	}
	return syncCalls, nil
}

func measureDiskPolicy(path, policy string, keys []string, value string, every int) (r diskResult, err error) {
	db, err := tinydb.Open(path)
	if err != nil {
		return r, err
	}
	defer func() { joinCloseError(&err, db) }()
	if err := db.Sync(); err != nil {
		return r, fmt.Errorf("flush initial header: %w", err)
	}
	// Collect previous policy garbage before measuring this policy's allocations.
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	syncCalls, err := writeWithSync(db, keys, value, every)
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	if err != nil {
		return r, err
	}
	if elapsed <= 0 {
		return r, errors.New("write run too short for the clock; increase -disk-ops")
	}
	if err := db.Close(); err != nil {
		return r, fmt.Errorf("close written log: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return r, fmt.Errorf("measure log size: %w", err)
	}
	start = time.Now()
	reopened, err := tinydb.Open(path)
	replayTime := time.Since(start)
	if err != nil {
		return r, fmt.Errorf("reopen written log: %w", err)
	}
	defer func() { joinCloseError(&err, reopened) }()
	for _, key := range keys {
		if got, found := reopened.Get(key); !found || got != value {
			return r, fmt.Errorf("replay verification failed for %q", key)
		}
	}
	return diskResult{
		Policy: policy, Writes: len(keys), SyncEvery: every, SyncCalls: syncCalls,
		ElapsedNS: elapsed.Nanoseconds(), OpsPerSecond: float64(len(keys)) / elapsed.Seconds(),
		MeanNS:              float64(elapsed.Nanoseconds()) / float64(len(keys)),
		AllocatedBytesPerOp: float64(after.TotalAlloc-before.TotalAlloc) / float64(len(keys)),
		AllocationsPerOp:    float64(after.Mallocs-before.Mallocs) / float64(len(keys)),
		LogBytes:            info.Size(), ReplayNS: replayTime.Nanoseconds(), VerifiedKeys: len(keys),
	}, nil
}

func joinCloseError(err *error, db *tinydb.DB) {
	if closeErr := db.Close(); closeErr != nil && !errors.Is(*err, closeErr) {
		*err = errors.Join(*err, fmt.Errorf("close benchmark database: %w", closeErr))
	}
}

func printDiskReport(output io.Writer, r diskReport) error {
	var text strings.Builder
	fmt.Fprintf(&text, "tiny-kube persistent writes | %s | %s | %s\n", r.MeasuredAt.Format(time.RFC3339), r.GoVersion, r.Platform)
	fmt.Fprintf(&text, "Workload: %d distinct keys per policy, %d-byte values, 1 worker\nFilesystem directory: %s\n\n", r.Config.Writes, r.Config.ValueSize, r.Directory)
	w := tabwriter.NewWriter(&text, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "Policy\tSync every\tSync calls\tWrites/sec\tMean us/write\tLog KiB\tReopen ms\tVerified")
	for _, row := range r.Results {
		fmt.Fprintf(w, "%s\t%d\t%d\t%.0f\t%.2f\t%.2f\t%.3f\t%d\n", row.Policy, row.SyncEvery, row.SyncCalls, row.OpsPerSecond, row.MeanNS/1e3, float64(row.LogBytes)/1024, float64(row.ReplayNS)/1e6, row.VerifiedKeys)
	}
	_ = w.Flush()
	fmt.Fprintln(&text, "\nAllocations during writes (including flush calls):")
	w = tabwriter.NewWriter(&text, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "Policy\tB/write\tAllocs/write")
	for _, row := range r.Results {
		fmt.Fprintf(w, "%s\t%.2f\t%.4f\n", row.Policy, row.AllocatedBytesPerOp, row.AllocationsPerOp)
	}
	_ = w.Flush()
	fmt.Fprintln(&text, "\nHow to read this:")
	for _, note := range r.Notes {
		fmt.Fprintln(&text, "-", note)
	}
	_, err := io.WriteString(output, text.String())
	return err
}
