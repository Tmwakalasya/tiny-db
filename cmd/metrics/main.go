// Command metrics measures TinyDB with a bounded, single-goroutine workload.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Tmwakalasya/tiny-db"
)

type config struct {
	Keys      int `json:"keys"`
	Ops       int `json:"read_and_overwrite_operations_each"`
	ValueSize int `json:"value_bytes"`
	Samples   int `json:"requested_latency_samples_per_workload"`
}

type latency struct {
	Samples int   `json:"samples"`
	P50NS   int64 `json:"p50_ns"`
	P95NS   int64 `json:"p95_ns"`
	P99NS   int64 `json:"p99_ns"`
}

type result struct {
	Name                string  `json:"workload"`
	Operations          int     `json:"operations"`
	Successful          int     `json:"successful_operations"`
	ElapsedNS           int64   `json:"elapsed_ns"`
	OpsPerSecond        float64 `json:"operations_per_second"`
	MeanNS              float64 `json:"mean_ns_per_operation"`
	AllocatedBytesPerOp float64 `json:"allocated_bytes_per_operation"`
	AllocationsPerOp    float64 `json:"allocations_per_operation"`
	Latency             latency `json:"sampled_latency"`
}

type memory struct {
	LogicalBytes       int64  `json:"logical_key_and_value_bytes"`
	LiveHeapDeltaBytes int64  `json:"process_live_heap_delta_bytes"`
	HeapObjectsDelta   int64  `json:"process_heap_objects_delta"`
	LiveHeapBytes      uint64 `json:"process_live_heap_bytes_after_load"`
}

type report struct {
	MeasuredAt    time.Time `json:"measured_at"`
	GoVersion     string    `json:"go_version"`
	Platform      string    `json:"platform"`
	CPUs          int       `json:"logical_cpus"`
	GOMAXPROCS    int       `json:"gomaxprocs"`
	Workers       int       `json:"workers"`
	Config        config    `json:"config"`
	Memory        memory    `json:"memory"`
	TimerMedianNS int64     `json:"empty_timer_pair_median_ns"`
	Results       []result  `json:"results"`
	Notes         []string  `json:"notes"`
}

type dataset struct {
	keys        []string
	values      []string
	replacement string
	logicalSize int64
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "metrics:", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	c := config{}
	flags := flag.NewFlagSet("metrics", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.IntVar(&c.Keys, "keys", 10_000, "number of distinct keys to insert and delete")
	flags.IntVar(&c.Ops, "ops", 1_000_000, "operations each for read and overwrite")
	flags.IntVar(&c.ValueSize, "value-size", 128, "bytes per string value")
	flags.IntVar(&c.Samples, "samples", 10_000, "separate latency samples per workload (insert/delete capped at keys)")
	jsonOutput := flags.Bool("json", false, "print a JSON report for saving or comparison")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q; use -help for options", flags.Arg(0))
	}
	if err := c.validate(); err != nil {
		return err
	}
	r, err := collect(c)
	if err != nil {
		return err
	}
	if *jsonOutput {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		return encoder.Encode(r)
	}
	return printReport(output, r)
}

func (c config) validate() error {
	if c.Keys <= 0 || c.Ops <= 0 || c.Samples <= 0 {
		return errors.New("-keys, -ops, and -samples must be greater than zero")
	}
	if c.ValueSize < 0 {
		return errors.New("-value-size must be zero or greater")
	}
	return nil
}

func makeDataset(c config) dataset {
	d := dataset{
		keys:        make([]string, c.Keys),
		values:      make([]string, c.Keys),
		replacement: strings.Repeat("u", c.ValueSize),
	}
	value := strings.Repeat("v", c.ValueSize)
	for i := range d.keys {
		d.keys[i] = "key:" + strconv.Itoa(i)
		// Give each nonempty initial value its own backing memory. The old
		// microbenchmarks deliberately share one value across the entire map.
		d.values[i] = strings.Clone(value)
		d.logicalSize += int64(len(d.keys[i])) + int64(len(d.values[i]))
	}
	return d
}

func collect(c config) (report, error) {
	if err := c.validate(); err != nil {
		return report{}, err
	}
	r := report{
		MeasuredAt: time.Now().UTC(), GoVersion: runtime.Version(),
		Platform: runtime.GOOS + "/" + runtime.GOARCH,
		CPUs:     runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		Workers: 1, Config: c,
		Notes: []string{
			"In-memory, single-goroutine workload; keys are visited in a repeating sequence.",
			"Bulk means include loop, key selection, callback, and success-counting overhead; setup and latency sampling are excluded.",
			"All workloads use prebuilt keys and values. Go shares immutable string bytes; payload creation and copying are excluded.",
			"Sampled latency is measured in a separate pass and includes clock overhead/resolution. A zero sample can mean the clock did not advance.",
			"Allocation counters are process-wide deltas during bulk work. Live heap is a process-wide change after GC, including the DB and input fixtures; it is not RSS or an exact DB-only size.",
			"Insert/delete each perform keys operations; read/overwrite each perform ops operations. No disk or network work is measured.",
		},
	}
	runtime.GC()
	var before, loaded runtime.MemStats
	runtime.ReadMemStats(&before)
	d := makeDataset(c)
	db := tinydb.New()

	// Each mutating workload starts with the state its name promises.
	workloads := []struct {
		name  string
		count int
		op    func(int) bool
	}{
		{"Insert new", c.Keys, func(i int) bool { return db.Put(d.keys[i], d.values[i]) == nil }},
		{"Read hit", c.Ops, func(i int) bool { _, ok := db.Get(d.keys[i%len(d.keys)]); return ok }},
		{"Overwrite", c.Ops, func(i int) bool { return db.Put(d.keys[i%len(d.keys)], d.replacement) == nil }},
		{"Delete hit", c.Keys, func(i int) bool { deleted, err := db.Delete(d.keys[i]); return err == nil && deleted }},
	}
	for i, workload := range workloads {
		measurement, err := measure(workload.name, workload.count, workload.op)
		if err != nil {
			return report{}, err
		}
		r.Results = append(r.Results, measurement)
		if i == 0 {
			// Force collection outside timed work and keep the loaded data live
			// through the snapshot, even if compiler liveness changes later.
			runtime.GC()
			runtime.ReadMemStats(&loaded)
			runtime.KeepAlive(db)
			runtime.KeepAlive(d)
			r.Memory = memory{
				LogicalBytes:       d.logicalSize,
				LiveHeapDeltaBytes: int64(loaded.HeapAlloc) - int64(before.HeapAlloc),
				HeapObjectsDelta:   int64(loaded.HeapObjects) - int64(before.HeapObjects),
				LiveHeapBytes:      loaded.HeapAlloc,
			}
		}
	}

	// Delete has emptied the database. Sample inserts into a fresh map so
	// their latency includes map growth, just like the bulk insertion pass.
	db = tinydb.New()
	for i, workload := range workloads {
		samples := c.Samples
		if i == 0 || i == 3 {
			samples = min(samples, c.Keys)
		}
		observed, err := sampleLatency(samples, workload.op)
		if err != nil {
			return report{}, fmt.Errorf("%s latency: %w", workload.name, err)
		}
		r.Results[i].Latency = observed
		if i == 0 {
			// Complete the load outside sample timing if fewer keys were sampled.
			for key := samples; key < len(d.keys); key++ {
				if err := db.Put(d.keys[key], d.values[key]); err != nil {
					return report{}, fmt.Errorf("load latency dataset: %w", err)
				}
			}
		}
	}
	timer, err := sampleLatency(c.Samples, func(int) bool { return true })
	if err != nil {
		return report{}, err
	}
	r.TimerMedianNS = timer.P50NS
	runtime.KeepAlive(db)
	runtime.KeepAlive(d)
	return r, nil
}

func measure(name string, operations int, op func(int) bool) (result, error) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	successful := 0
	start := time.Now()
	for i := 0; i < operations; i++ {
		if op(i) {
			successful++
		}
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	if successful != operations {
		return result{}, fmt.Errorf("%s: only %d of %d operations succeeded", name, successful, operations)
	}
	if elapsed <= 0 {
		return result{}, fmt.Errorf("%s: run too short for the clock; increase -keys or -ops", name)
	}
	return result{
		Name: name, Operations: operations, Successful: successful, ElapsedNS: elapsed.Nanoseconds(),
		OpsPerSecond:        float64(operations) / elapsed.Seconds(),
		MeanNS:              float64(elapsed.Nanoseconds()) / float64(operations),
		AllocatedBytesPerOp: float64(after.TotalAlloc-before.TotalAlloc) / float64(operations),
		AllocationsPerOp:    float64(after.Mallocs-before.Mallocs) / float64(operations),
	}, nil
}

func sampleLatency(count int, op func(int) bool) (latency, error) {
	values := make([]int64, count)
	for i := range values {
		start := time.Now()
		ok := op(i)
		values[i] = time.Since(start).Nanoseconds()
		if !ok {
			return latency{}, fmt.Errorf("operation %d failed", i)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return latency{
		Samples: count,
		P50NS:   percentile(values, 0.50), P95NS: percentile(values, 0.95), P99NS: percentile(values, 0.99),
	}, nil
}

// percentile uses nearest rank on an already sorted, nonempty sample.
func percentile(sorted []int64, quantile float64) int64 {
	return sorted[int(math.Ceil(quantile*float64(len(sorted))))-1]
}

func printReport(output io.Writer, r report) error {
	var text strings.Builder
	fmt.Fprintf(&text, "TinyDB performance | %s | %s | %s\n", r.MeasuredAt.Format(time.RFC3339), r.GoVersion, r.Platform)
	fmt.Fprintf(&text, "Workload: %d keys, %d-byte values, %d read/overwrite ops each, 1 worker\n\n", r.Config.Keys, r.Config.ValueSize, r.Config.Ops)
	w := tabwriter.NewWriter(&text, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "Operation\tOps\tOps/sec\tMean ns/op\tB/op\tAllocs/op")
	for _, row := range r.Results {
		fmt.Fprintf(w, "%s\t%d\t%.0f\t%.2f\t%.2f\t%.4f\n", row.Name, row.Operations, row.OpsPerSecond, row.MeanNS, row.AllocatedBytesPerOp, row.AllocationsPerOp)
	}
	_ = w.Flush() // strings.Builder writes cannot fail.
	fmt.Fprintln(&text, "\nSampled latency (separate pass, includes clock overhead):")
	w = tabwriter.NewWriter(&text, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "Operation\tSamples\tp50 ns\tp95 ns\tp99 ns")
	for _, row := range r.Results {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\n", row.Name, row.Latency.Samples, row.Latency.P50NS, row.Latency.P95NS, row.Latency.P99NS)
	}
	_ = w.Flush()
	fmt.Fprintf(&text, "Empty timer/callback median: %d ns (shown as context; never subtracted).\n", r.TimerMedianNS)
	const mib = 1024 * 1024
	fmt.Fprintf(&text, "\nMemory after initial load:\n  Logical keys + values: %.2f MiB\n  Live heap change (DB + fixtures + runtime): %+.2f MiB\n  Live heap objects change: %+d\n  Total process live Go heap: %.2f MiB\n", float64(r.Memory.LogicalBytes)/mib, float64(r.Memory.LiveHeapDeltaBytes)/mib, r.Memory.HeapObjectsDelta, float64(r.Memory.LiveHeapBytes)/mib)
	fmt.Fprintln(&text, "\nHow to read this:")
	for _, note := range r.Notes {
		fmt.Fprintln(&text, "-", note)
	}
	_, err := io.WriteString(output, text.String())
	return err
}
