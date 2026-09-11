package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"runtime"
	"strings"
	"testing"
)

func TestPercentileNearestRank(t *testing.T) {
	hundred := make([]int64, 100)
	for i := range hundred {
		hundred[i] = int64(i + 1)
	}
	for _, tc := range []struct {
		name     string
		sorted   []int64
		quantile float64
		want     int64
	}{
		{"single sample", []int64{7}, 0.99, 7},
		{"even median takes lower rank", []int64{10, 20, 30, 40}, 0.50, 20},
		{"fractional rank rounds up", []int64{10, 20, 30}, 0.50, 20},
		{"small sample upper tail", []int64{10, 20, 30}, 0.95, 30},
		{"duplicate samples", []int64{0, 0, 4, 4}, 0.50, 0},
		{"p50", hundred, 0.50, 50},
		{"p95", hundred, 0.95, 95},
		{"p99", hundred, 0.99, 99},
		{"maximum", hundred, 1, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := percentile(tc.sorted, tc.quantile); got != tc.want {
				t.Errorf("percentile(%v, %g) = %d, want %d", tc.sorted, tc.quantile, got, tc.want)
			}
		})
	}
}

func TestCollect(t *testing.T) {
	// Test both partial insert sampling (which needs a completed load before
	// read sampling) and a request larger than the available insert/delete keys.
	for _, tc := range []struct {
		name    string
		samples int
	}{
		{"samples below key count", 25},
		{"samples above key count", 150},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := config{Keys: 100, Ops: 1000, ValueSize: 128, Samples: tc.samples}
			r, err := collect(c)
			if err != nil {
				t.Fatal(err)
			}
			if r.Config != c {
				t.Errorf("report config = %+v, want %+v", r.Config, c)
			}
			if r.MeasuredAt.IsZero() || r.GoVersion != runtime.Version() || r.Platform != runtime.GOOS+"/"+runtime.GOARCH {
				t.Errorf("missing or incorrect measurement metadata: time=%v, Go=%q, platform=%q", r.MeasuredAt, r.GoVersion, r.Platform)
			}
			if r.CPUs != runtime.NumCPU() || r.GOMAXPROCS != runtime.GOMAXPROCS(0) || r.Workers != 1 {
				t.Errorf("incorrect machine/workload concurrency: CPUs=%d, GOMAXPROCS=%d, workers=%d", r.CPUs, r.GOMAXPROCS, r.Workers)
			}
			// key:0 through key:9 use 50 bytes; key:10 through key:99 use 540.
			const wantLogicalBytes = 50 + 540 + 100*128
			if r.Memory.LogicalBytes != wantLogicalBytes {
				t.Errorf("logical bytes = %d, want %d", r.Memory.LogicalBytes, wantLogicalBytes)
			}
			if r.TimerMedianNS < 0 {
				t.Errorf("timer median = %d, want nonnegative", r.TimerMedianNS)
			}
			wantNames := []string{"Insert new", "Read hit", "Overwrite", "Delete hit"}
			wantOperations := []int{c.Keys, c.Ops, c.Ops, c.Keys}
			if len(r.Results) != len(wantNames) {
				t.Fatalf("result count = %d, want %d", len(r.Results), len(wantNames))
			}
			for i, row := range r.Results {
				if row.Name != wantNames[i] || row.Operations != wantOperations[i] || row.Successful != wantOperations[i] {
					t.Errorf("result %d = %q, %d operations, %d successful; want %q and %d successful operations", i, row.Name, row.Operations, row.Successful, wantNames[i], wantOperations[i])
				}
				for name, value := range map[string]float64{
					"operations per second":         row.OpsPerSecond,
					"mean ns per operation":         row.MeanNS,
					"allocated bytes per operation": row.AllocatedBytesPerOp,
					"allocations per operation":     row.AllocationsPerOp,
				} {
					if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
						t.Errorf("%s %s = %g, want finite and nonnegative", row.Name, name, value)
					}
				}
				if row.ElapsedNS < 0 {
					t.Errorf("%s elapsed ns = %d, want nonnegative", row.Name, row.ElapsedNS)
				}
				wantSamples := c.Samples
				if i == 0 || i == 3 {
					wantSamples = min(c.Samples, c.Keys)
				}
				if row.Latency.Samples != wantSamples {
					t.Errorf("%s sample count = %d, want %d", row.Name, row.Latency.Samples, wantSamples)
				}
				l := row.Latency
				if l.P50NS < 0 || l.P50NS > l.P95NS || l.P95NS > l.P99NS {
					t.Errorf("%s percentiles must be nonnegative and ordered: %+v", row.Name, l)
				}
			}
		})
	}
}

func TestRunJSONAndZeroByteValues(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"-keys", "100", "-ops", "1000", "-value-size", "0", "-samples", "125", "-json"}, &output); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	var raw map[string]json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		t.Fatalf("decode JSON report: %v\n%s", err, output.String())
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("expected only one JSON report, extra decode returned %v", err)
	}
	var r report
	if err := json.Unmarshal(output.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	if r.Config != (config{Keys: 100, Ops: 1000, ValueSize: 0, Samples: 125}) {
		t.Errorf("decoded config = %+v", r.Config)
	}
	if r.Memory.LogicalBytes != 590 {
		t.Errorf("zero-byte values logical size = %d, want 590 key bytes", r.Memory.LogicalBytes)
	}
	requireJSONFields(t, raw, "measured_at", "go_version", "platform", "logical_cpus", "gomaxprocs", "workers", "empty_timer_pair_median_ns")
	var memoryFields map[string]json.RawMessage
	if err := json.Unmarshal(raw["memory"], &memoryFields); err != nil {
		t.Fatal(err)
	}
	requireJSONFields(t, memoryFields, "logical_key_and_value_bytes", "process_live_heap_delta_bytes", "process_heap_objects_delta", "process_live_heap_bytes_after_load")
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(raw["results"], &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("JSON workload count = %d, want 4", len(rows))
	}
	for _, row := range rows {
		requireJSONFields(t, row, "workload", "operations", "successful_operations", "elapsed_ns", "operations_per_second", "mean_ns_per_operation", "allocated_bytes_per_operation", "allocations_per_operation")
		var latencyFields map[string]json.RawMessage
		if err := json.Unmarshal(row["sampled_latency"], &latencyFields); err != nil {
			t.Fatal(err)
		}
		requireJSONFields(t, latencyFields, "samples", "p50_ns", "p95_ns", "p99_ns")
	}
}

func requireJSONFields(t *testing.T, object map[string]json.RawMessage, fields ...string) {
	t.Helper()
	for _, field := range fields {
		if value, ok := object[field]; !ok || string(value) == "null" {
			t.Errorf("JSON field %q missing or null", field)
		}
	}
}

func TestRunHumanReport(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"-keys", "100", "-ops", "1000", "-value-size", "16", "-samples", "25"}, &output); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"TinyDB performance", "100 keys", "16-byte values", "1000 read/overwrite ops each", "1 worker",
		"Insert new", "Read hit", "Overwrite", "Delete hit", "Ops/sec", "Mean ns/op", "B/op", "Allocs/op",
		"Sampled latency", "p50 ns", "p95 ns", "p99 ns", "clock overhead",
		"Memory after initial load", "Logical keys + values", "Live heap change", "MiB",
	} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("human report missing %q", expected)
		}
	}
}

func TestRunRejectsInvalidArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"-unknown"}},
		{"noninteger count", []string{"-keys", "many"}},
		{"invalid boolean", []string{"-json=maybe"}},
		{"missing flag value", []string{"-ops"}},
		{"zero keys", []string{"-keys", "0"}},
		{"negative keys", []string{"-keys", "-1"}},
		{"zero ops", []string{"-ops", "0"}},
		{"negative ops", []string{"-ops", "-1"}},
		{"zero samples", []string{"-samples", "0"}},
		{"negative samples", []string{"-samples", "-1"}},
		{"negative value size", []string{"-value-size", "-1"}},
		{"positional argument", []string{"extra"}},
		{"positional argument after flags", []string{"-keys", "100", "extra"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := run(tc.args, io.Discard); err == nil {
				t.Errorf("run(%q) succeeded, want an error", tc.args)
			}
		})
	}
}
