package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

type diskWriterSpy struct {
	events       []string
	failPut      string
	failSyncCall int
	syncAttempts int
	err          error
}

func (s *diskWriterSpy) Put(key, value string) error {
	s.events = append(s.events, "put "+key+"="+value)
	if key == s.failPut {
		return s.err
	}
	return nil
}

func (s *diskWriterSpy) Sync() error {
	s.events = append(s.events, "sync")
	s.syncAttempts++
	if s.syncAttempts == s.failSyncCall {
		return s.err
	}
	return nil
}

func TestWriteWithSyncSchedule(t *testing.T) {
	for _, tc := range []struct {
		name      string
		keys      []string
		every     int
		wantCalls int
		wantOrder []string
	}{
		{"each", []string{"a", "b", "c"}, 1, 3,
			[]string{"put a=v", "sync", "put b=v", "sync", "put c=v", "sync"}},
		{"partial final batch", []string{"a", "b", "c", "d", "e"}, 2, 3,
			[]string{"put a=v", "put b=v", "sync", "put c=v", "put d=v", "sync", "put e=v", "sync"}},
		{"exact batches avoid extra sync", []string{"a", "b", "c", "d"}, 2, 2,
			[]string{"put a=v", "put b=v", "sync", "put c=v", "put d=v", "sync"}},
		{"batch larger than workload", []string{"a", "b", "c"}, 10, 1,
			[]string{"put a=v", "put b=v", "put c=v", "sync"}},
		{"sync at end", []string{"a", "b", "c"}, 3, 1,
			[]string{"put a=v", "put b=v", "put c=v", "sync"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spy := &diskWriterSpy{}
			calls, err := writeWithSync(spy, tc.keys, "v", tc.every)
			if err != nil {
				t.Fatal(err)
			}
			if calls != tc.wantCalls {
				t.Errorf("Sync calls = %d, want %d", calls, tc.wantCalls)
			}
			if !reflect.DeepEqual(spy.events, tc.wantOrder) {
				t.Errorf("operation order = %q, want %q", spy.events, tc.wantOrder)
			}
		})
	}
}

func TestWriteWithSyncRejectsInvalidWorkload(t *testing.T) {
	for _, tc := range []struct {
		name  string
		keys  []string
		every int
	}{
		{"no keys", nil, 1},
		{"zero interval", []string{"a"}, 0},
		{"negative interval", []string{"a"}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spy := &diskWriterSpy{}
			if calls, err := writeWithSync(spy, tc.keys, "v", tc.every); err == nil || calls != 0 {
				t.Errorf("invalid writeWithSync = (%d, %v), want (0, error)", calls, err)
			}
			if len(spy.events) != 0 {
				t.Errorf("invalid workload performed operations: %q", spy.events)
			}
		})
	}
}

func TestWriteWithSyncStopsOnError(t *testing.T) {
	failure := errors.New("injected disk failure")
	for _, tc := range []struct {
		name         string
		keys         []string
		every        int
		failPut      string
		failSyncCall int
		wantCalls    int
		wantOrder    []string
	}{
		{"first put", []string{"a", "b", "c"}, 1, "a", 0, 0,
			[]string{"put a=v"}},
		{"later put", []string{"a", "b", "c"}, 1, "b", 0, 1,
			[]string{"put a=v", "sync", "put b=v"}},
		{"batch sync", []string{"a", "b", "c", "d"}, 2, "", 1, 0,
			[]string{"put a=v", "put b=v", "sync"}},
		{"partial final sync", []string{"a", "b", "c"}, 2, "", 2, 1,
			[]string{"put a=v", "put b=v", "sync", "put c=v", "sync"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spy := &diskWriterSpy{failPut: tc.failPut, failSyncCall: tc.failSyncCall, err: failure}
			calls, err := writeWithSync(spy, tc.keys, "v", tc.every)
			if !errors.Is(err, failure) {
				t.Errorf("writeWithSync error = %v, want injected failure", err)
			}
			if calls != tc.wantCalls {
				t.Errorf("successful Sync calls before failure = %d, want %d", calls, tc.wantCalls)
			}
			if !reflect.DeepEqual(spy.events, tc.wantOrder) {
				t.Errorf("operation order = %q, want %q", spy.events, tc.wantOrder)
			}
		})
	}
}

func TestCollectDiskVerifiesReplayAndCleansScratch(t *testing.T) {
	directory := t.TempDir()
	sentinelPath := filepath.Join(directory, "keep.log")
	sentinel := []byte("existing user data\x00\xff")
	if err := os.WriteFile(sentinelPath, sentinel, 0600); err != nil {
		t.Fatal(err)
	}
	// Five writes keep the real filesystem test small and produce a partial
	// batch at SyncEvery=2. Empty values exercise a valid boundary case.
	c := diskConfig{Writes: 5, ValueSize: 0, SyncEvery: 2, Directory: directory}
	r, err := collectDisk(c)
	if err != nil {
		t.Fatal(err)
	}
	if r.Storage != "disk" || r.Config != c || r.Directory != directory || r.Workers != 1 {
		t.Errorf("incorrect disk report configuration: storage=%q, config=%+v, directory=%q, workers=%d", r.Storage, r.Config, r.Directory, r.Workers)
	}
	if r.GoVersion != runtime.Version() || r.Platform != runtime.GOOS+"/"+runtime.GOARCH {
		t.Errorf("incorrect runtime metadata: Go=%q, platform=%q", r.GoVersion, r.Platform)
	}
	wantPolicies := []string{"sync-each", "sync-batch", "sync-end"}
	wantSyncCalls := []int{5, 3, 1}
	wantSyncEvery := []int{1, 2, 5}
	if len(r.Results) != len(wantPolicies) {
		t.Fatalf("disk result count = %d, want %d", len(r.Results), len(wantPolicies))
	}
	for i, row := range r.Results {
		if row.Policy != wantPolicies[i] || row.Writes != c.Writes || row.SyncCalls != wantSyncCalls[i] || row.VerifiedKeys != c.Writes {
			t.Errorf("result %d = %+v; want policy %q, %d writes verified, and %d Sync calls", i, row, wantPolicies[i], c.Writes, wantSyncCalls[i])
		}
		if row.SyncEvery != wantSyncEvery[i] {
			t.Errorf("%s Sync interval = %d, want %d", row.Policy, row.SyncEvery, wantSyncEvery[i])
		}
		if row.LogBytes <= 0 || row.LogBytes != r.Results[0].LogBytes {
			t.Errorf("%s log size = %d, want the same positive size as other policies (%d)", row.Policy, row.LogBytes, r.Results[0].LogBytes)
		}
		if row.ElapsedNS < 0 || row.ReplayNS < 0 {
			t.Errorf("%s has negative timing: elapsed=%d ns, replay=%d ns", row.Policy, row.ElapsedNS, row.ReplayNS)
		}
		for name, value := range map[string]float64{
			"operations per second":         row.OpsPerSecond,
			"mean ns per operation":         row.MeanNS,
			"allocated bytes per operation": row.AllocatedBytesPerOp,
			"allocations per operation":     row.AllocationsPerOp,
		} {
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				t.Errorf("%s %s = %g, want finite and nonnegative", row.Policy, name, value)
			}
		}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "keep.log" {
		t.Errorf("scratch directory was not cleaned or existing files changed: %v", entries)
	}
	after, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, sentinel) {
		t.Errorf("existing file changed: %q", after)
	}
}

func TestCollectDiskRejectsInvalidConfiguration(t *testing.T) {
	directory := t.TempDir()
	for _, tc := range []struct {
		name   string
		config diskConfig
	}{
		{"zero writes", diskConfig{Writes: 0, ValueSize: 0, SyncEvery: 2, Directory: directory}},
		{"negative writes", diskConfig{Writes: -1, ValueSize: 0, SyncEvery: 2, Directory: directory}},
		{"zero batch", diskConfig{Writes: 5, ValueSize: 0, SyncEvery: 0, Directory: directory}},
		{"negative batch", diskConfig{Writes: 5, ValueSize: 0, SyncEvery: -1, Directory: directory}},
		{"negative value size", diskConfig{Writes: 5, ValueSize: -1, SyncEvery: 2, Directory: directory}},
		{"empty directory", diskConfig{Writes: 5, ValueSize: 0, SyncEvery: 2, Directory: ""}},
		{"missing directory", diskConfig{Writes: 5, ValueSize: 0, SyncEvery: 2, Directory: filepath.Join(directory, "missing")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := collectDisk(tc.config); err == nil {
				t.Error("collectDisk succeeded with invalid configuration")
			}
		})
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("invalid configurations left files behind: %v", entries)
	}
}

func TestRunRejectsFlagsForWrongStorageMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"disk with keys", []string{"-disk", "-keys=10000"}},
		{"disk with ops", []string{"-disk", "-ops=1000000"}},
		{"disk with samples", []string{"-disk", "-samples=10000"}},
		{"memory with disk ops", []string{"-disk-ops=1000"}},
		{"memory with sync frequency", []string{"-sync-every=100"}},
		{"memory with disk directory", []string{"-disk-dir=."}},
		{"explicit memory with disk flag", []string{"-disk=false", "-sync-every=100"}},
		{"zero disk ops", []string{"-disk", "-disk-ops=0"}},
		{"zero disk batch", []string{"-disk", "-sync-every=0"}},
		{"negative disk value size", []string{"-disk", "-value-size=-1"}},
		{"empty disk directory", []string{"-disk", "-disk-dir="}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := run(tc.args, io.Discard); err == nil {
				t.Errorf("run(%q) succeeded, want an error", tc.args)
			}
		})
	}
}

func TestRunDiskJSON(t *testing.T) {
	var output bytes.Buffer
	directory := t.TempDir()
	args := []string{"-disk", "-disk-ops=5", "-sync-every=2", "-value-size=16", "-disk-dir", directory, "-json"}
	if err := run(args, &output); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	var r diskReport
	if err := decoder.Decode(&r); err != nil {
		t.Fatalf("decode disk report: %v\n%s", err, output.String())
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("expected one JSON report, trailing decode returned %v", err)
	}
	if r.Storage != "disk" || r.Config.Writes != 5 || r.Config.ValueSize != 16 || r.Config.SyncEvery != 2 || len(r.Results) != 3 {
		t.Errorf("incorrect decoded disk report: %+v", r)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	requireJSONFields(t, raw, "storage", "go_version", "platform", "storage_directory", "workers", "config", "results")
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(raw["results"], &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		requireJSONFields(t, row, "policy", "writes", "sync_every_writes", "timed_sync_calls", "write_elapsed_ns", "writes_per_second", "mean_ns_per_write", "allocated_bytes_per_write", "allocations_per_write", "log_bytes", "reopen_elapsed_ns", "verified_keys")
	}
}

func TestRunDiskHumanReport(t *testing.T) {
	var output bytes.Buffer
	args := []string{"-disk", "-disk-ops=5", "-sync-every=2", "-value-size=0", "-disk-dir", t.TempDir()}
	if err := run(args, &output); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"tiny-kube persistent writes", "5 distinct keys per policy", "0-byte values", "1 worker", "Filesystem directory",
		"sync-each", "sync-batch", "sync-end", "Sync every", "Sync calls", "Writes/sec", "Mean us/write",
		"Log KiB", "Reopen ms", "Verified", "B/write", "Allocs/write", "warm OS file cache",
	} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("human disk report missing %q", expected)
		}
	}
}
