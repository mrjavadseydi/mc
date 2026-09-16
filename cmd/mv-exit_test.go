// Copyright (c) 2026 PGSTY
//
// This file is part of the Silo object storage client.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
)

func TestMoveFailureExitStatus(t *testing.T) {
	for _, tc := range []struct {
		name       string
		denyDelete bool
		copyError  int
	}{
		{name: "success"},
		{name: "delete denied", denyDelete: true},
		{name: "copy denied", copyError: http.StatusForbidden},
		{name: "embedded copy error", copyError: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var copied, deleted atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				switch {
				case r.Method == http.MethodGet && r.URL.Query().Has("object-lock"):
					_, _ = fmt.Fprint(w, `<ObjectLockConfiguration/>`)
				case r.Method == http.MethodGet && r.URL.Query().Has("list-type"):
					_, _ = fmt.Fprint(w, `<ListBucketResult><Name>bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
				case r.Method == http.MethodHead:
					if r.URL.Path == "/bucket/target" {
						w.Header().Set("X-Minio-Error-Code", "NoSuchKey")
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("Content-Length", "7")
					w.Header().Set("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
					w.Header().Set("ETag", `"etag"`)
				case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
					if tc.copyError != 0 {
						w.WriteHeader(tc.copyError)
						_, _ = fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>copy denied</Message></Error>`)
						return
					}
					copied.Store(true)
					_, _ = fmt.Fprint(w, `<CopyObjectResult><LastModified>2026-01-01T00:00:00Z</LastModified><ETag>"etag"</ETag></CopyObjectResult>`)
				case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
					if tc.denyDelete {
						_, _ = fmt.Fprint(w, `<DeleteResult><Error><Key>source</Key><Code>AccessDenied</Code><Message>delete denied</Message></Error></DeleteResult>`)
					} else {
						deleted.Store(true)
						_, _ = fmt.Fprint(w, `<DeleteResult><Deleted><Key>source</Key></Deleted></DeleteResult>`)
					}
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			result := runChecksumVerifyCLI(t, server.URL, []string{"MC_REGION=us-east-1"}, "--json", "mv", "b4/bucket/source", "b4/bucket/target")
			wantExit := 0
			if tc.denyDelete || tc.copyError != 0 {
				wantExit = 1
			}
			if result.exitCode != wantExit || copied.Load() != (tc.copyError == 0) || deleted.Load() != (wantExit == 0) {
				t.Fatalf("exit=%d copied=%v deleted=%v; stdout=%s stderr=%s", result.exitCode, copied.Load(), deleted.Load(), result.stdout, result.stderr)
			}
			if tc.denyDelete && !bytes.Contains(result.stdout, []byte("delete denied")) {
				t.Fatalf("missing deletion diagnostic: %s", result.stdout)
			}
		})
	}
}

func TestRemoveManagerLifecycle(t *testing.T) {
	t.Run("partial deletion failure", func(t *testing.T) {
		rm := &removeManager{}
		results := make(chan RemoveResult, 2)
		results <- RemoveResult{}
		results <- RemoveResult{Err: probe.NewError(errors.New("delete denied"))}
		close(results)
		captureCommandJSON(t, func() {
			rm.readErrors(results, "/source")
			if rm.close() == nil {
				t.Error("deletion error was swallowed")
			}
		})
	})
	t.Run("full queue cancellation and concurrent close", func(t *testing.T) {
		queue := make(chan *ClientContent, 1)
		queue <- &ClientContent{}
		rm := &removeManager{removeMap: map[string]*removeClientInfo{"": {contentCh: queue}}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() { rm.add(ctx, "", "/source") })
		}
		done := make(chan struct{})
		go func() { _ = rm.close(); workers.Wait(); close(done) }()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("full queue or concurrent close deadlocked")
		}
		if rm.close() == nil {
			t.Error("canceled or rejected deletions were not recorded")
		}
	})
	t.Run("client creation releases lock", func(t *testing.T) {
		oldNew, oldLoad := S3New, loadMcConfig
		defer func() { S3New, loadMcConfig = oldNew, oldLoad }()
		loadMcConfig = func() (*configV10, *probe.Error) { return &configV10{}, nil }
		t.Setenv("MC_HOST_mvfail", "http://access:secret@localhost")
		S3New = func(*Config) (Client, *probe.Error) { return nil, probe.NewError(errors.New("invalid delete client")) }
		rm := &removeManager{removeMap: make(map[string]*removeClientInfo)}
		captureCommandJSON(t, func() {
			rm.add(context.Background(), "mvfail", "http://localhost/bucket/source")
			if !rm.removeMapMutex.TryLock() {
				t.Fatal("client creation error left the manager locked")
			}
			rm.removeMapMutex.Unlock()
			if rm.close() == nil {
				t.Error("client creation failure was not recorded")
			}
		})
	})
}

func TestMoveRepeatedInvocation(t *testing.T) {
	oldManager, oldLoad := rmManager, loadMcConfig
	defer func() { rmManager, loadMcConfig = oldManager, oldLoad }()
	loadMcConfig = func() (*configV10, *probe.Error) { return &configV10{}, nil }
	for i := range 2 {
		source, target := filepath.Join(t.TempDir(), "source"), filepath.Join(t.TempDir(), "target")
		if err := os.WriteFile(source, []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}
		flags := flag.NewFlagSet("mv", flag.ContinueOnError)
		for _, f := range mvFlags {
			f.Apply(flags)
		}
		if err := flags.Parse([]string{source, target}); err != nil {
			t.Fatal(err)
		}
		captureCommandJSON(t, func() {
			if err := mainMove(cli.NewContext(cli.NewApp(), flags, nil)); err != nil {
				t.Fatalf("invocation %d: %v", i+1, err)
			}
		})
		if _, err := os.Stat(source); !os.IsNotExist(err) {
			t.Fatalf("invocation %d left the source behind: %v", i+1, err)
		}
		if data, err := os.ReadFile(target); err != nil || string(data) != "payload" {
			t.Fatalf("invocation %d target=%q error=%v", i+1, data, err)
		}
	}
}
