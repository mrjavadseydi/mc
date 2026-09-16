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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v7"
	"github.com/pgsty/silo-pkg/v3/console"
)

// captureCommandJSON is for serial in-process tests of command diagnostics.
func captureCommandJSON(t *testing.T, fn func()) string {
	t.Helper()
	oldJSON, oldPrint := globalJSON, console.Println
	defer func() { globalJSON, console.Println = oldJSON, oldPrint }()
	var out bytes.Buffer
	globalJSON = true
	console.Println = func(args ...any) { _, _ = fmt.Fprintln(&out, args...) }
	fn()
	return out.String()
}

func TestObjectLockFailureExitStatus(t *testing.T) {
	for _, operation := range [][]string{
		{"legalhold", "set"}, {"legalhold", "clear"},
		{"retention", "set", "governance", "1d"}, {"retention", "clear"},
	} {
		for _, tc := range []struct {
			name      string
			keys      []string
			recursive bool
			versions  bool
		}{
			{name: "single denied", keys: []string{"denied"}},
			{name: "single allowed", keys: []string{"allowed"}},
			{name: "partial failure", keys: []string{"denied", "allowed"}, recursive: true},
			{name: "all failed", keys: []string{"denied"}, recursive: true},
			{name: "empty", recursive: true},
			{name: "versions", keys: []string{"denied", "allowed"}, recursive: true, versions: true},
		} {
			t.Run(strings.Join(operation[:2], "/")+"/"+tc.name, func(t *testing.T) {
				var puts atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/xml")
					switch {
					case r.Method == http.MethodGet && r.URL.Query().Has("object-lock"):
						_, _ = fmt.Fprint(w, `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`)
					case r.Method == http.MethodGet && r.URL.Path == "/bucket/":
						root, entry := "ListBucketResult", "Contents"
						if tc.versions {
							root, entry = "ListVersionsResult", "Version"
						}
						_, _ = fmt.Fprintf(w, "<%s><Name>bucket</Name><IsTruncated>false</IsTruncated>", root)
						for _, key := range tc.keys {
							_, _ = fmt.Fprintf(w, `<%s><Key>%s</Key><VersionId>v1</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-01T00:00:00Z</LastModified><ETag>"etag"</ETag><Size>1</Size></%s>`, entry, key, entry)
						}
						_, _ = fmt.Fprintf(w, "</%s>", root)
					case r.Method == http.MethodPut:
						puts.Add(1)
						if r.URL.Path == "/bucket/denied" {
							w.WriteHeader(http.StatusForbidden)
							_, _ = fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>object lock denied</Message></Error>`)
						}
					default:
						t.Errorf("unexpected request: %s %s", r.Method, r.URL)
						w.WriteHeader(http.StatusBadRequest)
					}
				}))
				defer server.Close()
				args := append([]string{"--json"}, operation...)
				target := "b4/bucket/"
				if tc.recursive {
					args = append(args, "--recursive")
				} else {
					target += tc.keys[0]
				}
				if tc.versions {
					args = append(args, "--versions")
				}
				result := runChecksumVerifyCLI(t, server.URL, []string{"MC_REGION=us-east-1"}, append(args, target)...)
				wantExit := 0
				if tc.name != "single allowed" && (len(tc.keys) != 0 || operation[0] == "retention") {
					wantExit = 1
				}
				if result.exitCode != wantExit || puts.Load() != int32(len(tc.keys)) {
					t.Fatalf("exit=%d want=%d puts=%d want=%d; stdout=%s stderr=%s", result.exitCode, wantExit, puts.Load(), len(tc.keys), result.stdout, result.stderr)
				}
				failures := bytes.Count(result.stdout, []byte(`"status":"error"`)) + bytes.Count(result.stdout, []byte(`"status":"failure"`))
				if len(tc.keys) != 0 && wantExit != 0 && (failures != 1 || !bytes.Contains(result.stdout, []byte("object lock denied"))) {
					t.Errorf("expected exactly one failure diagnostic: %s", result.stdout)
				}
				if bytes.Contains(result.stdout, []byte("Invalid URL")) || bytes.Contains(result.stdout, []byte("Unable to find")) && len(tc.keys) != 0 {
					t.Errorf("misleading diagnostic: %s", result.stdout)
				}
			})
		}
	}
}

type legalHoldListClient struct{ Client }

func (legalHoldListClient) GetURL() ClientURL {
	return *newClientURL("http://localhost/bucket/")
}

func (legalHoldListClient) List(context.Context, ListOptions) <-chan *ClientContent {
	ch := make(chan *ClientContent, 1)
	ch <- &ClientContent{URL: *newClientURL("http://localhost/bucket/object")}
	close(ch)
	return ch
}

func TestObjectLockClientCreationFailure(t *testing.T) {
	oldNew, oldLoad := S3New, loadMcConfig
	defer func() { S3New, loadMcConfig = oldNew, oldLoad }()
	loadMcConfig = func() (*configV10, *probe.Error) { return &configV10{}, nil }
	t.Setenv("MC_HOST_lockfail", "http://access:secret@localhost")
	want := errors.New("cannot construct object client")
	calls := 0
	S3New = func(*Config) (Client, *probe.Error) {
		calls++
		if calls == 1 {
			return legalHoldListClient{}, nil
		}
		return nil, probe.NewError(want)
	}
	out := captureCommandJSON(t, func() {
		if err := setLegalHold(context.Background(), "lockfail/bucket/", "", time.Time{}, false, true, minio.LegalHoldEnabled); err == nil {
			t.Error("legalhold swallowed client creation error")
		}
	})
	if !strings.Contains(out, want.Error()) {
		t.Fatalf("missing legalhold diagnostic: %s", out)
	}
	out = captureCommandJSON(t, func() {
		err := setRetentionSingle(context.Background(), lockOpSet, "lockfail", "http://localhost/bucket/object", "", minio.Governance, time.Now(), false)
		if err == nil || !errors.Is(err.ToGoError(), want) {
			t.Errorf("retention error = %v", err)
		}
	})
	if strings.Count(out, `"status":"failure"`) != 1 || !strings.Contains(out, "lockfail/bucket/object") {
		t.Fatalf("missing retention diagnostic: %s", out)
	}
}

func TestRetentionEmptyValidity(t *testing.T) {
	result := runChecksumVerifyCLI(t, "http://localhost", nil, "--json", "retention", "set", "governance", "", "b4/bucket/object")
	if result.exitCode != 1 || !bytes.Contains(result.stdout, []byte("invalid validity argument")) || bytes.Contains(result.stderr, []byte("panic")) {
		t.Fatalf("exit=%d stdout=%s stderr=%s", result.exitCode, result.stdout, result.stderr)
	}
	if validity, unit, err := parseRetentionValidity("1d"); err != nil || validity != 1 || unit != minio.Days {
		t.Fatalf("valid duration changed: %d %s %v", validity, unit, err)
	}
}
