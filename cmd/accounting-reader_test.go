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
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"
)

func TestAccountStatElapsedTime(t *testing.T) {
	for _, tc := range []struct {
		name        string
		elapsed     time.Duration
		transferred int64
		wantSpeed   float64
		wantElapsed time.Duration
	}{
		{name: "same clock tick", transferred: 7},
		{name: "future start", elapsed: -time.Second, transferred: 7},
		{name: "elapsed transfer", elapsed: time.Second, transferred: 7, wantSpeed: 7, wantElapsed: time.Second},
		{name: "no transfer", elapsed: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// A frozen clock reproduces a transfer completing within one
				// clock tick without depending on the host timer resolution.
				a := &accounter{
					startTime:  time.Now().Add(-tc.elapsed),
					total:      23,
					current:    tc.transferred,
					isFinished: make(chan struct{}),
				}
				stat := a.Stat()
				if stat.Speed != tc.wantSpeed || stat.Duration != tc.wantElapsed || stat.Total != 23 || stat.Transferred != tc.transferred {
					t.Fatalf("total=%d transferred=%d speed=%v duration=%v", stat.Total, stat.Transferred, stat.Speed, stat.Duration)
				}
				if !json.Valid([]byte(stat.JSON())) {
					t.Fatal("summary is not valid JSON")
				}
			})
		})
	}
}
