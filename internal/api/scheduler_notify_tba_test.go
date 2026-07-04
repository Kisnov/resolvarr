package api

import (
	"strings"
	"testing"

	"resolvarr/internal/core"
)

// TestScheduleRunFoundNothing locks in the "notify only on change" gate:
// a TBA run that found nothing is suppressed; a run that found files, and
// any non-TBA mode, are not.
func TestScheduleRunFoundNothing(t *testing.T) {
	cases := []struct {
		name string
		mode core.JobMode
		res  any
		want bool
	}{
		{"tba found nothing", core.JobModeTbaRefresh, tbaRefreshScheduleResult{Found: 0}, true},
		{"tba found files", core.JobModeTbaRefresh, tbaRefreshScheduleResult{Found: 3, Series: 2}, false},
		{"tba applied renames", core.JobModeTbaRefresh, tbaRefreshScheduleResult{Found: 3, Applied: true, Queued: 3}, false},
		{"tba missing result", core.JobModeTbaRefresh, nil, false},
		{"non-tba mode never suppressed", core.JobModeRecover, tbaRefreshScheduleResult{Found: 0}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scheduleRunFoundNothing(tc.mode, core.RunSummary{Result: tc.res})
			if got != tc.want {
				t.Fatalf("scheduleRunFoundNothing = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestTbaRefreshModeField checks the notification field renders the same
// "Label: N" multi-line shape as the other mode fields, per apply/preview/
// error outcome.
func TestTbaRefreshModeField(t *testing.T) {
	cases := []struct {
		name string
		res  tbaRefreshScheduleResult
		want []string
	}{
		{"applied", tbaRefreshScheduleResult{Found: 4, Series: 2, Applied: true, Queued: 4}, []string{"Found: 4 across 2 series", "Renamed: 4"}},
		{"applied with failures", tbaRefreshScheduleResult{Found: 4, Series: 2, Applied: true, Queued: 3, Failed: 1}, []string{"Renamed: 3", "Series failed: 1"}},
		{"preview only", tbaRefreshScheduleResult{Found: 4, Series: 2}, []string{"Found: 4 across 2 series", "Preview only (no rename)"}},
		{"rename error", tbaRefreshScheduleResult{Found: 4, Series: 2, Applied: true, RenameError: "boom"}, []string{"Rename failed: boom"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := tbaRefreshModeField(core.RunSummary{Result: tc.res})
			if f.Name != "TBA refresh" {
				t.Fatalf("field name = %q, want %q", f.Name, "TBA refresh")
			}
			for _, want := range tc.want {
				if !strings.Contains(f.Value, want) {
					t.Fatalf("field value %q missing %q", f.Value, want)
				}
			}
		})
	}
}
