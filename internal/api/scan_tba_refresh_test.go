package api

import (
	"testing"

	"resolvarr/internal/arr"
)

// TestIsTbaRefreshCandidate locks in that the scan only targets files the
// rename can actually fix: current name has "TBA", new name does not. A file
// whose new name is still "TBA" (title not known yet) must be skipped so it
// doesn't recur and re-notify on every run.
func TestIsTbaRefreshCandidate(t *testing.T) {
	cases := []struct {
		name            string
		existing        string
		newPath         string
		season          int
		includeSpecials bool
		want            bool
	}{
		{"existing TBA, new real title", "Show/Season 01/Show - S01E01 - TBA.mkv", "Show/Season 01/Show - S01E01 - Real Title.mkv", 1, false, true},
		{"existing TBA, new still TBA", "Show/Season 01/Show - S01E01 - TBA WEBDL-720p.mkv", "Show/Season 01/Show - S01E01 - TBA WEBDL-1080p.mkv", 1, false, false},
		{"existing not TBA", "Show/Season 01/Show - S01E01 - Real.mkv", "Show/Season 01/Show - S01E01 - Better.mkv", 1, false, false},
		{"specials excluded by default", "Show/Specials/Show - S00E01 - TBA.mkv", "Show/Specials/Show - S00E01 - Real.mkv", 0, false, false},
		{"specials included when opted in", "Show/Specials/Show - S00E01 - TBA.mkv", "Show/Specials/Show - S00E01 - Real.mkv", 0, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := arr.SonarrRenameRecord{SeasonNumber: tc.season, ExistingPath: tc.existing, NewPath: tc.newPath}
			if got := isTbaRefreshCandidate(rec, tc.includeSpecials); got != tc.want {
				t.Fatalf("isTbaRefreshCandidate(%q -> %q, specials=%v) = %v, want %v",
					tc.existing, tc.newPath, tc.includeSpecials, got, tc.want)
			}
		})
	}
}
