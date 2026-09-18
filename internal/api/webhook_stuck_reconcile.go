package api

// webhook_stuck_reconcile.go — stuck-downloads adapter for the
// M-Webhook dispatcher. Event-driven twin of the Library scan's
// "Stuck downloads" action (scan_reconcile.go), which until now was
// manual-only: the user had to open the page and press Run.
//
// Why an IMPORT event is the right trigger: a download gets stuck
// precisely because a BETTER release of the same thing imported first.
// Sonarr/Radarr grab two releases seconds apart, the higher-scoring one
// wins the import race, and the loser sits in the queue forever in
// importPending / importBlocked on the pre-import category — the
// Your Friends & Neighbors S02E08 case in scan_reconcile.go's header.
// The Download event IS the winning import, so firing the sweep right
// after it is the narrowest possible moment to catch the loser.
//
// Scope: by default the sweep is narrowed to the movie / series the
// event was about (the losers of THIS race). ScopeWholeQueue widens it
// to the Library scan action's full-queue behaviour when the user wants
// every fire to be a general cleanup pass.
//
// Fan-out: the rule carries a LIST of (qBit instance, target category)
// targets, so one import event reconciles every download client the Arr
// feeds from. Each stuck hash is resolved to the client that actually
// holds it (see scan_reconcile_targets.go) — names aren't matched,
// hashes are.
//
// Deferred, always: the sweep reads the Arr queue + per-target history +
// manualimport evaluations, which is far too much work to do inline on
// a Connect webhook response that Sonarr/Radarr block on (that blocking
// is what serialised per-file imports on a season pack for the qBit
// category-fix adapter). It also needs the Arr to finish settling the
// losing rows into their stuck state first. So the dispatcher schedules
// it and returns immediately.
//
// Architectural rule, same as the Library scan path: the adapter NEVER
// decides redundancy itself. engine.ClassifyStuckDownload, reached via
// Server.runReconcile, is the single decision point. This file only
// decides WHEN to run it and WHAT to narrow it to.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"resolvarr/internal/core"
)

// stuckReconcileDeferDelay resolves how long after the import event the
// deferred sweep runs. 0 / unset → 60s default; clamped to 600s.
//
// The default is longer than the category-fix's 30s on purpose: that
// one only re-checks a single torrent's category, while this one needs
// the Arr to have finished processing the winning import AND settled
// the losing queue rows into importPending / importBlocked. Sweeping
// too early finds nothing and the rule looks broken.
func stuckReconcileDeferDelay(rule *core.WebhookRule) time.Duration {
	secs := 60
	if rule != nil && rule.StuckReconcile != nil && rule.StuckReconcile.DeferSeconds > 0 {
		secs = rule.StuckReconcile.DeferSeconds
	}
	if secs > maxStuckReconcileDeferSeconds {
		secs = maxStuckReconcileDeferSeconds
	}
	return time.Duration(secs) * time.Second
}

// scheduleDeferredStuckReconcile runs the stuck-downloads sweep in the
// background `delay` after the import event instead of inline in the
// Connect dispatch, and records its own single-function history entry +
// notification when it completes.
//
// Inputs are snapshotted (the request body + structs must survive past
// the HTTP response; config is re-read fresh at fire-time). Pending
// timers are lost on container restart — acceptable: the next import
// event fires another sweep, and the Library scan page is still there
// for a manual run.
//
// Mirrors scheduleDeferredCategoryFix exactly; if you change the
// snapshot / panic-guard / history-append shape in one, change both.
func (s *Server) scheduleDeferredStuckReconcile(rule *core.WebhookRule, inst *core.Instance, env *connectEventEnvelope, body []byte) {
	if rule == nil || inst == nil || env == nil {
		return
	}
	delay := stuckReconcileDeferDelay(rule)
	ruleCopy := *rule
	instCopy := *inst
	envCopy := *env
	bodyCopy := append([]byte(nil), body...)
	started := time.Now().UTC()
	time.AfterFunc(delay, func() {
		defer func() {
			if rec := recover(); rec != nil {
				fmt.Fprintf(os.Stderr, "resolvarr: deferred stuck-downloads sweep panicked for rule %s: %v\n", ruleCopy.ID, rec)
			}
		}()
		// Generous budget: the sweep does one queue read plus per-target
		// history + manualimport lookups, and then one category change
		// per redundant download across every configured client.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cfg := s.App.Config.Get()
		res := s.dispatchStuckReconcile(ctx, &ruleCopy, cfg, &envCopy, bodyCopy)
		run := buildWebhookRuleRun(&envCopy, bodyCopy, started, []functionResult{res})
		s.fireWebhookNotifications(&ruleCopy, &instCopy, &envCopy, bodyCopy, []functionResult{res}, run)
		s.appendWebhookRuleRunsBatch([]pendingRuleRun{{ruleID: ruleCopy.ID, run: run}})
	})
}

// dispatchStuckReconcile runs the stuck-downloads sweep for one rule on
// one import event. Returns OK=true for every clean path including
// "nothing was stuck"; OK=false only on real failures (missing config,
// Arr unreachable, qBit client init).
//
// Changed is true only when at least one download actually moved — the
// notification layer consumes Changed, and a rule firing on every single
// import must not emit a "swept, found nothing" line each time.
func (s *Server) dispatchStuckReconcile(
	ctx context.Context,
	rule *core.WebhookRule,
	cfg core.Config,
	env *connectEventEnvelope,
	body []byte,
) functionResult {
	const fn = core.WebhookFnStuckReconcile

	if env.EventType != string(core.WebhookEventDownload) {
		return functionResult{Function: fn, OK: true, Summary: "skipped (not a Download event)"}
	}
	if rule.StuckReconcile == nil || len(rule.StuckReconcile.Targets) == 0 {
		return functionResult{
			Function: fn, OK: false,
			Summary: "rule has the stuck-downloads function but no download clients configured",
		}
	}

	inst := findInstanceByID(cfg, rule.InstanceID)
	if inst == nil {
		return functionResult{
			Function: fn, OK: false,
			Summary: "instance vanished between event receive and dispatch",
		}
	}

	req := scanRunRequest{
		InstanceID: rule.InstanceID,
		Action:     "reconcile",
		Mode:       "apply",
	}
	for _, t := range rule.StuckReconcile.Targets {
		req.ReconcileTargets = append(req.ReconcileTargets, scanReconcileTarget{
			QbitInstanceID: t.QbitInstanceID,
			PostCategory:   t.PostCategory,
		})
	}

	// Narrow to the item this event was about unless the rule asked for
	// a whole-queue sweep. A rule firing on every import should do the
	// small obviously-correct thing by default.
	scopeLabel := "whole queue"
	if !rule.StuckReconcile.ScopeWholeQueue {
		var payload downloadEventPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			return functionResult{Function: fn, OK: false, Summary: "decode payload failed", Err: err}
		}
		switch rule.AppType {
		case "radarr":
			if payload.Movie == nil || payload.Movie.ID <= 0 {
				return functionResult{Function: fn, OK: true, Summary: "skipped (no movie on the event to scope the sweep to)"}
			}
			req.ReconcileOnlyMovieID = payload.Movie.ID
			scopeLabel = payload.Movie.Title
		case "sonarr":
			if payload.Series == nil || payload.Series.ID <= 0 {
				return functionResult{Function: fn, OK: true, Summary: "skipped (no series on the event to scope the sweep to)"}
			}
			req.ReconcileOnlySeriesID = payload.Series.ID
			scopeLabel = payload.Series.Title
		default:
			return functionResult{Function: fn, OK: false, Summary: "unknown app type " + rule.AppType}
		}
		if strings.TrimSpace(scopeLabel) == "" {
			scopeLabel = "this item"
		}
	}

	resp, apiErr := s.runReconcile(ctx, cfg, inst, rule.AppType, req)
	s.auditScan("webhook:"+rule.ID, "reconcile", inst, req, resp, errMsgOf(apiErr))
	if apiErr != nil {
		return functionResult{Function: fn, OK: false, Summary: "stuck-downloads sweep", Err: apiErr}
	}

	t := resp.Totals
	detail := StuckReconcileDetail{
		Scope:          scopeLabel,
		Stuck:          t.ReconcileStuck,
		Moved:          t.ReconcileRecategorised,
		NeedsAttention: t.ReconcileNeedsAttention,
		Failed:         t.ReconcileFailed,
		Moves:          collectStuckReconcileMoves(resp),
	}

	if t.ReconcileStuck == 0 {
		return functionResult{
			Function: fn, OK: true,
			Summary: fmt.Sprintf("nothing stuck (%s)", scopeLabel),
			Detail:  detail,
		}
	}

	parts := []string{fmt.Sprintf("%d stuck", t.ReconcileStuck)}
	if t.ReconcileRecategorised > 0 {
		parts = append(parts, fmt.Sprintf("%d moved", t.ReconcileRecategorised))
	}
	if t.ReconcileNeedsAttention > 0 {
		parts = append(parts, fmt.Sprintf("%d need attention", t.ReconcileNeedsAttention))
	}
	if t.ReconcileFailed > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", t.ReconcileFailed))
	}
	return functionResult{
		Function: fn,
		// A failed row is a per-item problem, not a failed run — the
		// sweep itself worked and the row carries the reason. Same
		// convention the scheduler's reconcile phase uses.
		OK:      true,
		Changed: t.ReconcileRecategorised > 0,
		Summary: fmt.Sprintf("%s (%s)", strings.Join(parts, "; "), scopeLabel),
		Detail:  detail,
	}
}

// collectStuckReconcileMoves flattens the per-row results into one line
// per download that actually moved, deduped by hash (a Sonarr season
// pack is N queue rows sharing one torrent).
func collectStuckReconcileMoves(resp *scanResponse) []StuckReconcileMove {
	if resp == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []StuckReconcileMove
	for _, row := range resp.Reconcile {
		if row.Status != "recategorised" || row.QbitCategory == "" {
			continue
		}
		if seen[row.DownloadID] {
			continue
		}
		seen[row.DownloadID] = true
		out = append(out, StuckReconcileMove{
			Title:        row.Title,
			QbitInstance: row.QbitInstanceName,
			Category:     row.QbitCategory,
		})
		if len(out) >= stuckReconcileMaxMovesInNotification {
			break
		}
	}
	return out
}

// stuckReconcileMaxMovesInNotification bounds how many individual moves
// a notification lists before falling back to the count in the summary.
// A whole-queue sweep on a backlogged setup can move dozens; an embed
// listing all of them is unreadable and may blow Discord's field limit.
const stuckReconcileMaxMovesInNotification = 10
