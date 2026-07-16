// Read-side operator endpoints: pass detail, error browser, journal query,
// migration report and queue view (DESIGN-coordinator §6). All are thin views
// over the store plus the pass journals — no new state.
package api

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

func (s *Server) jobOr404(w http.ResponseWriter, name string) *store.Job {
	job, err := s.st.GetJob(name)
	if errors.Is(err, sql.ErrNoRows) {
		httpErr(w, http.StatusNotFound, "no such job")
		return nil
	}
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return nil
	}
	return job
}

// GET /api/v1/jobs/{name}/passes/{n}
func (s *Server) getPass(w http.ResponseWriter, r *http.Request) {
	job := s.jobOr404(w, r.PathValue("name"))
	if job == nil {
		return
	}
	n, err := strconv.Atoi(r.PathValue("n"))
	if err != nil {
		httpErr(w, http.StatusBadRequest, "bad pass number")
		return
	}
	p, err := s.st.PassByNo(job.ID, n)
	if errors.Is(err, sql.ErrNoRows) {
		httpErr(w, http.StatusNotFound, "no such pass")
		return
	}
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	counts, err := s.st.ShardStateCounts(p.ID)
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	shards := map[string]int64{}
	for st, c := range counts {
		shards[string(st)] = c
	}
	out := map[string]any{
		"job": job.Name, "pass": passViewOf(p), "shards": shards,
		"started_at_ms": p.Started.Int64,
	}
	if p.Finished.Valid {
		out["finished_at_ms"] = p.Finished.Int64
		out["duration_ms"] = p.Finished.Int64 - p.Started.Int64
	}
	writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------------------
// Journal-backed views
// ---------------------------------------------------------------------------

const (
	defaultPageLimit = 1000
	maxPageLimit     = 10000
)

// pageParams: ?pass=N|latest|all, ?limit, ?offset.
//
// The default (no pass= given) is ALL passes, not the latest one: errors and
// verify failures are recorded in whichever pass produced them, and a converged
// job's latest pass is typically the clean one — so a latest-only default would
// hide exactly the records an operator opens these views to find. `latest` is
// still available explicitly.
func (s *Server) pageParams(w http.ResponseWriter, r *http.Request, job *store.Job) (passes []int, limit, offset int, ok bool) {
	all, err := s.st.ListPasses(job.ID)
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return nil, 0, 0, false
	}
	if len(all) == 0 {
		httpErr(w, http.StatusNotFound, "job has no passes yet")
		return nil, 0, 0, false
	}
	switch q := r.URL.Query().Get("pass"); q {
	case "", "all":
		for _, p := range all {
			passes = append(passes, p.PassNo)
		}
	case "latest":
		passes = []int{all[len(all)-1].PassNo}
	default:
		n, err := strconv.Atoi(q)
		if err != nil {
			httpErr(w, http.StatusBadRequest, "bad pass parameter")
			return nil, 0, 0, false
		}
		passes = []int{n}
	}
	limit = defaultPageLimit
	if q := r.URL.Query().Get("limit"); q != "" {
		if limit, err = strconv.Atoi(q); err != nil || limit < 1 {
			httpErr(w, http.StatusBadRequest, "bad limit")
			return nil, 0, 0, false
		}
		limit = min(limit, maxPageLimit)
	}
	if q := r.URL.Query().Get("offset"); q != "" {
		if offset, err = strconv.Atoi(q); err != nil || offset < 0 {
			httpErr(w, http.StatusBadRequest, "bad offset")
			return nil, 0, 0, false
		}
	}
	return passes, limit, offset, true
}

var errPageFull = errors.New("page full")

// scanAll is an effectively-unbounded page limit used by summary aggregation,
// which must visit every matching record rather than one page of them.
const scanAll = int(^uint(0) >> 1)

// scanJournal streams matching records across the requested passes, applying
// offset/limit after the filter. Returns matched-total-so-far (capped by the
// page) and whether more matches remain beyond the page.
func (s *Server) scanJournal(job *store.Job, passes []int, limit, offset int,
	match func(*drsyncpb.JournalRecord) bool,
	emit func(passNo int, r *drsyncpb.JournalRecord)) (truncated bool, err error) {

	skipped, emitted := 0, 0
	for _, pn := range passes {
		err := journal.ReadRecords(s.journalRoot, job.ID, pn, func(rec *drsyncpb.JournalRecord) error {
			if !match(rec) {
				return nil
			}
			if skipped < offset {
				skipped++
				return nil
			}
			if emitted >= limit {
				truncated = true
				return errPageFull
			}
			emit(pn, rec)
			emitted++
			return nil
		})
		if err != nil && !errors.Is(err, errPageFull) {
			return truncated, err
		}
		if truncated {
			break
		}
	}
	return truncated, nil
}

func recView(passNo int, r *drsyncpb.JournalRecord) map[string]any {
	out := map[string]any{
		"pass":     passNo,
		"type":     strings.TrimPrefix(r.Type.String(), "JR_"),
		"rel_path": string(r.RelPath),
		"ts_ns":    r.TsNs,
	}
	if r.Src != nil {
		out["src"] = map[string]any{"mode": fmt.Sprintf("%04o", r.Src.Mode),
			"uid": r.Src.Uid, "gid": r.Src.Gid, "size": r.Src.Size,
			"mtime_ns": r.Src.MtimeNs, "nlink": r.Src.Nlink}
	}
	if r.Xxh3Lo != 0 || r.Xxh3Hi != 0 {
		out["xxh3"] = fmt.Sprintf("%016x%016x", r.Xxh3Hi, r.Xxh3Lo)
	}
	if r.Errno != 0 {
		out["errno"] = r.Errno
		out["class"] = errnoClass(r.Errno)
	}
	if r.Detail != "" {
		out["detail"] = r.Detail
	}
	return out
}

// errnoClass renders the symbolic errno name (EACCES, ESTALE, ...) — the
// error-browser grouping key. Falls back to the numeric value.
func errnoClass(errno int32) string {
	if name := unix.ErrnoName(syscall.Errno(errno)); name != "" {
		return name
	}
	return strconv.Itoa(int(errno))
}

// GET /api/v1/jobs/{name}/errors?pass=N|all&class=EACCES&path=prefix
func (s *Server) getErrors(w http.ResponseWriter, r *http.Request) {
	job := s.jobOr404(w, r.PathValue("name"))
	if job == nil {
		return
	}
	passes, limit, offset, ok := s.pageParams(w, r, job)
	if !ok {
		return
	}
	class := r.URL.Query().Get("class")
	pathPrefix := r.URL.Query().Get("path")

	// A verify failure is an error condition (mtime/mode/checksum/... mismatch);
	// it carries a reason in Detail rather than an errno, so it is grouped under
	// a synthetic "VERIFY_FAIL" class instead of an errnoClass.
	classOf := func(rec *drsyncpb.JournalRecord) string {
		if rec.Type == drsyncpb.JournalRecord_JR_VERIFY_FAIL {
			return "VERIFY_FAIL"
		}
		return errnoClass(rec.Errno)
	}

	records := []map[string]any{}
	byClass := map[string]int64{}
	truncated, err := s.scanJournal(job, passes, limit, offset,
		func(rec *drsyncpb.JournalRecord) bool {
			if rec.Type != drsyncpb.JournalRecord_JR_ERROR &&
				rec.Type != drsyncpb.JournalRecord_JR_FIDELITY_EXCEPTION &&
				rec.Type != drsyncpb.JournalRecord_JR_VERIFY_FAIL {
				return false
			}
			if pathPrefix != "" && !strings.HasPrefix(string(rec.RelPath), pathPrefix) {
				return false
			}
			if class != "" && classOf(rec) != class {
				return false
			}
			return true
		},
		func(pn int, rec *drsyncpb.JournalRecord) {
			byClass[classOf(rec)]++
			records = append(records, recView(pn, rec))
		})
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "read journal: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"job": job.Name, "count": len(records), "truncated": truncated,
		"by_class": byClass, "errors": records,
	})
}

// GET /api/v1/jobs/{name}/journal?pass=N|all&type=ORPHAN&path=prefix
func (s *Server) getJournal(w http.ResponseWriter, r *http.Request) {
	job := s.jobOr404(w, r.PathValue("name"))
	if job == nil {
		return
	}
	passes, limit, offset, ok := s.pageParams(w, r, job)
	if !ok {
		return
	}
	typeFilter := strings.ToUpper(r.URL.Query().Get("type"))
	typeFilter = strings.TrimPrefix(typeFilter, "JR_")
	pathPrefix := r.URL.Query().Get("path")

	match := func(rec *drsyncpb.JournalRecord) bool {
		if typeFilter != "" &&
			strings.TrimPrefix(rec.Type.String(), "JR_") != typeFilter {
			return false
		}
		return pathPrefix == "" || strings.HasPrefix(string(rec.RelPath), pathPrefix)
	}

	// Summary mode: count matching records by type across every requested pass
	// (no paging), returning the histogram instead of the records themselves.
	if r.URL.Query().Get("summary") == "true" {
		byType := map[string]int64{}
		var total int64
		_, err := s.scanJournal(job, passes, scanAll, 0, match,
			func(pn int, rec *drsyncpb.JournalRecord) {
				byType[strings.TrimPrefix(rec.Type.String(), "JR_")]++
				total++
			})
		if err != nil {
			httpErr(w, http.StatusInternalServerError, "read journal: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"job": job.Name, "summary": byType, "total": total,
		})
		return
	}

	records := []map[string]any{}
	truncated, err := s.scanJournal(job, passes, limit, offset, match,
		func(pn int, rec *drsyncpb.JournalRecord) {
			records = append(records, recView(pn, rec))
		})
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "read journal: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"job": job.Name, "count": len(records), "truncated": truncated,
		"records": records,
	})
}

// ---------------------------------------------------------------------------
// Migration report
// ---------------------------------------------------------------------------

// GET /api/v1/jobs/{name}/report — the audit/cutover summary: per-pass delta
// trajectory (the convergence curve), fidelity totals, nlink duplication
// cost, verify outcomes and outstanding operator work (parked shards,
// unreclaimed orphans).
func (s *Server) getReport(w http.ResponseWriter, r *http.Request) {
	job := s.jobOr404(w, r.PathValue("name"))
	if job == nil {
		return
	}
	passes, err := s.st.ListPasses(job.ID)
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}

	type passReport struct {
		passView
		DurationMS int64 `json:"duration_ms,omitempty"`
		DeltaFiles int64 `json:"delta_files"` // copied + meta-fixed
		DeltaBytes int64 `json:"delta_bytes"`
	}
	var (
		rep      []passReport
		totals   passView
		orphans  int64 // as of the latest scan pass
		deletePd bool
	)
	for _, p := range passes {
		pr := passReport{passView: passViewOf(p),
			DeltaFiles: p.FilesCopied + p.MetaFixed,
			DeltaBytes: p.BytesCopied}
		if p.Finished.Valid && p.Started.Valid {
			pr.DurationMS = p.Finished.Int64 - p.Started.Int64
		}
		rep = append(rep, pr)
		totals.FilesCopied += p.FilesCopied
		totals.BytesCopied += p.BytesCopied
		totals.MetaFixed += p.MetaFixed
		totals.Errors += p.Errors
		totals.FidelityExc += p.FidelityExceptions
		totals.VerifyOK += p.VerifyOK
		totals.VerifyFail += p.VerifyFail
		if p.EntriesWalked > 0 { // scan pass: orphan census supersedes previous
			orphans = p.Orphans
		} else if p.Orphans > 0 { // delete pass reports removals here
			deletePd = true
			orphans = 0
		}
	}

	parked, err := s.st.ParkedShards()
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	var parkedHere []map[string]any
	for _, sh := range parked {
		if sh.Job != job.Name {
			continue
		}
		parkedHere = append(parkedHere, map[string]any{
			"shard_id": sh.ID, "pass_no": sh.PassNo, "kind": sh.Kind,
			"rel_path": sh.RelPath, "attempt": sh.Attempt, "error": sh.Error,
		})
	}

	converged := job.State == model.JobCompleted
	writeJSON(w, http.StatusOK, map[string]any{
		"job": job.Name, "state": job.State, "dry_run": job.DryRun,
		"created_at_ms": job.CreatedAt,
		"passes":        rep,
		"totals": map[string]any{
			"files_copied": totals.FilesCopied, "bytes_copied": totals.BytesCopied,
			"meta_fixed": totals.MetaFixed, "errors": totals.Errors,
			"fidelity_exceptions": totals.FidelityExc,
			"verify_ok":           totals.VerifyOK, "verify_fail": totals.VerifyFail,
		},
		"converged":          converged,
		"orphans_remaining":  orphans,
		"delete_pass_ran":    deletePd,
		"parked_shards":      parkedHere,
		"parked_shard_count": len(parkedHere),
	})
}

// ---------------------------------------------------------------------------
// Queue view
// ---------------------------------------------------------------------------

// GET /api/v1/queue
func (s *Server) getQueue(w http.ResponseWriter, r *http.Request) {
	rows, err := s.st.QueueSummary()
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	parked, err := s.st.ParkedShards()
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	type bucket struct {
		Job    string           `json:"job"`
		PassNo int              `json:"pass_no"`
		Kind   model.ShardKind  `json:"kind"`
		State  model.ShardState `json:"state"`
		Count  int64            `json:"count"`
	}
	depth := make([]bucket, 0, len(rows))
	for _, q := range rows {
		depth = append(depth, bucket{q.Job, q.PassNo, q.Kind, q.State, q.Count})
	}
	sort.Slice(depth, func(i, j int) bool {
		if depth[i].Job != depth[j].Job {
			return depth[i].Job < depth[j].Job
		}
		return depth[i].PassNo < depth[j].PassNo
	})
	parkedOut := make([]map[string]any, 0, len(parked))
	for _, sh := range parked {
		parkedOut = append(parkedOut, map[string]any{
			"shard_id": sh.ID, "job": sh.Job, "pass_no": sh.PassNo,
			"kind": sh.Kind, "rel_path": sh.RelPath, "attempt": sh.Attempt,
			"error": sh.Error, "last_agent": sh.LastAgent,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"depth": depth, "parked": parkedOut})
}
