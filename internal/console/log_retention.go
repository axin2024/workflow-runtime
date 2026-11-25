package console

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/axin2024/workflow-runtime/internal/store"
)

var cleanupFormKey = regexp.MustCompile(`^cleanup-[a-z2-7]{26,56}$`)

func (s server) retention(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	report, ok := s.retentionReport(w, r, auth)
	if !ok {
		return
	}
	data := retentionPage(auth, report)
	if r.URL.Query().Get("saved") == "1" {
		data.Notice = "日志保留策略已保存。"
	}
	s.render(w, http.StatusOK, data)
}

func (s server) retentionExport(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	report, ok := s.retentionReport(w, r, auth)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="log-retention.json"`)
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		s.logger.Printf("stream log retention report: %v", err)
	}
}

func (s server) retentionReport(w http.ResponseWriter, r *http.Request, auth session) (store.LogRetentionReport, bool) {
	var before int64
	if filter := r.URL.Query().Get("before"); filter != "" {
		var err error
		before, err = positiveID(filter)
		if err != nil {
			http.Error(w, "清理作业分页编号无效。", http.StatusBadRequest)
			return store.LogRetentionReport{}, false
		}
	}
	report, err := s.db.ExportLogRetention(r.Context(), auth.Scope, before)
	if err != nil {
		s.readError(w, r, err)
	}
	return report, err == nil
}

func retentionPage(auth session, report store.LogRetentionReport) page {
	data := page{Title: "日志保留", Section: "retention", Auth: auth, Retention: &report}
	if report.NextBefore > 0 {
		data.Next = fmt.Sprintf("/retention?before=%d", report.NextBefore)
	}
	if report.Policy.Days > 0 {
		data.RequestKey = "cleanup-" + strings.ToLower(rand.Text())
	}
	return data
}

func (s server) retentionPolicy(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	generation, genErr := strconv.ParseUint(r.PostForm.Get("generation"), 10, 64)
	days, daysErr := strconv.Atoi(r.PostForm.Get("days"))
	if genErr != nil || daysErr != nil || days < 0 || days > store.MaxLogRetentionDays {
		http.Error(w, "保留天数必须是 0 至 3650 的整数。", http.StatusBadRequest)
		return
	}
	if _, err := s.db.SaveLogRetention(r.Context(), auth.Scope, generation, days); err != nil {
		s.retentionError(w, r, auth, err)
		return
	}
	http.Redirect(w, r, "/retention?saved=1", http.StatusSeeOther)
}

func (s server) retentionCleanup(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	generation, genErr := strconv.ParseUint(r.PostForm.Get("generation"), 10, 64)
	key := r.PostForm.Get("key")
	limit, limitErr := strconv.Atoi(r.PostForm.Get("limit"))
	if genErr != nil || limitErr != nil || limit < 1 || limit > store.MaxLogCleanupBatch ||
		!cleanupFormKey.MatchString(key) {
		http.Error(w, "清理请求无效，请刷新日志保留页面。", http.StatusBadRequest)
		return
	}
	job, err := s.db.CleanExpiredLogs(r.Context(), auth.Scope, generation, key, limit)
	if err != nil {
		s.retentionError(w, r, auth, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/retention#cleanup-%d", job.ID), http.StatusSeeOther)
}

func (s server) retentionError(w http.ResponseWriter, r *http.Request, auth session, err error) {
	problem := ""
	switch {
	case errors.Is(err, store.ErrLogRetentionConflict):
		problem = "策略版本或清理请求已变化，请核对当前设置后重新提交。"
	case errors.Is(err, store.ErrLogRetentionDisabled):
		problem = "当前策略已关闭日志清理。"
	default:
		s.readError(w, r, err)
		return
	}
	report, ok := s.retentionReport(w, r, auth)
	if ok {
		data := retentionPage(auth, report)
		data.Problem = problem
		s.render(w, http.StatusConflict, data)
	}
}
