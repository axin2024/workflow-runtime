package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workflow"
	"github.com/go-sql-driver/mysql"
)

var runStates = []string{"queued", "running", "succeeded", "failed", "action_required", "canceling", "canceled"}

func (s server) runs(w http.ResponseWriter, r *http.Request, auth session) {
	values := r.URL.Query()
	data := page{Title: "运行记录", Section: "runs", Auth: auth, Filters: values, States: runStates}
	filter, err := runFilter(values)
	if err != nil {
		data.Problem = err.Error()
		s.render(w, http.StatusBadRequest, data)
		return
	}
	data.Runs, err = s.db.ListRuns(r.Context(), auth.Scope, filter)
	if err != nil {
		s.actionError(w, r, data, err)
		return
	}
	query := make(url.Values)
	for _, field := range []string{"dag", "state", "from", "through", "before"} {
		if value := values.Get(field); value != "" {
			query.Set(field, value)
		}
	}
	data.ExportURL = "/runs/report.json"
	if len(query) > 0 {
		data.ExportURL += "?" + query.Encode()
	}
	var next int64
	data.Runs, next = runPageResults(data.Runs)
	if next > 0 {
		query.Set("before", strconv.FormatInt(next, 10))
		data.Next = "/?" + query.Encode()
	}
	s.render(w, http.StatusOK, data)
}

func runFilter(values url.Values) (store.RunFilter, error) {
	filter := store.RunFilter{DAG: values.Get("dag"), State: values.Get("state")}
	if filter.State != "" && !slices.Contains(runStates, filter.State) {
		return filter, errors.New("运行状态筛选无效。")
	}
	var err error
	if cursor := values.Get("before"); cursor != "" {
		filter.Before, err = positiveID(cursor)
		if err != nil {
			return filter, err
		}
	}
	filter.From, err = parseTimeInput(values.Get("from"))
	if err != nil {
		return filter, err
	}
	filter.Through, err = parseTimeInput(values.Get("through"))
	if err != nil {
		return filter, err
	}
	if filter.From != nil && filter.Through != nil && filter.From.After(*filter.Through) {
		return filter, errors.New("结束时间须晚于或等于开始时间。")
	}
	if filter.From != nil {
		utc := filter.From.UTC()
		filter.From = &utc
	}
	if filter.Through != nil {
		utc := filter.Through.UTC()
		filter.Through = &utc
	}
	return filter, nil
}

func runPageResults(items []store.RunSummary) ([]store.RunSummary, int64) {
	if len(items) <= store.ConsolePageSize {
		return items, 0
	}
	page := items[:store.ConsolePageSize]
	return page, page[len(page)-1].ID
}

func (s server) exportRunList(w http.ResponseWriter, r *http.Request, auth session) {
	filter, err := runFilter(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	items, err := s.db.ListRuns(r.Context(), auth.Scope, filter)
	if err != nil {
		s.actionError(w, r, page{Title: "运行记录", Section: "runs", Auth: auth, Filters: r.URL.Query(), States: runStates}, err)
		return
	}
	items, next := runPageResults(items)
	report := struct {
		Format      string             `json:"format"`
		GeneratedAt time.Time          `json:"generated_at"`
		GeneratedBy string             `json:"generated_by"`
		Workspace   string             `json:"workspace"`
		Filter      store.RunFilter    `json:"filter"`
		Items       []store.RunSummary `json:"items"`
		NextBefore  int64              `json:"next_before,omitempty"`
	}{
		Format: "workflow-runtime/run-list-v1", GeneratedAt: time.Now().UTC(), GeneratedBy: auth.Scope.Actor,
		Workspace: auth.Scope.Workspace, Filter: filter, Items: items, NextBefore: next,
	}
	s.writeDownload(w, "run-list.json", report)
}

func (s server) run(w http.ResponseWriter, r *http.Request, auth session) {
	data, ok := s.runPage(w, r, auth)
	if !ok {
		return
	}
	switch r.URL.Query().Get("result") {
	case "triggered":
		data.Notice = "运行已登记，可以在此跟踪执行。"
	case "canceled":
		data.Notice = "取消请求已记录。"
	case "rerun":
		data.Notice = "子运行已登记。"
	case "resolved":
		data.Notice = "外部动作的核对结果已登记。"
	}
	s.render(w, http.StatusOK, data)
}

func (s server) runPage(w http.ResponseWriter, r *http.Request, auth session) (page, bool) {
	id, err := positiveID(r.PathValue("id"))
	if err != nil {
		http.NotFound(w, r)
		return page{}, false
	}
	detail, err := s.db.GetRun(r.Context(), auth.Scope, id)
	if err != nil {
		s.readError(w, r, err)
		return page{}, false
	}
	return page{Title: fmt.Sprintf("运行 #%d", id), Section: "run", Auth: auth, Detail: &detail,
		Diagram: makeDiagram(detail.Version.Graph, detail.Nodes)}, true
}

func (p page) CanCancel() bool {
	return p.Auth.CanOperate() && slices.Contains([]string{"queued", "running", "action_required", "canceling"}, p.Detail.Run.State)
}

func (p page) FailedNodes() []string {
	var keys []string
	if p.Auth.CanOperate() && (p.Detail.Run.State == "failed" || p.Detail.Run.State == "canceled") {
		for _, node := range p.Detail.Nodes {
			if node.State == "failed" {
				keys = append(keys, node.Key)
			}
		}
	}
	return keys
}

func (s server) export(w http.ResponseWriter, r *http.Request, auth session) {
	id, err := positiveID(r.PathValue("id"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	report, err := s.db.ExportRun(r.Context(), auth.Scope, id)
	if err != nil {
		s.readError(w, r, err)
		return
	}
	s.writeDownload(w, fmt.Sprintf("run-%d.json", id), report)
}

func (s server) writeDownload(w http.ResponseWriter, filename string, report any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		s.logger.Printf("stream console report %s: %v", filename, err)
	}
}

type nodeComparison struct {
	Parent, Child store.NodeRun
}

func (s server) compare(w http.ResponseWriter, r *http.Request, auth session) {
	data, ok := s.runPage(w, r, auth)
	if !ok {
		return
	}
	if data.Detail.Rerun == nil {
		http.NotFound(w, r)
		return
	}
	parent, err := s.db.GetRun(r.Context(), auth.Scope, data.Detail.Rerun.Plan.ParentRunID)
	if err != nil {
		s.readError(w, r, err)
		return
	}
	oldNodes := make(map[string]store.NodeRun, len(parent.Nodes))
	for _, node := range parent.Nodes {
		oldNodes[node.Key] = node
	}
	for _, node := range data.Detail.Nodes {
		data.Comparison = append(data.Comparison, nodeComparison{Parent: oldNodes[node.Key], Child: node})
	}
	data.Title, data.Section, data.Compare = "父子运行对照", "compare", &parent
	s.render(w, http.StatusOK, data)
}

func (s server) runAction(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	data, ok := s.runPage(w, r, auth)
	if !ok {
		return
	}
	id := data.Detail.Run.ID
	form := r.PostForm
	key := form.Get("key")
	var err error
	var result string
	switch r.PathValue("action") {
	case "cancel":
		grace, parseErr := strconv.Atoi(form.Get("grace"))
		if parseErr != nil {
			err = errors.New("取消宽限必须是秒数。")
			break
		}
		_, err = s.db.CancelRun(r.Context(), auth.Scope, id, store.CancelRequest{Key: key, Reason: form.Get("reason"), GraceSeconds: grace})
		result = "canceled"
	case "rerun-preview":
		plan, previewErr := s.db.PreviewRerun(r.Context(), auth.Scope, id, form["nodes"])
		if previewErr != nil {
			err = previewErr
			break
		}
		data.Plan, data.RequestKey = &plan, key
		s.render(w, http.StatusOK, data)
		return
	case "rerun":
		record, rerunErr := s.db.CreateRerun(r.Context(), auth.Scope, id, key, form["nodes"])
		if rerunErr != nil {
			err = rerunErr
			break
		}
		id, result = record.RunID, "rerun"
	case "resolve":
		epoch, parseErr := strconv.ParseUint(form.Get("epoch"), 10, 64)
		if parseErr != nil {
			err = errors.New("核对必须指定尝试代数。")
			break
		}
		request := store.ResolveRequest{Key: key, Epoch: epoch, Evidence: form.Get("evidence"),
			State: workflow.NodeState(form.Get("state")), Branch: form.Get("branch"), Outputs: json.RawMessage(form.Get("outputs"))}
		_, err = s.db.ResolveExternal(r.Context(), auth.Scope, id, form.Get("node"), request)
		result = "resolved"
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.actionError(w, r, data, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/runs/%d?result=%s", id, result), http.StatusSeeOther)
}

func (s server) actionError(w http.ResponseWriter, r *http.Request, data page, err error) {
	var databaseError *mysql.MySQLError
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrNotFound):
		http.NotFound(w, r)
		return
	case errors.As(err, &databaseError):
		s.internalError(w, err)
		return
	case errors.Is(err, store.ErrRunControlConflict), errors.Is(err, store.ErrCapacityConflict), errors.Is(err, store.ErrBackfillSelectionChanged):
		status = http.StatusConflict
	}
	data.Problem = strings.TrimSpace(err.Error())
	s.render(w, status, data)
}
