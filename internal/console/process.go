package console

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/axin2024/workflow-runtime/internal/process"
	"github.com/axin2024/workflow-runtime/internal/store"
)

type processPage struct {
	Definitions []store.ProcessSummary
	Version     *store.ProcessVersion
	Runs        []store.ProcessRun
	Detail      *store.ProcessRunDetail
	TaskQueue   []store.ProcessTask
	Diagnostics []process.Diagnostic
	Migration   *store.ProcessMigration
	Preview     bool
	Key         string
	Expected    int
	Source      string
}

func (s server) registerProcessPages(mux *http.ServeMux) {
	mux.HandleFunc("GET /processes", s.protect(s.processHome))
	mux.HandleFunc("GET /processes/{key}", s.protect(s.processVersion))
	mux.HandleFunc("GET /process-designer", s.protect(s.processDesigner))
	mux.HandleFunc("POST /process-designer", s.protect(s.processDesign))
	mux.HandleFunc("POST /processes/{key}/start", s.protect(s.processStart))
	mux.HandleFunc("GET /process-instances/{id}", s.protect(s.processInstance))
	mux.HandleFunc("GET /process-instances/{id}/report.json", s.protect(s.processInstance))
	mux.HandleFunc("POST /process-instances/{id}/cancel", s.protect(s.processCancel))
	mux.HandleFunc("GET /process-instances/{id}/signals", s.protect(s.processSignal))
	mux.HandleFunc("GET /process-tasks", s.protect(s.processTasks))
	mux.HandleFunc("POST /process-tasks/{id}", s.protect(s.processTaskChange))
	mux.HandleFunc("POST /process-actions/{id}/resolve", s.protect(s.processActionResolve))
	mux.HandleFunc("GET /process-migrations", s.protect(s.processMigrationEditor))
	mux.HandleFunc("POST /process-migrations", s.protect(s.processMigrationPreview))
	mux.HandleFunc("GET /process-migrations/{id}", s.protect(s.processMigrationDetail))
	mux.HandleFunc("POST /process-migrations/{id}", s.protect(s.processMigrationApply))
}

func processScreen(auth session, title, section string) page {
	return page{Title: title, Section: section, Auth: auth, RequestKey: "console-" + strings.ToLower(rand.Text()), Processes: &processPage{}}
}

func (s server) processHome(w http.ResponseWriter, r *http.Request, auth session) {
	data := processScreen(auth, "业务流程", "processes")
	var err error
	data.Processes.Definitions, err = s.db.ProcessDefinitions(r.Context(), auth.Scope, r.URL.Query().Get("after"))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	if len(data.Processes.Definitions) > store.ConsolePageSize {
		data.Processes.Definitions = data.Processes.Definitions[:store.ConsolePageSize]
		data.Next = "/processes?after=" + url.QueryEscape(data.Processes.Definitions[len(data.Processes.Definitions)-1].Key)
	}
	data.Processes.Runs, err = s.db.ProcessRuns(r.Context(), auth.Scope, "", 0)
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	data.Processes.Runs = data.Processes.Runs[:min(len(data.Processes.Runs), 12)]
	s.render(w, http.StatusOK, data)
}

func (s server) processVersion(w http.ResponseWriter, r *http.Request, auth session) {
	key := r.PathValue("key")
	if !process.ValidKey(key) {
		http.NotFound(w, r)
		return
	}
	rev, err := processNumber(r.URL.Query().Get("revision"))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	version, err := s.db.ProcessDefinition(r.Context(), auth.Scope, key, rev)
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	data := processScreen(auth, key, "process-version")
	data.Processes.Version = &version
	data.Diagram = makeProcessDiagram(version.Definition, "", "")
	before, err := processNumber(r.URL.Query().Get("before"))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	data.Processes.Runs, err = s.db.ProcessRuns(r.Context(), auth.Scope, key, int64(before))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	if len(data.Processes.Runs) > store.ConsolePageSize {
		data.Processes.Runs = data.Processes.Runs[:store.ConsolePageSize]
		data.Next = fmt.Sprintf("/processes/%s?revision=%d&before=%d", key, version.Revision, data.Processes.Runs[len(data.Processes.Runs)-1].ID)
	}
	s.render(w, http.StatusOK, data)
}

func (s server) processDesigner(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanAuthor()) {
		return
	}
	data := processScreen(auth, "流程设计器", "process-editor")
	if key := r.URL.Query().Get("from"); key != "" {
		version, err := s.db.ProcessDefinition(r.Context(), auth.Scope, key, 0)
		if err != nil {
			s.processProblem(w, r, err)
			return
		}
		data.Processes.Key, data.Processes.Expected, data.Processes.Source = version.Key, version.Revision, prettyJSON(version.Definition)
	}
	s.render(w, http.StatusOK, data)
}

func (s server) processDesign(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanAuthor()) {
		return
	}
	data := processScreen(auth, "流程设计器", "process-editor")
	values := data.Processes
	values.Key, values.Source = strings.TrimSpace(r.PostFormValue("key")), r.PostFormValue("definition")
	var err error
	values.Expected, err = processNumber(r.PostFormValue("expected"))
	def, decodeErr := process.Decode([]byte(values.Source))
	if err != nil || decodeErr != nil || !process.ValidKey(values.Key) {
		data.Problem = "请检查流程键、基线版本和 JSON 定义。"
		s.render(w, http.StatusUnprocessableEntity, data)
		return
	}
	values.Diagnostics, err = s.db.CheckProcessDefinition(r.Context(), auth.Scope, def)
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	if len(values.Diagnostics) != 0 {
		data.Problem = "流程定义有待修正的字段。"
		s.render(w, http.StatusUnprocessableEntity, data)
		return
	}
	values.Version = &store.ProcessVersion{Key: values.Key, Definition: def, Revision: values.Expected + 1}
	values.Preview = true
	data.Diagram = makeProcessDiagram(def, "", "")
	if r.PostFormValue("intent") != "publish" {
		s.render(w, http.StatusOK, data)
		return
	}
	version, err := s.db.PublishProcess(r.Context(), auth.Scope, values.Key, values.Expected, def)
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/processes/%s?revision=%d", version.Key, version.Revision), http.StatusSeeOther)
}

func (s server) processStart(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	revision, err := processNumber(r.PostFormValue("revision"))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	run, err := s.db.StartProcess(r.Context(), auth.Scope, r.PathValue("key"), store.NewProcessRun{Revision: revision,
		Business: strings.TrimSpace(r.PostFormValue("business")), Input: json.RawMessage(r.PostFormValue("input"))})
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	redirectProcessInstance(w, r, run.ID)
}

func (s server) processInstance(w http.ResponseWriter, r *http.Request, auth session) {
	id, ok := processConsoleID(w, r)
	if !ok {
		return
	}
	before, err := processNumber(r.URL.Query().Get("before"))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	detail, err := s.db.ProcessDetail(r.Context(), auth.Scope, id, int64(before))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/report.json") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(detail)
		return
	}
	data := processScreen(auth, detail.Run.Business, "process-instance")
	data.Processes.Detail = &detail
	data.Diagram = makeProcessDiagram(detail.Definition, detail.Run.Step, detail.Run.Status)
	if detail.LastEvent > 0 {
		data.Next = fmt.Sprintf("/process-instances/%d?before=%d#timeline", id, detail.LastEvent)
	}
	s.render(w, http.StatusOK, data)
}

func (s server) processCancel(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	id, ok := processConsoleID(w, r)
	if !ok {
		return
	}
	if _, err := s.db.CancelProcess(r.Context(), auth.Scope, id, r.PostFormValue("request")); err != nil {
		s.processProblem(w, r, err)
		return
	}
	redirectProcessInstance(w, r, id)
}

func (s server) processSignal(w http.ResponseWriter, r *http.Request, auth session) {
	id, ok := processConsoleID(w, r)
	if !ok {
		return
	}
	receipt, err := s.db.ProcessSignalReceipt(r.Context(), auth.Scope, "", id, strings.TrimSpace(r.URL.Query().Get("event")))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(receipt)
}

func processConsoleID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		http.NotFound(w, r)
		return 0, false
	}
	return id, true
}

func processNumber(input string) (int, error) {
	if input == "" {
		return 0, nil
	}
	number, err := strconv.Atoi(input)
	if err != nil || number < 0 {
		return 0, store.ErrInvalidProcess
	}
	return number, nil
}

func redirectProcessInstance(w http.ResponseWriter, r *http.Request, id int64) {
	http.Redirect(w, r, fmt.Sprintf("/process-instances/%d", id), http.StatusSeeOther)
}

func (s server) processProblem(w http.ResponseWriter, r *http.Request, err error) {
	var validation *store.ProcessValidationError
	switch {
	case errors.Is(err, store.ErrProcessPaused):
		http.Error(w, "该工作空间已暂停接收新实例。", http.StatusConflict)
	case errors.Is(err, store.ErrNotFound):
		http.NotFound(w, r)
	case errors.Is(err, store.ErrProcessConflict):
		http.Error(w, "流程状态或表单版本已更新，请返回刷新后重试。", http.StatusConflict)
	case errors.Is(err, store.ErrInvalidProcess), errors.As(err, &validation):
		http.Error(w, "请检查输入字段并重试。", http.StatusUnprocessableEntity)
	default:
		s.internalError(w, err)
	}
}
