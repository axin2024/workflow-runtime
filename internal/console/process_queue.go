package console

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/axin2024/workflow-runtime/internal/store"
)

func (s server) processTasks(w http.ResponseWriter, r *http.Request, auth session) {
	data := processScreen(auth, "人工待办", "process-tasks")
	var err error
	data.Processes.TaskQueue, err = s.db.ProcessTaskQueue(r.Context(), auth.Scope)
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	s.render(w, http.StatusOK, data)
}

func (s server) processTaskChange(w http.ResponseWriter, r *http.Request, auth session) {
	id, ok := processConsoleID(w, r)
	if !ok {
		return
	}
	revision, err := strconv.ParseUint(r.PostFormValue("revision"), 10, 64)
	if err != nil {
		s.processProblem(w, r, store.ErrInvalidProcess)
		return
	}
	seconds, err := processNumber(r.PostFormValue("seconds"))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	input := store.ProcessTaskInput{Action: r.PostFormValue("action"), Revision: revision,
		Assignee: strings.TrimSpace(r.PostFormValue("assignee")), ClaimSecs: seconds, Note: r.PostFormValue("note")}
	if _, err = s.db.ChangeProcessTask(r.Context(), auth.Scope, id, r.PostFormValue("request"), input); err != nil {
		s.processProblem(w, r, err)
		return
	}
	http.Redirect(w, r, "/process-tasks", http.StatusSeeOther)
}

func (s server) processActionResolve(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	id, ok := processConsoleID(w, r)
	if !ok {
		return
	}
	outcome := store.ProcessActionResolution{Key: r.PostFormValue("operation_key"), Decision: r.PostFormValue("decision"),
		Receipt: strings.TrimSpace(r.PostFormValue("receipt")), Evidence: json.RawMessage(r.PostFormValue("evidence")), Reason: r.PostFormValue("reason")}
	action, err := s.db.ResolveProcessAction(r.Context(), auth.Scope, id, r.PostFormValue("request"), outcome)
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	redirectProcessInstance(w, r, action.Run)
}

func (s server) processMigrationEditor(w http.ResponseWriter, _ *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	data := processScreen(auth, "迁移预演", "process-migration-editor")
	s.render(w, http.StatusOK, data)
}

func (s server) processMigrationPreview(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	input := store.ProcessMigrationSpec{Process: strings.TrimSpace(r.PostFormValue("process"))}
	var err error
	input.Source, err = processNumber(r.PostFormValue("source_revision"))
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	input.Target, err = processNumber(r.PostFormValue("target_revision"))
	if err != nil || json.Unmarshal([]byte(r.PostFormValue("mapping")), &input.Mapping) != nil {
		s.processProblem(w, r, store.ErrInvalidProcess)
		return
	}
	for _, value := range strings.FieldsFunc(r.PostFormValue("runs"), func(r rune) bool { return strings.ContainsRune(", \r\n\t", r) }) {
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil || id <= 0 {
			s.processProblem(w, r, store.ErrInvalidProcess)
			return
		}
		input.Runs = append(input.Runs, id)
	}
	batch, err := s.db.PreviewProcessMigration(r.Context(), auth.Scope, r.PostFormValue("request"), input)
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/process-migrations/%d", batch.ID), http.StatusSeeOther)
}

func (s server) processMigrationDetail(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	id, ok := processConsoleID(w, r)
	if !ok {
		return
	}
	batch, err := s.db.ProcessMigration(r.Context(), auth.Scope, id)
	if err != nil {
		s.processProblem(w, r, err)
		return
	}
	data := processScreen(auth, "迁移批次", "process-migration")
	data.Processes.Migration = &batch
	s.render(w, http.StatusOK, data)
}

func (s server) processMigrationApply(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	id, ok := processConsoleID(w, r)
	if !ok {
		return
	}
	if _, err := s.db.ApplyProcessMigration(r.Context(), auth.Scope, id); err != nil {
		s.processProblem(w, r, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/process-migrations/%d", id), http.StatusSeeOther)
}
