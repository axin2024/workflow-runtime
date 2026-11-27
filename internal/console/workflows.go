package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workflow"
)

type page struct {
	Title, Section  string
	Auth            session
	Problem, Notice string
	Next            string
	ExportURL       string
	Workflows       []store.WorkflowSummary
	Runs            []store.RunSummary
	Filters         url.Values
	States          []string
	Revision        uint64
	Source          string
	Version         *store.Version
	Detail          *store.RunDetail
	Diagram         diagram
	Diff            []string
	Plan            *store.RerunPlan
	RequestKey      string
	Trigger         triggerForm
	Compare         *store.RunDetail
	Comparison      []nodeComparison
	DAG             string
	Schedules       []store.ScheduleSummary
	ScheduleEdit    *scheduleForm
	Backfills       []store.BackfillSummary
	Batch           *store.BackfillBatch
	BackfillDraft   *backfillForm
	Selected        []int
	Capacity        *capacityPage
	Retention       *store.LogRetentionReport
	Processes       *processPage
}

func (s server) workflows(w http.ResponseWriter, r *http.Request, auth session) {
	list, err := s.db.ListWorkflows(r.Context(), auth.Scope, r.URL.Query().Get("after"))
	if err != nil {
		s.readError(w, r, err)
		return
	}
	data := page{Title: "工作流", Section: "workflows", Auth: auth, Workflows: list}
	if len(list) > store.ConsolePageSize {
		data.Workflows = list[:store.ConsolePageSize]
		data.Next = "/workflows?after=" + url.QueryEscape(data.Workflows[len(data.Workflows)-1].Key)
	}
	s.render(w, http.StatusOK, data)
}

func (s server) workflow(w http.ResponseWriter, r *http.Request, auth session) {
	key := r.PathValue("key")
	var revision uint64
	if requested := r.URL.Query().Get("revision"); requested != "" {
		var err error
		revision, err = strconv.ParseUint(requested, 10, 64)
		if err != nil || revision == 0 {
			http.Error(w, "版本号必须是正整数。", http.StatusBadRequest)
			return
		}
	}
	version, err := s.db.GetVersion(r.Context(), auth.Scope, key, revision)
	if err != nil {
		s.readError(w, r, err)
		return
	}
	data := workflowPage(auth, version)
	if r.URL.Query().Get("published") == "1" {
		data.Notice = fmt.Sprintf("版本 %d 已发布。", version.Revision)
	}
	s.render(w, http.StatusOK, data)
}

func workflowPage(auth session, version store.Version) page {
	return page{Title: version.Graph.Definition.Key, Section: "workflow", Auth: auth, Version: &version,
		Diagram: makeDiagram(version.Graph, nil), Trigger: triggerForm{Inputs: "{}", Kind: "manual"}}
}

func (s server) editor(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanAuthor()) {
		return
	}
	data := page{Title: "工作流编辑器", Section: "editor", Auth: auth}
	if source := r.URL.Query().Get("from"); source != "" {
		version, err := s.db.GetVersion(r.Context(), auth.Scope, source, 0)
		if err != nil {
			s.readError(w, r, err)
			return
		}
		data.Revision, data.Source = version.Revision, prettyJSON(version.Graph.Definition)
		data.Version, data.Diagram = &version, makeDiagram(version.Graph, nil)
	}
	s.render(w, http.StatusOK, data)
}

func (s server) edit(w http.ResponseWriter, r *http.Request, auth session) {
	command := r.PostForm.Get("command")
	if !requireRole(w, auth.CanAuthor() && (command != "publish" || auth.CanOperate())) {
		return
	}
	data := page{Title: "工作流编辑器", Section: "editor", Auth: auth, Source: r.PostForm.Get("source")}
	base, err := strconv.ParseUint(r.PostForm.Get("revision"), 10, 64)
	if err != nil {
		data.Problem = "基础版本必须是非负整数。"
		s.render(w, http.StatusBadRequest, data)
		return
	}
	data.Revision = base
	raw := []byte(data.Source)
	compiled, err := workflow.Parse(raw)
	if err != nil {
		data.Problem = err.Error()
		s.render(w, http.StatusBadRequest, data)
		return
	}
	data.Source, data.Diagram = prettyJSON(compiled.Definition), makeDiagram(compiled, nil)
	switch command {
	case "validate":
		data.Notice = "定义校验通过。"
		s.render(w, http.StatusOK, data)
	case "publish":
		version, err := s.db.Publish(r.Context(), auth.Scope, base, raw)
		var conflict *store.RevisionConflict
		if errors.As(err, &conflict) {
			data.Problem = fmt.Sprintf("当前版本为 %d，编辑基线为 %d。请核对差异后更新基础版本。", conflict.Active, base)
			if conflict.Active > 0 {
				current, err := s.db.GetVersion(r.Context(), auth.Scope, compiled.Definition.Key, conflict.Active)
				if err != nil {
					s.readError(w, r, err)
					return
				}
				data.Diff = definitionDiff(current.Graph.Definition, compiled.Definition)
			}
			s.render(w, http.StatusConflict, data)
			return
		}
		if err != nil {
			s.readError(w, r, err)
			return
		}
		http.Redirect(w, r, fmt.Sprintf("/workflows/%s?revision=%d&published=1", version.Graph.Definition.Key, version.Revision), http.StatusSeeOther)
	default:
		http.Error(w, "请选择校验或发布。", http.StatusBadRequest)
	}
}

func definitionDiff(current, proposed workflow.Definition) []string {
	var result []string
	if !reflect.DeepEqual(current.Inputs, proposed.Inputs) {
		result = append(result, "工作流输入契约有变化")
	}
	previous := make(map[string]workflow.Node, len(current.Nodes))
	for _, node := range current.Nodes {
		previous[node.Key] = node
	}
	for _, node := range proposed.Nodes {
		old, found := previous[node.Key]
		switch {
		case !found:
			result = append(result, "新增节点 "+node.Key)
		case !reflect.DeepEqual(old, node):
			result = append(result, "更新节点 "+node.Key)
		}
		delete(previous, node.Key)
	}
	for key := range previous {
		result = append(result, "移除节点 "+key)
	}
	slices.Sort(result)
	return result
}

type triggerForm struct {
	Key, Kind, Inputs, LogicalAt string
}

func (s server) trigger(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	key := r.PathValue("key")
	revision, err := strconv.ParseUint(r.PostForm.Get("revision"), 10, 64)
	if err != nil || revision == 0 {
		http.Error(w, "版本号必须是正整数。", http.StatusBadRequest)
		return
	}
	version, err := s.db.GetVersion(r.Context(), auth.Scope, key, revision)
	if err != nil {
		s.readError(w, r, err)
		return
	}
	data := workflowPage(auth, version)
	data.Trigger = triggerForm{Key: r.PostForm.Get("key"), Kind: r.PostForm.Get("kind"),
		Inputs: r.PostForm.Get("inputs"), LogicalAt: r.PostForm.Get("logical_at")}
	request := store.TriggerRequest{DAG: key, Revision: revision, Key: data.Trigger.Key,
		Kind: data.Trigger.Kind, Inputs: []byte(data.Trigger.Inputs)}
	request.LogicalAt, err = parseTimeInput(data.Trigger.LogicalAt)
	if err == nil {
		var values map[string]json.RawMessage
		if json.Unmarshal(request.Inputs, &values) != nil || values == nil {
			err = errors.New("输入必须是 JSON 对象。")
		} else {
			err = workflow.ValidateValues("inputs", version.Graph.Definition.Inputs, values)
		}
	}
	if err != nil {
		data.Problem = err.Error()
		s.render(w, http.StatusBadRequest, data)
		return
	}
	result, err := s.db.Trigger(r.Context(), auth.Scope, request)
	if err != nil {
		var conflict *store.TriggerConflict
		if errors.As(err, &conflict) {
			data.Problem = fmt.Sprintf("此触发键已属于运行 #%d，请更换键或使用原输入。", conflict.RunID)
			s.render(w, http.StatusConflict, data)
			return
		}
		s.actionError(w, r, data, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/runs/%d?result=triggered", result.Run.ID), http.StatusSeeOther)
}

func parseTimeInput(value string) (*time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	at, err := time.Parse(time.RFC3339, value)
	if err != nil {
		at, err = time.Parse("2006-01-02T15:04", value)
	}
	if err != nil || at.UTC().Year() < 1000 || at.UTC().Year() > 9999 {
		return nil, errors.New("请填写有效的 UTC 日期时间。")
	}
	return &at, nil
}
