package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
)

type scheduleForm struct {
	Key, Cron, Zone, Misfire, Grace, Limit, Revision, Inputs, Start, Generation string
	Enabled                                                                     bool
	Current                                                                     *store.Schedule
	ActiveRevision                                                              uint64
}

func timestamp(at time.Time) string { return at.Format(time.RFC3339Nano) }

func scheduleEditor(current *store.Schedule, active uint64) *scheduleForm {
	if current == nil {
		return &scheduleForm{Cron: "0 * * * *", Zone: "UTC", Misfire: "catch_up", Grace: "60", Limit: "3",
			Revision: "0", Generation: "0", Inputs: "{}", Start: timestamp(time.Now().UTC().Truncate(time.Second)), ActiveRevision: active}
	}
	return &scheduleForm{Key: current.Key, Cron: current.Cron, Zone: current.Timezone, Misfire: current.Misfire,
		Grace: strconv.Itoa(current.GraceSeconds), Limit: strconv.Itoa(current.MaxActiveRuns),
		Revision: strconv.FormatUint(current.Revision, 10), Generation: strconv.FormatUint(current.Generation, 10),
		Inputs: prettyJSON(current.Inputs), Start: timestamp(current.StartAfter), Enabled: current.Enabled,
		Current: current, ActiveRevision: active}
}

func (s server) schedules(w http.ResponseWriter, r *http.Request, auth session) {
	data, ok := s.schedulePage(w, r, auth)
	if ok {
		s.render(w, http.StatusOK, data)
	}
}

func (s server) schedule(w http.ResponseWriter, r *http.Request, auth session) {
	data, ok := s.schedulePage(w, r, auth)
	if !ok {
		return
	}
	if r.URL.Query().Get("saved") == "1" {
		data.Notice = "定时计划已保存。"
	}
	s.render(w, http.StatusOK, data)
}

func (s server) schedulePage(w http.ResponseWriter, r *http.Request, auth session) (page, bool) {
	dag := r.PathValue("key")
	version, err := s.db.GetVersion(r.Context(), auth.Scope, dag, 0)
	if err != nil {
		s.readError(w, r, err)
		return page{}, false
	}
	data := page{Title: "定时计划", Section: "schedules", Auth: auth, DAG: dag}
	if key := r.PathValue("schedule"); key != "" {
		current, err := s.db.GetSchedule(r.Context(), auth.Scope, dag, key)
		if err != nil {
			s.readError(w, r, err)
			return page{}, false
		}
		data.Title, data.Section = current.Key, "schedule"
		data.ScheduleEdit = scheduleEditor(&current, version.Revision)
		revision := current.Revision
		if revision == 0 {
			revision = version.Revision
		}
		data.BackfillDraft = &backfillForm{Revision: strconv.FormatUint(revision, 10)}
		return data, true
	}
	data.Schedules, err = s.db.ListSchedules(r.Context(), auth.Scope, dag, r.URL.Query().Get("after"))
	if err != nil {
		s.actionError(w, r, data, err)
		return page{}, false
	}
	if len(data.Schedules) > store.ConsolePageSize {
		data.Schedules = data.Schedules[:store.ConsolePageSize]
		data.Next = fmt.Sprintf("/workflows/%s/schedules?after=%s", dag, url.QueryEscape(data.Schedules[len(data.Schedules)-1].Key))
	}
	data.ScheduleEdit = scheduleEditor(nil, version.Revision)
	return data, true
}

func (s server) saveSchedule(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	data, ok := s.schedulePage(w, r, auth)
	if !ok {
		return
	}
	schedule := data.ScheduleEdit
	values := r.PostForm
	if schedule.Current == nil {
		schedule.Key = strings.TrimSpace(values.Get("key"))
	}
	schedule.Cron, schedule.Zone, schedule.Misfire = values.Get("cron"), strings.TrimSpace(values.Get("zone")), values.Get("misfire")
	schedule.Grace, schedule.Limit, schedule.Revision = values.Get("grace"), values.Get("limit"), values.Get("revision")
	schedule.Inputs, schedule.Start, schedule.Generation = values.Get("inputs"), values.Get("start_after"), values.Get("generation")
	schedule.Enabled = values.Get("enabled") == "1"
	generation, spec, err := schedule.parameters()
	if err != nil {
		s.actionError(w, r, data, err)
		return
	}
	saved, err := s.db.PutSchedule(r.Context(), auth.Scope, data.DAG, generation, spec)
	var conflict *store.ScheduleConflict
	if errors.As(err, &conflict) {
		data.Problem = fmt.Sprintf("计划已更新到第 %d 版，当前草稿保留。请打开最新配置核对后重新保存。", conflict.Generation)
		s.render(w, http.StatusConflict, data)
		return
	}
	if err != nil {
		s.actionError(w, r, data, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/workflows/%s/schedules/%s?saved=1", data.DAG, saved.Key), http.StatusSeeOther)
}

func (f scheduleForm) parameters() (uint64, store.ScheduleSpec, error) {
	var spec store.ScheduleSpec
	generation, errGeneration := strconv.ParseUint(f.Generation, 10, 64)
	revision, errRevision := strconv.ParseUint(f.Revision, 10, 64)
	grace, errGrace := strconv.Atoi(f.Grace)
	limit, errLimit := strconv.Atoi(f.Limit)
	start, errStart := parseTimeInput(f.Start)
	if errGeneration != nil || errRevision != nil || errGrace != nil || errLimit != nil {
		return 0, spec, errors.New("请输入有效的基础版本、固定版本、宽限秒数和运行上限。")
	}
	if errStart != nil || start == nil {
		return 0, spec, errors.New("请输入有效的起算时间。")
	}
	return generation, store.ScheduleSpec{Key: f.Key, Cron: f.Cron, Timezone: f.Zone, Misfire: f.Misfire,
		GraceSeconds: grace, MaxActiveRuns: limit, Revision: revision, Inputs: json.RawMessage(f.Inputs), Enabled: f.Enabled, StartAfter: *start}, nil
}
