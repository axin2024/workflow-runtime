package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/axin2024/workflow-runtime/internal/store"
)

type backfillForm struct{ Revision, From, Through, Inputs string }

func (s server) backfills(w http.ResponseWriter, r *http.Request, auth session) {
	data := page{Title: "历史补跑", Section: "backfills", Auth: auth}
	var before int64
	var err error
	if cursor := r.URL.Query().Get("before"); cursor != "" {
		before, err = positiveID(cursor)
		if err != nil {
			s.actionError(w, r, data, err)
			return
		}
	}
	data.Backfills, err = s.db.ListBackfillBatches(r.Context(), auth.Scope, before)
	if err != nil {
		s.readError(w, r, err)
		return
	}
	if len(data.Backfills) > store.ConsolePageSize {
		data.Backfills = data.Backfills[:store.ConsolePageSize]
		data.Next = fmt.Sprintf("/backfills?before=%d", data.Backfills[len(data.Backfills)-1].ID)
	}
	s.render(w, http.StatusOK, data)
}

func (s server) previewBackfill(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	data, ok := s.schedulePage(w, r, auth)
	if !ok {
		return
	}
	form := r.PostForm
	data.BackfillDraft = &backfillForm{Revision: form.Get("revision"), From: form.Get("from"), Through: form.Get("through"), Inputs: form.Get("inputs")}
	spec, err := data.BackfillDraft.parameters(data.ScheduleEdit.Key)
	if err != nil {
		s.actionError(w, r, data, err)
		return
	}
	batch, err := s.db.PreviewBackfill(r.Context(), auth.Scope, data.DAG, spec)
	if err != nil {
		s.actionError(w, r, data, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/backfills/%d", batch.ID), http.StatusSeeOther)
}

func (f backfillForm) parameters(schedule string) (store.BackfillSpec, error) {
	var spec store.BackfillSpec
	revision, err := strconv.ParseUint(f.Revision, 10, 64)
	if err != nil || revision == 0 {
		return spec, errors.New("补跑需要指定一个已发布的 DAG 版本。")
	}
	from, errFrom := parseTimeInput(f.From)
	through, errThrough := parseTimeInput(f.Through)
	if errFrom != nil || errThrough != nil || from == nil || through == nil {
		return spec, errors.New("请填写有效的补跑起止时间。")
	}
	return store.BackfillSpec{Schedule: schedule, Revision: revision, From: *from, Through: *through,
		Inputs: json.RawMessage(strings.TrimSpace(f.Inputs))}, nil
}

func (s server) backfill(w http.ResponseWriter, r *http.Request, auth session) {
	data, ok := s.backfillPage(w, r, auth)
	if !ok {
		return
	}
	if r.URL.Query().Get("updated") == "1" {
		data.Notice = "批次回执已更新。"
	}
	s.render(w, http.StatusOK, data)
}

func (s server) exportBackfill(w http.ResponseWriter, r *http.Request, auth session) {
	id, err := positiveID(r.PathValue("id"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	report, err := s.db.ExportBackfill(r.Context(), auth.Scope, id)
	if err != nil {
		s.readError(w, r, err)
		return
	}
	s.writeDownload(w, fmt.Sprintf("backfill-%d.json", id), report)
}

func (s server) backfillPage(w http.ResponseWriter, r *http.Request, auth session) (page, bool) {
	id, err := positiveID(r.PathValue("id"))
	if err != nil {
		http.NotFound(w, r)
		return page{}, false
	}
	batch, err := s.db.GetBackfill(r.Context(), auth.Scope, id)
	if err != nil {
		s.readError(w, r, err)
		return page{}, false
	}
	return page{Title: fmt.Sprintf("补跑批次 #%d", id), Section: "backfill", Auth: auth, Batch: &batch}, true
}

func (s server) backfillAction(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	id, err := positiveID(r.PathValue("id"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	var selection []int
	switch r.PathValue("action") {
	case "confirm":
		values := r.PostForm["ordinals"]
		if len(values) == 0 || len(values) > 128 {
			err = errors.New("请至少选择一个可补跑时刻，且最多选择 128 个。")
		}
		for _, value := range values {
			if err != nil {
				break
			}
			ordinal, parseErr := strconv.Atoi(value)
			if parseErr != nil || ordinal <= 0 {
				err = errors.New("候选序号无效。")
				break
			}
			selection = append(selection, ordinal)
		}
		if err == nil {
			_, err = s.db.ConfirmBackfill(r.Context(), auth.Scope, id, selection)
		}
	case "resume":
		_, err = s.db.ResumeBackfill(r.Context(), auth.Scope, id)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		data, ok := s.backfillPage(w, r, auth)
		if ok {
			data.Selected = selection
			s.actionError(w, r, data, err)
		}
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/backfills/%d?updated=1", id), http.StatusSeeOther)
}

func backfillState(state string) string {
	switch state {
	case "draft":
		return "待确认"
	case "confirmed":
		return "处理中"
	case "completed":
		return "已登记"
	case "available":
		return "可选"
	case "already_registered":
		return "已有运行"
	case "excluded":
		return "未选"
	case "pending":
		return "待登记"
	case "created":
		return "已创建"
	case "reused":
		return "已复用"
	default:
		return state
	}
}
