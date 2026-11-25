package console

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/axin2024/workflow-runtime/internal/store"
)

type capacityPage struct {
	Workspace store.Capacity
	Queue     *store.Capacity
	Queues    []store.Capacity
	Draft     *capacityForm
}

type capacityForm struct{ Resource, Maximum, Generation string }

func (s server) capacity(w http.ResponseWriter, r *http.Request, auth session) {
	data, ok := s.capacityPage(w, r, auth, r.URL.Query().Get("queue"))
	if !ok {
		return
	}
	if r.URL.Query().Get("saved") == "1" {
		data.Notice = "执行配额已保存。"
	}
	s.render(w, http.StatusOK, data)
}

func (s server) capacityPage(w http.ResponseWriter, r *http.Request, auth session, queue string) (page, bool) {
	contents := &capacityPage{}
	data := page{Title: "执行配额", Section: "capacity", Auth: auth, Capacity: contents}
	var err error
	contents.Workspace, err = s.db.GetWorkspaceCapacity(r.Context(), auth.Scope)
	if err != nil {
		s.readError(w, r, err)
		return page{}, false
	}
	contents.Queues, err = s.db.ListCapacityQueues(r.Context(), auth.Scope, r.URL.Query().Get("after"))
	if err != nil {
		s.actionError(w, r, data, err)
		return page{}, false
	}
	if queue != "" {
		quota, err := s.db.GetQueueCapacity(r.Context(), auth.Scope, queue)
		if err != nil {
			s.actionError(w, r, data, err)
			return page{}, false
		}
		contents.Queue = &quota
	}
	if len(contents.Queues) > store.ConsolePageSize {
		contents.Queues = contents.Queues[:store.ConsolePageSize]
		values := url.Values{"after": {contents.Queues[len(contents.Queues)-1].Queue}}
		if queue != "" {
			values.Set("queue", queue)
		}
		data.Next = "/capacity?" + values.Encode()
	}
	return data, true
}

func (s server) saveCapacity(w http.ResponseWriter, r *http.Request, auth session) {
	if !requireRole(w, auth.CanOperate()) {
		return
	}
	resource := r.PathValue("resource")
	queue := ""
	if resource == "queue" {
		queue = strings.TrimSpace(r.PostForm.Get("queue"))
		if queue == "" {
			http.Error(w, "请指定队列键。", http.StatusBadRequest)
			return
		}
	} else if resource != "workspace" {
		http.NotFound(w, r)
		return
	}
	data, ok := s.capacityPage(w, r, auth, queue)
	if !ok {
		return
	}
	form := capacityForm{Resource: resource, Maximum: strings.TrimSpace(r.PostForm.Get("maximum")), Generation: r.PostForm.Get("generation")}
	data.Capacity.Draft = &form
	generation, maximum, err := form.parameters()
	if err == nil && resource == "workspace" {
		_, err = s.db.PutWorkspaceCapacity(r.Context(), auth.Scope, generation, maximum)
	}
	if err == nil && resource == "queue" {
		// Pool assignments are controlled by deployment operators. Retain the
		// current assignment rather than accepting a client-supplied pool.
		spec := store.QueueCapacitySpec{MaxActive: maximum, Pool: data.Capacity.Queue.Pool}
		_, err = s.db.PutQueueCapacity(r.Context(), auth.Scope, queue, generation, spec)
	}
	if err != nil {
		if errors.Is(err, store.ErrCapacityConflict) {
			s.renderCapacityConflict(w, r, auth, queue, form)
			return
		}
		s.actionError(w, r, data, err)
		return
	}
	path := "/capacity?saved=1"
	if queue != "" {
		path += "&queue=" + url.QueryEscape(queue)
	}
	http.Redirect(w, r, path, http.StatusSeeOther)
}

func (s server) renderCapacityConflict(w http.ResponseWriter, r *http.Request, auth session, queue string, form capacityForm) {
	data, ok := s.capacityPage(w, r, auth, queue)
	if !ok {
		return
	}
	current := data.Capacity.Workspace
	if form.Resource == "queue" {
		current = *data.Capacity.Queue
	}
	data.Problem = fmt.Sprintf("配额未保存。当前版本 %d，正在占用 %d 个槽位，当前上限为 %s。请核对后调整并重试。",
		current.Generation, current.Active, quotaLabel(current.MaxActive))
	form.Generation = strconv.FormatUint(current.Generation, 10)
	data.Capacity.Draft = &form
	s.render(w, http.StatusConflict, data)
}

func (f capacityForm) parameters() (uint64, *int, error) {
	generation, err := strconv.ParseUint(f.Generation, 10, 64)
	if err != nil {
		return 0, nil, errors.New("配额版本必须为非负整数。")
	}
	if f.Maximum == "" || f.Maximum == "unlimited" {
		return generation, nil, nil
	}
	maximum, err := strconv.Atoi(f.Maximum)
	if err != nil || maximum < 0 || maximum > 10000 {
		return 0, nil, errors.New("并发上限必须是 0 到 10000，留空表示不限。")
	}
	return generation, &maximum, nil
}

func quotaLabel(limit *int) string {
	if limit == nil {
		return "不限"
	}
	return strconv.Itoa(*limit)
}

func limitInput(limit *int) string {
	if limit == nil {
		return ""
	}
	return strconv.Itoa(*limit)
}
