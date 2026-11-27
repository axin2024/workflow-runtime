package console

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workflow"
)

//go:embed assets/*
var files embed.FS

type server struct {
	db           *store.Store
	logger       *log.Logger
	templates    *template.Template
	cookieName   string
	secureCookie bool
}

type session struct {
	store.ConsoleIdentity
	Token string
	CSRF  string
}

func (s session) CanAuthor() bool  { return s.Role == store.ConsoleAuthor || s.CanOperate() }
func (s session) CanOperate() bool { return s.Role == store.ConsoleOperator }

func Handler(db *store.Store, logger *log.Logger, secureCookie bool) (http.Handler, error) {
	templates, err := template.New("layout").Funcs(template.FuncMap{
		"json": prettyJSON, "date": date, "optionalDate": optionalDate,
		"status": stateLabel, "deref": deref[string], "duration": duration, "elapsed": elapsed,
		"short":     shorten,
		"timestamp": timestamp, "backfillState": backfillState, "quota": quotaLabel, "limitInput": limitInput,
		"selected": slices.Contains[[]int, int],
		"claimable": func(task store.ProcessTask) bool {
			return task.Status == "pending" || (task.ClaimUntil != nil && !time.Now().Before(*task.ClaimUntil))
		},
	}).ParseFS(files, "assets/*.html")
	if err != nil {
		return nil, err
	}
	name := "workflow_session"
	if secureCookie {
		name = "__Host-workflow_session"
	}
	s := server{db: db, logger: logger, templates: templates, cookieName: name, secureCookie: secureCookie}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /assets/{file}", s.asset)
	mux.HandleFunc("GET /login", s.loginPage)
	mux.HandleFunc("POST /login", s.login)
	mux.HandleFunc("POST /logout", s.protect(s.logout))
	mux.HandleFunc("GET /{$}", s.protect(s.runs))
	mux.HandleFunc("GET /runs/report.json", s.protect(s.exportRunList))
	mux.HandleFunc("GET /runs/{id}", s.protect(s.run))
	mux.HandleFunc("GET /runs/{id}/report.json", s.protect(s.export))
	mux.HandleFunc("GET /runs/{id}/compare", s.protect(s.compare))
	mux.HandleFunc("POST /runs/{id}/{action}", s.protect(s.runAction))
	mux.HandleFunc("GET /workflows", s.protect(s.workflows))
	mux.HandleFunc("GET /workflows/{key}", s.protect(s.workflow))
	mux.HandleFunc("GET /editor", s.protect(s.editor))
	mux.HandleFunc("POST /editor", s.protect(s.edit))
	mux.HandleFunc("POST /workflows/{key}/trigger", s.protect(s.trigger))
	mux.HandleFunc("GET /workflows/{key}/schedules", s.protect(s.schedules))
	mux.HandleFunc("POST /workflows/{key}/schedules", s.protect(s.saveSchedule))
	mux.HandleFunc("GET /workflows/{key}/schedules/{schedule}", s.protect(s.schedule))
	mux.HandleFunc("POST /workflows/{key}/schedules/{schedule}", s.protect(s.saveSchedule))
	mux.HandleFunc("POST /workflows/{key}/schedules/{schedule}/backfill", s.protect(s.previewBackfill))
	mux.HandleFunc("GET /backfills", s.protect(s.backfills))
	mux.HandleFunc("GET /backfills/{id}", s.protect(s.backfill))
	mux.HandleFunc("GET /backfills/{id}/report.json", s.protect(s.exportBackfill))
	mux.HandleFunc("POST /backfills/{id}/{action}", s.protect(s.backfillAction))
	mux.HandleFunc("GET /capacity", s.protect(s.capacity))
	mux.HandleFunc("POST /capacity/{resource}", s.protect(s.saveCapacity))
	mux.HandleFunc("GET /retention", s.protect(s.retention))
	mux.HandleFunc("GET /retention/report.json", s.protect(s.retentionExport))
	mux.HandleFunc("POST /retention/policy", s.protect(s.retentionPolicy))
	mux.HandleFunc("POST /retention/cleanup", s.protect(s.retentionCleanup))
	s.registerProcessPages(mux)
	protected := http.NewCrossOriginProtection().Handler(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; script-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		protected.ServeHTTP(w, r)
	}), nil
}

func (s server) asset(w http.ResponseWriter, r *http.Request) {
	file := r.PathValue("file")
	switch file {
	case "console.css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case "console.js":
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	default:
		http.NotFound(w, r)
		return
	}
	content, err := fs.ReadFile(files, "assets/"+file)
	if err != nil {
		s.internalError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=300")
	w.Write(content)
}

func (s server) protect(next func(http.ResponseWriter, *http.Request, session)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(s.cookieName)
		if err != nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		identity, err := s.db.ConsoleIdentity(r.Context(), cookie.Value)
		if errors.Is(err, store.ErrConsoleUnauthorized) {
			s.setCookie(w, "", -1)
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		if err != nil {
			s.internalError(w, err)
			return
		}
		digest := sha256.Sum256([]byte("workflow-console/csrf/" + cookie.Value))
		csrf := base64.RawURLEncoding.EncodeToString(digest[:])
		if r.Method == http.MethodPost {
			if !parseForm(w, r, 3*workflow.MaxDefinitionBytes+8192) {
				return
			}
			if subtle.ConstantTimeCompare([]byte(r.PostForm.Get("csrf")), []byte(csrf)) != 1 {
				http.Error(w, "会话表单已失效，请刷新后重试。", http.StatusForbidden)
				return
			}
		}
		next(w, r, session{ConsoleIdentity: identity, Token: cookie.Value, CSRF: csrf})
	}
}

func (s server) loginPage(w http.ResponseWriter, _ *http.Request) {
	s.render(w, http.StatusOK, page{Title: "登录", Section: "login"})
}

func (s server) login(w http.ResponseWriter, r *http.Request) {
	if !parseForm(w, r, 4096) {
		return
	}
	token, err := s.db.StartConsoleSession(r.Context(), strings.TrimSpace(r.PostForm.Get("credential")))
	if errors.Is(err, store.ErrConsoleUnauthorized) {
		s.render(w, http.StatusUnauthorized, page{Title: "登录", Section: "login", Problem: "凭据无效，请检查后重试。"})
		return
	}
	if err != nil {
		s.internalError(w, err)
		return
	}
	if previous, err := r.Cookie(s.cookieName); err == nil {
		if err := s.db.EndConsoleSession(r.Context(), previous.Value); err != nil && !errors.Is(err, store.ErrConsoleUnauthorized) {
			s.logger.Printf("replace console session: %v", err)
		}
	}
	s.setCookie(w, token, int(store.ConsoleSessionLifetime.Seconds()))
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s server) logout(w http.ResponseWriter, r *http.Request, auth session) {
	if err := s.db.EndConsoleSession(r.Context(), auth.Token); err != nil {
		s.internalError(w, err)
		return
	}
	s.setCookie(w, "", -1)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s server) setCookie(w http.ResponseWriter, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{Name: s.cookieName, Value: value, Path: "/", HttpOnly: true,
		Secure: s.secureCookie, SameSite: http.SameSiteStrictMode, MaxAge: maxAge})
}

func parseForm(w http.ResponseWriter, r *http.Request, limit int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	if err := r.ParseForm(); err != nil {
		var size *http.MaxBytesError
		if errors.As(err, &size) {
			http.Error(w, "表单超过允许大小。", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "表单格式无效。", http.StatusBadRequest)
		}
		return false
	}
	return true
}

func requireRole(w http.ResponseWriter, allowed bool) bool {
	if !allowed {
		http.Error(w, "当前身份无权执行此操作。", http.StatusForbidden)
	}
	return allowed
}

func (s server) render(w http.ResponseWriter, status int, data page) {
	var content bytes.Buffer
	if err := s.templates.ExecuteTemplate(&content, "layout", data); err != nil {
		s.internalError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	w.Write(content.Bytes())
}

func (s server) internalError(w http.ResponseWriter, err error) {
	s.logger.Printf("console request: %v", err)
	http.Error(w, "工作台暂时无法取回结果，请稍后重试。", http.StatusInternalServerError)
}

func (s server) readError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	s.internalError(w, err)
}

func prettyJSON(value any) string {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return ""
	}
	return string(data)
}

func date(value time.Time) string { return value.UTC().Format("2006-01-02 15:04:05") }

func optionalDate(value *time.Time) string {
	if value == nil {
		return "—"
	}
	return date(*value)
}

func deref[T any](p *T) T {
	if p != nil {
		return *p
	}
	var zero T
	return zero
}

func duration(ms *int64) string {
	if ms == nil {
		return "—"
	}
	return (time.Duration(*ms) * time.Millisecond).String()
}

func elapsed(start time.Time, finish *time.Time) string {
	if finish == nil {
		return "—"
	}
	ms := finish.Sub(start).Milliseconds()
	return duration(&ms)
}

func stateLabel(state string) string {
	switch state {
	case "queued":
		return "待执行"
	case "waiting":
		return "等待依赖"
	case "ready":
		return "可领取"
	case "running":
		return "运行中"
	case "succeeded":
		return "成功"
	case "failed":
		return "失败"
	case "retry_wait":
		return "等待重试"
	case "canceling":
		return "取消中"
	case "canceled":
		return "已取消"
	case "skipped":
		return "已跳过"
	case "action_required":
		return "待核对"
	case "expired":
		return "租约过期"
	default:
		return state
	}
}

func positiveID(value string) (int64, error) {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("编号必须是正整数")
	}
	return id, nil
}
