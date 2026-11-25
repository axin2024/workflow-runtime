package workerapi

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workflow"
)

const triggersPath = "/v1/triggers"

type triggerProblem struct {
	failure
	Field string `json:"field,omitempty"`
	RunID int64  `json:"run_id,omitempty"`
}

func registerTriggers(mux *http.ServeMux, backend Backend, logger *log.Logger) {
	mux.HandleFunc("POST "+triggersPath, triggerRequest(logger, func(w http.ResponseWriter, r *http.Request, token string) error {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeFailure(w, http.StatusUnsupportedMediaType, "unsupported_media_type", errors.New("expected application/json"))
			return nil
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var request *store.ExternalTriggerRequest
		if err := decoder.Decode(&request); err != nil || request == nil || decoder.Decode(new(any)) != io.EOF {
			return &workflow.Issue{Path: "body", Message: "expected one trigger JSON object"}
		}
		result, err := backend.ExternalTrigger(r.Context(), token, *request)
		if err != nil {
			return err
		}
		w.Header().Set("Location", triggersPath+"/"+strconv.FormatInt(result.Run.ID, 10))
		status := http.StatusAccepted
		if result.Replayed {
			status = http.StatusOK
		}
		writeResponse(w, status, result)
		return nil
	}))
	get := triggerRequest(logger, func(w http.ResponseWriter, r *http.Request, token string) error {
		var id int64
		key := r.URL.Query().Get("key")
		if value := r.PathValue("run"); value != "" {
			var err error
			id, err = strconv.ParseInt(value, 10, 64)
			if err != nil || id <= 0 {
				return &workflow.Issue{Path: "run", Message: "expected a positive run ID"}
			}
		}
		receipt, err := backend.ExternalReceipt(r.Context(), token, id, key)
		if err != nil {
			return err
		}
		writeResponse(w, http.StatusOK, receipt)
		return nil
	})
	mux.HandleFunc("GET "+triggersPath, get)
	mux.HandleFunc("GET "+triggersPath+"/{run}", get)
}

func triggerRequest(logger *log.Logger, invoke func(http.ResponseWriter, *http.Request, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		authorization := strings.Fields(r.Header.Get("Authorization"))
		if len(authorization) != 2 || !strings.EqualFold(authorization[0], "Bearer") {
			writeFailure(w, http.StatusUnauthorized, "unauthorized", store.ErrTriggerUnauthorized)
			return
		}
		err := invoke(w, r, authorization[1])
		var issue *workflow.Issue
		var conflict *store.TriggerConflict
		switch {
		case err == nil:
		case errors.As(err, &issue):
			writeResponse(w, http.StatusBadRequest, triggerProblem{failure: failure{"invalid_request", issue.Message}, Field: issue.Path})
		case errors.As(err, &conflict):
			writeResponse(w, http.StatusConflict, triggerProblem{failure: failure{"trigger_conflict", err.Error()}, RunID: conflict.RunID})
		case errors.Is(err, store.ErrTriggerKeyUnavailable):
			writeFailure(w, http.StatusConflict, "trigger_conflict", err)
		case errors.Is(err, store.ErrTriggerUnauthorized):
			writeFailure(w, http.StatusUnauthorized, "unauthorized", err)
		case errors.Is(err, store.ErrNotFound):
			writeFailure(w, http.StatusNotFound, "not_found", errors.New("trigger resource not found"))
		default:
			logger.Printf("trigger API %s: %v", r.Method, err)
			writeFailure(w, http.StatusInternalServerError, "internal_error", errors.New("trigger operation could not be completed"))
		}
	}
}
