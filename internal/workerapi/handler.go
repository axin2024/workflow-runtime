package workerapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/axin2024/workflow-runtime/internal/store"
)

const (
	prefix       = "/v1/worker/"
	maxBodyBytes = 2 << 20
)

type Backend interface {
	Claim(context.Context, string, store.ClaimRequest) (*store.ClaimedTask, error)
	Renew(context.Context, string, store.RenewRequest) (store.LeaseRenewal, error)
	Complete(context.Context, string, store.CompletionRequest) (store.CompletionReceipt, error)
	AcknowledgeCancel(context.Context, string, store.LeaseRef) (store.CompletionReceipt, error)
}

type claimResponse struct {
	Task *store.ClaimedTask `json:"task"`
}

type failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func Handler(backend Backend, logger *log.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+prefix+"claim", operation(logger, func(ctx context.Context, token string, request store.ClaimRequest) (claimResponse, error) {
		task, err := backend.Claim(ctx, token, request)
		return claimResponse{Task: task}, err
	}))
	mux.HandleFunc("POST "+prefix+"renew", operation(logger, backend.Renew))
	mux.HandleFunc("POST "+prefix+"complete", operation(logger, backend.Complete))
	mux.HandleFunc("POST "+prefix+"cancel-ack", operation(logger, backend.AcknowledgeCancel))
	return mux
}

func operation[Request, Result any](logger *log.Logger, invoke func(context.Context, string, Request) (Result, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		authorization := strings.Fields(r.Header.Get("Authorization"))
		if len(authorization) != 2 || !strings.EqualFold(authorization[0], "Bearer") {
			writeFailure(w, http.StatusUnauthorized, "unauthorized", store.ErrWorkerUnauthorized)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var request *Request
		if err := decoder.Decode(&request); err != nil || request == nil || decoder.Decode(new(any)) != io.EOF {
			writeFailure(w, http.StatusBadRequest, "invalid_request", errors.New("body must be one supported JSON object"))
			return
		}
		result, err := invoke(r.Context(), authorization[1], *request)
		if err == nil {
			writeResponse(w, http.StatusOK, result)
			return
		}
		switch {
		case errors.Is(err, store.ErrInvalidWorkerRequest):
			writeFailure(w, http.StatusBadRequest, "invalid_request", err)
		case errors.Is(err, store.ErrWorkerUnauthorized):
			writeFailure(w, http.StatusUnauthorized, "unauthorized", err)
		case errors.Is(err, store.ErrLeaseLost):
			writeFailure(w, http.StatusConflict, "lease_lost", err)
		case errors.Is(err, store.ErrCompletionConflict):
			writeFailure(w, http.StatusConflict, "completion_conflict", err)
		case errors.Is(err, store.ErrCancellationRequested):
			writeFailure(w, http.StatusConflict, "cancel_requested", err)
		case errors.Is(err, store.ErrRunControlConflict):
			writeFailure(w, http.StatusConflict, "run_conflict", err)
		default:
			logger.Printf("%s: %v", r.URL.Path, err)
			writeFailure(w, http.StatusInternalServerError, "internal_error", errors.New("worker operation could not be completed"))
		}
	}
}

func writeFailure(w http.ResponseWriter, status int, code string, err error) {
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	writeResponse(w, status, failure{Code: code, Message: err.Error()})
}

func writeResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
