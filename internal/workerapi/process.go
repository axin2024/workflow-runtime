package workerapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
)

type processConnectorBackend interface {
	AuthenticateProcessConnector(context.Context, string) (store.ProcessConnector, error)
	ConnectorProcessSignal(context.Context, store.ProcessConnector, int64, string, store.ProcessSignalInput) (store.SignalReceipt, error)
	ConnectorSignalReceipt(context.Context, store.ProcessConnector, int64, string) (store.SignalReceipt, error)
	ClaimProcessAction(context.Context, store.Scope, string, string, time.Duration) (store.ProcessAction, error)
	ReportProcessAction(context.Context, store.Scope, string, string, int64, string, store.ProcessActionReport) (store.ProcessAction, error)
}

func registerProcessConnectors(mux *http.ServeMux, backend processConnectorBackend, logger *log.Logger) {
	protect := func(next func(http.ResponseWriter, *http.Request, store.ProcessConnector) error) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			value := strings.Fields(r.Header.Get("Authorization"))
			if len(value) != 2 || !strings.EqualFold(value[0], "Bearer") {
				writeFailure(w, http.StatusUnauthorized, "unauthorized", store.ErrConnectorUnauthorized)
				return
			}
			connector, err := backend.AuthenticateProcessConnector(r.Context(), value[1])
			if err == nil {
				err = next(w, r, connector)
			}
			if err != nil {
				writeProcessConnectorError(w, err, logger)
			}
		}
	}
	mux.HandleFunc("POST /process-runs/{id}/signals", protect(func(w http.ResponseWriter, r *http.Request, connector store.ProcessConnector) error {
		id, err := processPathID(r)
		if err != nil {
			return err
		}
		var input store.ProcessSignalInput
		if err = decodeProcessRequest(w, r, &input); err != nil {
			return err
		}
		receipt, err := backend.ConnectorProcessSignal(r.Context(), connector, id, r.Header.Get("Idempotency-Key"), input)
		if err != nil {
			return err
		}
		status := http.StatusOK
		if receipt.Outcome == "waiting_for_match" {
			status = http.StatusAccepted
			w.Header().Set("Location", fmt.Sprintf("/process-runs/%d/signals/%s", id, url.PathEscape(receipt.Event)))
		}
		writeResponse(w, status, receipt)
		return nil
	}))
	mux.HandleFunc("GET /process-runs/{id}/signals/{event}", protect(func(w http.ResponseWriter, r *http.Request, connector store.ProcessConnector) error {
		id, err := processPathID(r)
		if err != nil {
			return err
		}
		receipt, err := backend.ConnectorSignalReceipt(r.Context(), connector, id, r.PathValue("event"))
		if err != nil {
			return err
		}
		writeResponse(w, http.StatusOK, receipt)
		return nil
	}))
	mux.HandleFunc("POST /process-actions/claim", protect(func(w http.ResponseWriter, r *http.Request, connector store.ProcessConnector) error {
		if connector.Operation == "" {
			return store.ErrNotFound
		}
		var input struct {
			Seconds int `json:"lease_seconds"`
		}
		if err := decodeProcessRequest(w, r, &input); err != nil {
			return err
		}
		if input.Seconds < 1 || input.Seconds > 3600 {
			return &store.ProcessFieldError{Field: "lease_seconds"}
		}
		action, err := backend.ClaimProcessAction(r.Context(), connector.Scope, connector.Process, connector.Operation, time.Duration(input.Seconds)*time.Second)
		if errors.Is(err, store.ErrNotFound) {
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
		if err != nil {
			return err
		}
		writeResponse(w, http.StatusOK, action)
		return nil
	}))
	mux.HandleFunc("POST /process-actions/{id}/receipt", protect(func(w http.ResponseWriter, r *http.Request, connector store.ProcessConnector) error {
		if connector.Operation == "" {
			return store.ErrNotFound
		}
		id, err := processPathID(r)
		if err != nil {
			return err
		}
		var input store.ProcessActionReport
		if err = decodeProcessRequest(w, r, &input); err != nil {
			return err
		}
		if r.Header.Get("Idempotency-Key") == "" {
			return &store.ProcessFieldError{Field: "headers.Idempotency-Key"}
		}
		result, err := backend.ReportProcessAction(r.Context(), connector.Scope, connector.Process, connector.Operation, id, r.Header.Get("Idempotency-Key"), input)
		if err != nil {
			return err
		}
		writeResponse(w, http.StatusOK, result)
		return nil
	}))
}

func processPathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, store.ErrNotFound
	}
	return id, nil
}

func decodeProcessRequest[T any](w http.ResponseWriter, r *http.Request, target *T) error {
	r.Body = http.MaxBytesReader(w, r.Body, 128<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input *T
	if err := decoder.Decode(&input); err != nil || input == nil || decoder.Decode(new(any)) != io.EOF {
		return &store.ProcessFieldError{Field: "body"}
	}
	*target = *input
	return nil
}

func writeProcessConnectorError(w http.ResponseWriter, err error, logger *log.Logger) {
	var field *store.ProcessFieldError
	switch {
	case errors.Is(err, store.ErrConnectorUnauthorized):
		writeFailure(w, http.StatusUnauthorized, "unauthorized", err)
	case errors.Is(err, store.ErrNotFound):
		writeFailure(w, http.StatusNotFound, "not_found", err)
	case errors.Is(err, store.ErrProcessConflict):
		writeFailure(w, http.StatusConflict, "process_conflict", err)
	case errors.As(err, &field):
		writeResponse(w, http.StatusUnprocessableEntity, map[string]string{"code": "invalid_input", "field": field.Field, "message": err.Error()})
	case errors.Is(err, store.ErrInvalidProcess):
		writeResponse(w, http.StatusUnprocessableEntity, map[string]string{"code": "invalid_input", "field": "body", "message": err.Error()})
	default:
		logger.Printf("process connector: %v", err)
		writeFailure(w, http.StatusInternalServerError, "internal_error", errors.New("process operation could not be completed"))
	}
}
