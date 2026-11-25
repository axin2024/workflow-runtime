package workerapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
)

type Client struct {
	endpoint string
	token    string
	http     *http.Client
}

func NewClient(endpoint, token string) (*Client, error) {
	base, err := url.Parse(endpoint)
	if err != nil || base.Hostname() == "" || base.User != nil || base.RawQuery != "" || base.ForceQuery || base.Fragment != "" ||
		base.RawPath != "" || (base.Path != "" && base.Path != "/") ||
		(base.Scheme != "https" && !(base.Scheme == "http" && IsLoopback(base.Hostname()))) {
		return nil, errors.New("worker endpoint must be an HTTPS origin or a local HTTP origin")
	}
	if token == "" {
		return nil, errors.New("WORKFLOW_WORKER_TOKEN is required")
	}
	base.Path = prefix
	return &Client{endpoint: base.String(), token: token, http: &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}, nil
}

func IsLoopback(host string) bool { return host == "localhost" || net.ParseIP(host).IsLoopback() }

func (c *Client) Claim(ctx context.Context, request store.ClaimRequest) (*store.ClaimedTask, error) {
	var result claimResponse
	err := c.post(ctx, "claim", request, &result)
	return result.Task, err
}

func (c *Client) Renew(ctx context.Context, request store.RenewRequest) (store.LeaseRenewal, error) {
	var result store.LeaseRenewal
	err := c.post(ctx, "renew", request, &result)
	return result, err
}

func (c *Client) Complete(ctx context.Context, request store.CompletionRequest) (store.CompletionReceipt, error) {
	var result store.CompletionReceipt
	err := c.post(ctx, "complete", request, &result)
	return result, err
}

var ErrTemporary = errors.New("worker API temporarily unavailable")

func (c *Client) post(ctx context.Context, resource string, value, result any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+resource, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrTemporary, err)
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxBodyBytes))
	if response.StatusCode != http.StatusOK {
		if response.StatusCode >= 500 {
			return fmt.Errorf("%w: %s", ErrTemporary, response.Status)
		}
		var problem failure
		if err := decoder.Decode(&problem); err != nil {
			return fmt.Errorf("worker API returned %s", response.Status)
		}
		switch problem.Code {
		case "unauthorized":
			return store.ErrWorkerUnauthorized
		case "lease_lost":
			return store.ErrLeaseLost
		case "completion_conflict":
			return store.ErrCompletionConflict
		case "invalid_request":
			return fmt.Errorf("%w: %s", store.ErrInvalidWorkerRequest, problem.Message)
		default:
			return fmt.Errorf("worker API returned %s", response.Status)
		}
	}
	if err := decoder.Decode(result); err != nil {
		return fmt.Errorf("%w: invalid response: %w", ErrTemporary, err)
	}
	return nil
}
