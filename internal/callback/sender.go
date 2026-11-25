package callback

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
)

type Sender struct {
	endpoint string
	key      []byte
	client   *http.Client
}

func NewSender(endpoint, secret string) (*Sender, error) {
	target, err := url.Parse(endpoint)
	if err != nil || target.Hostname() == "" || target.User != nil || target.Fragment != "" {
		return nil, errors.New("invalid callback URL")
	}
	host, _ := netip.ParseAddr(target.Hostname())
	if target.Scheme != "https" && !(target.Scheme == "http" && host.IsLoopback()) {
		return nil, errors.New("callback URL requires HTTPS or a loopback HTTP address")
	}
	key, err := hex.DecodeString(secret)
	if err != nil || len(key) != 32 {
		return nil, errors.New("WORKFLOW_CALLBACK_SECRET must contain 32 hexadecimal-encoded bytes")
	}
	return &Sender{endpoint: target.String(), key: key, client: &http.Client{
		Timeout: 8 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}, nil
}

// Send returns the HTTP status, or zero when no valid HTTP response was received.
func (s *Sender) Send(ctx context.Context, event store.CallbackEvent) (int, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("create callback request")
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(timestamp + "." + event.ID + "."))
	mac.Write(body)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Workflow-Event-ID", event.ID)
	request.Header.Set("Workflow-Timestamp", timestamp)
	request.Header.Set("Workflow-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	response, err := s.client.Do(request)
	if err != nil {
		// A lost response may follow a successful receiver-side commit. The outbox retries the same ID.
		return 0, nil
	}
	defer response.Body.Close()
	if response.StatusCode < 100 || response.StatusCode > 599 {
		return 0, nil
	}
	return response.StatusCode, nil
}
