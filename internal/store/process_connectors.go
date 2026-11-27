package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/axin2024/workflow-runtime/internal/process"
)

var ErrConnectorUnauthorized = errors.New("invalid process connector credential")

type ProcessConnector struct {
	Scope
	Process   string `json:"process_key"`
	Operation string `json:"operation,omitempty"`
}

type ProcessConnectorGrant struct {
	ProcessConnector
	Token string `json:"token"`
}

func (s *Store) GrantProcessConnector(ctx context.Context, identity ProcessConnector) (ProcessConnectorGrant, error) {
	if identity.Scope.validate() != nil || !process.ValidKey(identity.Process) || (identity.Operation != "" && !process.ValidKey(identity.Operation)) {
		return ProcessConnectorGrant{}, ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ProcessConnectorGrant{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, identity.Workspace)
	if err != nil {
		return ProcessConnectorGrant{}, err
	}
	version, err := loadProcessVersion(ctx, tx, workspace, identity.Process, 0)
	if err != nil {
		return ProcessConnectorGrant{}, err
	}
	token, digest := newToken()
	_, err = tx.ExecContext(ctx, `INSERT INTO process_connector_grants (workspace_id,process_id,actor,operation_name,token_digest)
		VALUES (?,?,?,?,?) AS incoming ON DUPLICATE KEY UPDATE token_digest=incoming.token_digest,granted_at=UTC_TIMESTAMP(6)`,
		workspace, version.ID, identity.Actor, identity.Operation, digest[:])
	if err != nil {
		return ProcessConnectorGrant{}, err
	}
	if err = tx.Commit(); err != nil {
		return ProcessConnectorGrant{}, err
	}
	return ProcessConnectorGrant{ProcessConnector: identity, Token: token}, nil
}

func (s *Store) RevokeProcessConnector(ctx context.Context, identity ProcessConnector) error {
	if identity.Scope.validate() != nil || !process.ValidKey(identity.Process) || (identity.Operation != "" && !process.ValidKey(identity.Operation)) {
		return ErrInvalidProcess
	}
	_, err := s.db.ExecContext(ctx, `DELETE c FROM process_connector_grants c JOIN workspaces w ON w.id=c.workspace_id
		JOIN process_names p ON p.workspace_id=c.workspace_id AND p.id=c.process_id
		WHERE w.workspace_key=? AND p.process_key=? AND c.actor=? AND c.operation_name=?`,
		identity.Workspace, identity.Process, identity.Actor, identity.Operation)
	return err
}

func (s *Store) AuthenticateProcessConnector(ctx context.Context, token string) (ProcessConnector, error) {
	var out ProcessConnector
	digest, err := consoleDigest(token)
	if err != nil {
		return out, ErrConnectorUnauthorized
	}
	err = s.db.QueryRowContext(ctx, `SELECT w.workspace_key,c.actor,p.process_key,c.operation_name FROM process_connector_grants c
		JOIN workspaces w ON w.id=c.workspace_id JOIN process_names p ON p.workspace_id=c.workspace_id AND p.id=c.process_id
		WHERE c.token_digest=?`, digest[:]).Scan(&out.Workspace, &out.Actor, &out.Process, &out.Operation)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrConnectorUnauthorized
	}
	return out, err
}

func (s *Store) ConnectorProcessSignal(ctx context.Context, connector ProcessConnector, run int64, request string, input ProcessSignalInput) (SignalReceipt, error) {
	if connector.Operation != "" {
		return SignalReceipt{}, ErrNotFound
	}
	return s.submitProcessSignal(ctx, connector.Scope, connector.Process, run, request, input)
}

func (s *Store) ConnectorSignalReceipt(ctx context.Context, connector ProcessConnector, id int64, event string) (SignalReceipt, error) {
	if connector.Operation != "" {
		return SignalReceipt{}, ErrNotFound
	}
	return s.ProcessSignalReceipt(ctx, connector.Scope, connector.Process, id, event)
}

func (s *Store) ProcessSignalReceipt(ctx context.Context, scope Scope, processKey string, id int64, event string) (SignalReceipt, error) {
	var out SignalReceipt
	if scope.validate() != nil || id <= 0 || !processReference.MatchString(event) {
		return out, ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return out, err
	}
	run, err := loadProcessRun(ctx, tx, workspace, id, false)
	if err != nil {
		return out, err
	}
	if processKey != "" && run.Key != processKey {
		return out, ErrNotFound
	}
	out, err = processSignalByEvent(ctx, tx, run, event)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}
