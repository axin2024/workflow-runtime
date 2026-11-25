package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/axin2024/workflow-runtime/internal/callback"
	"github.com/axin2024/workflow-runtime/internal/store"
)

func runCallbackDispatch(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("callback-dispatch", flag.ContinueOnError)
	once := flags.Bool("once", false, "try one delivery and exit")
	interval := flags.Duration("interval", time.Second, "polling interval while idle")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 3 {
		return errors.New("usage: workflow callback-dispatch [--once] [--interval DURATION] WORKSPACE DAG ACTOR")
	}
	if *interval < 250*time.Millisecond || *interval > time.Minute {
		return errors.New("callback polling interval must be between 250ms and 1m")
	}
	sender, err := callback.NewSender(os.Getenv("WORKFLOW_CALLBACK_URL"), os.Getenv("WORKFLOW_CALLBACK_SECRET"))
	if err != nil {
		return err
	}
	db, err := store.Open(os.Getenv("WORKFLOW_MYSQL_DSN"))
	if err != nil {
		return err
	}
	defer db.Close()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = db.Ready(startup)
	cancel()
	if err != nil {
		return err
	}
	scope := store.Scope{Workspace: flags.Arg(0), Actor: flags.Arg(2)}
	dag := flags.Arg(1)
	for ctx.Err() == nil {
		record, err := dispatchCallback(ctx, db, sender, scope, dag)
		switch {
		case ctx.Err() != nil:
			return nil
		case *once && err != nil:
			return err
		case *once:
			return writeJSON(out, record)
		case errors.Is(err, store.ErrCallbackLeaseLost):
			fmt.Fprintln(os.Stderr, err)
		case err != nil:
			return err
		case record != nil:
			if err := writeJSON(out, record); err != nil {
				return err
			}
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(*interval):
		}
	}
	return nil
}

func dispatchCallback(ctx context.Context, db *store.Store, sender *callback.Sender, scope store.Scope, dag string) (*store.CallbackStatus, error) {
	delivery, err := db.ClaimCallback(ctx, scope, dag)
	if err != nil || delivery == nil {
		return nil, err
	}
	status, err := sender.Send(ctx, delivery.Event)
	if err != nil {
		return nil, err
	}
	record, err := db.FinishCallback(ctx, *delivery, status)
	return &record, err
}
