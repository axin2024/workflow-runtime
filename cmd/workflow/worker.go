package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/worker"
	"github.com/axin2024/workflow-runtime/internal/workerapi"
)

func parseWorkerRegistration(args []string) (string, int, []store.WorkerPermission, error) {
	if len(args) < 5 {
		return "", 0, nil, errors.New("usage: workflow worker-register WORKSPACE NAME MAX_ACTIVE QUEUE:TASK [...]")
	}
	maxActive, err := strconv.Atoi(args[3])
	if err != nil {
		return "", 0, nil, errors.New("max active leases must be an integer")
	}
	permissions := make([]store.WorkerPermission, 0, len(args)-4)
	for _, permission := range args[4:] {
		queue, task, ok := strings.Cut(permission, ":")
		if !ok {
			return "", 0, nil, errors.New("permission must have the form QUEUE:TASK")
		}
		permissions = append(permissions, store.WorkerPermission{Queue: queue, Task: task})
	}
	return args[2], maxActive, permissions, nil
}

func runWorkerAPI(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("worker-api", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:8080", "loopback listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || !workerapi.IsLoopback(host) || flags.NArg() != 0 {
		return errors.New("worker-api requires a loopback host and port")
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
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	logger := log.New(os.Stderr, "worker-api ", log.LstdFlags|log.LUTC)
	server := &http.Server{Handler: workerapi.Handler(db, logger), ErrorLog: logger,
		ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	defer server.Close()
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	logger.Printf("listening on %s", listener.Addr())
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(stop)
	}
}

func runWorker(ctx context.Context, args []string, out, diagnostic io.Writer) error {
	endpoint := os.Getenv("WORKFLOW_WORKER_URL")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:8080"
	}
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	base := flags.String("endpoint", endpoint, "worker API origin")
	once := flags.Bool("once", false, "try one claim and exit")
	interval := flags.Duration("interval", time.Second, "polling interval while idle")
	lease := flags.Int("lease-seconds", 15, "lease duration in seconds")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *interval < 100*time.Millisecond || *interval > time.Minute {
		return errors.New("worker polling interval must be between 100ms and 1m")
	}
	client, err := workerapi.NewClient(*base, os.Getenv("WORKFLOW_WORKER_TOKEN"))
	if err != nil {
		return err
	}
	runner, err := worker.New(client, worker.DispatchHandlers(), *lease)
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		receipt, err := runner.RunOne(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case *once && err != nil:
			return err
		case *once:
			return writeJSON(out, receipt)
		case errors.Is(err, workerapi.ErrTemporary), errors.Is(err, store.ErrLeaseLost), errors.Is(err, context.DeadlineExceeded):
			fmt.Fprintln(diagnostic, err)
		case err != nil:
			return err
		case receipt != nil:
			if err := writeJSON(out, receipt); err != nil {
				return err
			}
			continue
		}
		timer := time.NewTimer(*interval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return nil
}
