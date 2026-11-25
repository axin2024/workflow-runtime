package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/axin2024/workflow-runtime/internal/console"
	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workerapi"
)

func runConsole(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("console", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:8081", "loopback listen address")
	secureCookie := flags.Bool("secure-cookie", false, "require HTTPS for the browser session cookie")
	if err := flags.Parse(args); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || !workerapi.IsLoopback(host) || flags.NArg() != 0 {
		return errors.New("console requires a loopback host and port")
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
	logger := log.New(os.Stderr, "console ", log.LstdFlags|log.LUTC)
	handler, err := console.Handler(db, logger, *secureCookie)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ErrorLog: logger, ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
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
