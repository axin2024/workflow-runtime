package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
)

func parseSchedule(generation, path string) (uint64, store.ScheduleSpec, error) {
	var spec store.ScheduleSpec
	base, err := strconv.ParseUint(generation, 10, 64)
	if err != nil {
		return 0, spec, errors.New("expected generation must be a nonnegative integer")
	}
	raw, err := readSample(path)
	if err != nil {
		return 0, spec, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		return 0, spec, fmt.Errorf("decode schedule: %w", err)
	}
	if decoder.Decode(new(json.RawMessage)) != io.EOF {
		return 0, spec, errors.New("expected one schedule object")
	}
	return base, spec, nil
}

func runScheduler(ctx context.Context, args []string, dsn, actor string, out, diagnostic io.Writer) error {
	flags := flag.NewFlagSet("scheduler", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	once := flags.Bool("once", false, "perform a single scan")
	at := flags.String("at", "", "override the database clock for a single scan")
	interval := flags.Duration("interval", 5*time.Second, "time between scans")
	limit := flags.Int("limit", 128, "maximum schedules per scan")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *interval < time.Second || *interval > time.Hour || *limit < 1 || *limit > 256 || (*at != "" && !*once) {
		return errors.New("scheduler requires a 1s to 1h interval and a 1 to 256 scan limit; --at requires --once")
	}
	var now time.Time
	if *at != "" {
		var err error
		now, err = time.Parse(time.RFC3339Nano, *at)
		if err != nil || now.IsZero() {
			return errors.New("scan time must be RFC3339")
		}
	}
	db, err := store.Open(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	start, cancel := context.WithTimeout(ctx, 15*time.Second)
	err = db.Ready(start)
	cancel()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	encoder := json.NewEncoder(out)
	for {
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		result, err := db.ScanDue(attempt, actor, now, *limit)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if (*once || len(result.Outcomes) > 0) && !result.At.IsZero() {
			if writeErr := encoder.Encode(result); writeErr != nil {
				return writeErr
			}
		}
		if *once {
			return err
		}
		if err != nil {
			fmt.Fprintln(diagnostic, err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
