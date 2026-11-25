package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/robfig/cron/v3"
)

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

type Calendar struct {
	expression string
	location   *time.Location
	spec       cron.Schedule
}

func Parse(expression, zone string) (Calendar, error) {
	fields := strings.Fields(expression)
	expression = strings.Join(fields, " ")
	if len(fields) != 5 || len(expression) > 128 {
		return Calendar{}, errors.New("cron must have five fields and at most 128 characters")
	}
	if zone == "" || zone == "Local" || len(zone) > 96 {
		return Calendar{}, errors.New("time zone must be a named IANA zone or UTC")
	}
	location, err := time.LoadLocation(zone)
	if err != nil {
		return Calendar{}, fmt.Errorf("load time zone: %w", err)
	}
	spec, err := parser.Parse("CRON_TZ=" + location.String() + " " + expression)
	if err != nil {
		return Calendar{}, fmt.Errorf("parse cron: %w", err)
	}
	return Calendar{expression: expression, location: location, spec: spec}, nil
}

func (c Calendar) Expression() string { return c.expression }

func (c Calendar) Local(instant time.Time) time.Time { return instant.In(c.location) }

// Next returns the next actual instant after the given time in UTC. A clock
// change may repeat a wall time, or skip it when that time never occurs.
func (c Calendar) Next(after time.Time) (time.Time, bool) {
	next := c.spec.Next(after.UTC()).UTC()
	return next, !next.IsZero() && next.Year() >= 1000 && next.Year() <= 9999
}
