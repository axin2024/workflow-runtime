package schedule

import (
	"testing"
	"time"
)

func TestCalendarUsesDistinctUTCInstantsDuringClockChanges(t *testing.T) {
	fall, err := Parse("30 1 * * *", "America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	first, ok := fall.Next(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	if !ok || first.Format(time.RFC3339) != "2026-11-01T05:30:00Z" {
		t.Fatalf("first repeated local time: %s", first)
	}
	second, ok := fall.Next(first)
	if !ok || second.Format(time.RFC3339) != "2026-11-01T06:30:00Z" {
		t.Fatalf("second repeated local time: %s", second)
	}
	if fall.Local(first).Format(time.RFC3339) != "2026-11-01T01:30:00-04:00" ||
		fall.Local(second).Format(time.RFC3339) != "2026-11-01T01:30:00-05:00" {
		t.Fatal("both local offsets must remain visible")
	}
	spring, err := Parse("30 2 * * *", "America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	next, ok := spring.Next(time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC))
	if !ok || spring.Local(next).Format(time.RFC3339) != "2026-03-09T02:30:00-04:00" {
		t.Fatalf("next occurrence after spring-forward gap: %s", next)
	}
}
