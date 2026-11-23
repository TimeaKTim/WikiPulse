package archive

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func raw(id, dt string) []byte {
	return []byte(fmt.Sprintf(`{"meta":{"id":%q,"dt":%q},"wiki":"rowiki","title":"T"}`, id, dt))
}

func write(t *testing.T, w *Writer, id, dt string) {
	t.Helper()
	ts, _ := time.Parse(time.RFC3339, dt)
	if err := w.Write(ts, raw(id, dt)); err != nil {
		t.Fatal(err)
	}
}

func TestRoundTrip(t *testing.T) {
	root := t.TempDir()

	w := NewWriter(root)
	write(t, w, "a", "2026-11-16T10:59:00Z")
	write(t, w, "b", "2026-11-16T11:00:30Z")
	write(t, w, "c", "2026-11-16T10:59:30Z") // out of order, same file as "a"
	write(t, w, "d", "2026-11-16T12:10:00Z")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// a second, overlapping run stores "b" again
	w2 := NewWriter(root)
	w2.run += "-2"
	write(t, w2, "b", "2026-11-16T11:00:30Z")
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(root, "*", "*", "*.gz"))
	if len(files) != 4 {
		t.Errorf("got %d part files, want 4 (hours 10, 11, 12 and the second run's 11)", len(files))
	}

	hours, err := Hours(root)
	if err != nil || len(hours) != 3 {
		t.Fatalf("hours = %v, %v; want 3", hours, err)
	}
	h10, _ := ReadHour(root, hours[0])
	if len(h10) != 2 || h10[0].Event.Meta.ID != "a" || h10[1].Event.Meta.ID != "c" {
		t.Errorf("hour 10 = %v; want a, c in time order", ids(h10))
	}
	h11, _ := ReadHour(root, hours[1])
	if len(h11) != 1 {
		t.Errorf("hour 11 = %v; want b once", ids(h11))
	}
}

func TestReadCutOffFile(t *testing.T) {
	root := t.TempDir()
	w := NewWriter(root)
	write(t, w, "a", "2026-11-16T10:00:00Z")
	write(t, w, "b", "2026-11-16T10:00:01Z")
	if err := w.Flush(); err != nil { // like a checkpoint, then a crash
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(root, "*", "*", "*.gz"))
	data, _ := os.ReadFile(files[0])
	if err := os.WriteFile(files[0], data[:len(data)-3], 0o644); err != nil {
		t.Fatal(err)
	}
	lines, err := ReadHour(root, time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC))
	if err != nil || len(lines) == 0 {
		t.Fatalf("got %d lines, %v; want the events before the cut", len(lines), err)
	}
}

func TestParseTime(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	for in, want := range map[string]time.Time{
		"7d":                   now.AddDate(0, 0, -7),
		"36h":                  now.Add(-36 * time.Hour),
		"2026-11-16T00:00:00Z": time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC),
	} {
		if got, err := ParseTime(in, now); err != nil || !got.Equal(want) {
			t.Errorf("ParseTime(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := ParseTime("soon", now); err == nil {
		t.Error("ParseTime(soon) should fail")
	}
}

func ids(ls []Line) []string {
	var s []string
	for _, l := range ls {
		s = append(s, l.Event.Meta.ID)
	}
	return s
}

func TestOpenFileLimit(t *testing.T) {
	root := t.TempDir()
	w := NewWriter(root)
	for h := 0; h < 10; h++ {
		write(t, w, fmt.Sprint(h), fmt.Sprintf("2026-11-16T%02d:00:00Z", h))
	}
	if len(w.open) != maxOpen {
		t.Errorf("%d files open, want %d", len(w.open), maxOpen)
	}
	if err := w.CloseIdle(0); err != nil || len(w.open) != 0 {
		t.Errorf("CloseIdle left %d open, %v", len(w.open), err)
	}
}
