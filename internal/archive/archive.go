// Package archive is the bronze layer: raw events as received, in hourly
// gzipped JSON-lines files, partitioned by event time (meta.dt, UTC):
//
//	<root>/date=YYYY-MM-DD/hour=HH/part-<run>-<seq>.jsonl.gz
//
// Every collector run writes its own part files, so runs never touch each
// other's files. Overlapping runs can store an event twice; readers
// de-duplicate by meta.id.
package archive

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/TimeaKTim/wikipulse/internal/event"
)

// Events are not in time order: the stream interleaves one topic per
// datacenter. So files are not closed by event time, but when more than
// maxOpen hours are open (least recently used first) or by CloseIdle.
// An event for an hour whose file was closed goes to a new part file.
const maxOpen = 4

// Dir is the folder of one hour.
func Dir(root string, hour time.Time) string {
	hour = hour.UTC()
	return filepath.Join(root, "date="+hour.Format("2006-01-02"), "hour="+hour.Format("15"))
}

type hourFile struct {
	f    *os.File
	gz   *gzip.Writer
	used time.Time
}

type Writer struct {
	root string
	run  string
	seq  int
	open map[time.Time]*hourFile
}

func NewWriter(root string) *Writer {
	return &Writer{
		root: root,
		run:  time.Now().UTC().Format("20060102T150405Z"),
		open: map[time.Time]*hourFile{},
	}
}

// Write appends one raw event to the file of the hour it belongs to.
func (w *Writer) Write(t time.Time, raw []byte) error {
	hour := t.UTC().Truncate(time.Hour)
	hf := w.open[hour]
	if hf == nil {
		if len(w.open) >= maxOpen {
			if err := w.closeLRU(); err != nil {
				return err
			}
		}
		var err error
		if hf, err = w.create(hour); err != nil {
			return err
		}
		w.open[hour] = hf
	}
	hf.used = time.Now()
	if _, err := hf.gz.Write(raw); err != nil {
		return err
	}
	if _, err := hf.gz.Write([]byte{'\n'}); err != nil {
		return err
	}
	return nil
}

func (w *Writer) closeLRU() error {
	var oldest time.Time
	var hf *hourFile
	for h, f := range w.open {
		if hf == nil || f.used.Before(hf.used) {
			oldest, hf = h, f
		}
	}
	return w.closeHour(oldest)
}

// CloseIdle finishes the files that got no event for d, so batch jobs can
// read complete hours while the collector keeps running.
func (w *Writer) CloseIdle(d time.Duration) error {
	for h, hf := range w.open {
		if time.Since(hf.used) >= d {
			if err := w.closeHour(h); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Writer) create(hour time.Time) (*hourFile, error) {
	dir := Dir(w.root, hour)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w.seq++
	name := filepath.Join(dir, fmt.Sprintf("part-%s-%03d.jsonl.gz", w.run, w.seq))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	return &hourFile{f: f, gz: gzip.NewWriter(f)}, nil
}

func (w *Writer) closeHour(h time.Time) error {
	hf := w.open[h]
	delete(w.open, h)
	err := hf.gz.Close()
	if e := hf.f.Close(); err == nil {
		err = e
	}
	return err
}

// Flush writes buffered events to disk, so they survive a crash.
func (w *Writer) Flush() error {
	for _, hf := range w.open {
		if err := hf.gz.Flush(); err != nil {
			return err
		}
		if err := hf.f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Close() error {
	var err error
	for h := range w.open {
		if e := w.closeHour(h); err == nil {
			err = e
		}
	}
	return err
}

// Hours lists the hours that have a folder in the archive, oldest first.
func Hours(root string) ([]time.Time, error) {
	dirs, err := filepath.Glob(filepath.Join(root, "date=*", "hour=*"))
	if err != nil {
		return nil, err
	}
	var hours []time.Time
	for _, d := range dirs {
		date := strings.TrimPrefix(filepath.Base(filepath.Dir(d)), "date=")
		hour := strings.TrimPrefix(filepath.Base(d), "hour=")
		if h, err := time.Parse("2006-01-02 15", date+" "+hour); err == nil {
			hours = append(hours, h)
		}
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].Before(hours[j]) })
	return hours, nil
}

type Line struct {
	Event event.Event
	Raw   []byte
}

// ReadHour returns the events of one hour from all its part files,
// de-duplicated by meta.id and sorted by event time.
func ReadHour(root string, hour time.Time) ([]Line, error) {
	files, err := filepath.Glob(filepath.Join(Dir(root, hour), "part-*.jsonl.gz"))
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []Line
	for _, name := range files {
		err := ReadFile(name, func(raw []byte) error {
			ev, err := event.Parse(raw)
			if err != nil || seen[ev.Meta.ID] {
				return nil // skip damaged lines and duplicates
			}
			seen[ev.Meta.ID] = true
			out = append(out, Line{Event: ev, Raw: append([]byte(nil), raw...)})
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Event.Time.Before(out[j].Event.Time) })
	return out, nil
}

// ReadFile calls fn for each line of a gzipped JSON-lines file and stops at the
// first error fn returns. The line buffer is reused, so fn must copy what it keeps.
// A file cut off by a crash, or still being written, is read up to the cut.
func ReadFile(name string, fn func(line []byte) error) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if errors.Is(err, io.EOF) {
		return nil // nothing flushed yet
	}
	if err != nil {
		return err
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for sc.Scan() {
		if err := fn(sc.Bytes()); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	return nil
}

// ParseTime reads an RFC 3339 time, or a duration back from now like 7d or 36h.
func ParseTime(s string, now time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	if days, ok := strings.CutSuffix(s, "d"); ok {
		if n, err := strconv.Atoi(days); err == nil && n >= 0 {
			return now.AddDate(0, 0, -n).UTC(), nil
		}
	} else if d, err := time.ParseDuration(s); err == nil && d >= 0 {
		return now.Add(-d).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("%q is neither a time like 2026-11-16T00:00:00Z nor a duration like 7d or 36h", s)
}
