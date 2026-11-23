// collector reads Wikimedia EventStreams, keeps the events of the chosen wikis
// (WIKIS), stores them raw in the hourly bronze archive and, in live mode, also
// publishes them to Kafka. The archive is the source of truth: it is written
// first, and the resume point is saved only after the archive is on disk, so a
// Kafka outage loses nothing (replay the archive to fill the gap).
//
//	go run ./cmd/collector             live: archive + Kafka, resumes from .state/last_event_id
//	go run ./cmd/collector -since 7d   backfill: archive the last 7 days, then exit (no Kafka)
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"

	"github.com/TimeaKTim/wikipulse/internal/archive"
	"github.com/TimeaKTim/wikipulse/internal/event"
)

const (
	checkpointEvery = 5 * time.Second
	closeIdleAfter  = 10 * time.Minute // finish an hour file when its hour has gone quiet
	topicQuietAfter = 30 * time.Second // backfill: a topic silent this long has nothing more to send
)

var (
	received, archived, skipped, outside atomic.Int64
	badEvents                            atomic.Int64
	kafkaSent, kafkaErrors, reconnects   atomic.Int64
	streamPos                            atomic.Int64 // meta.dt of the last event read, Unix ms
)

var (
	errDone  = errors.New("backfill reached -until")
	errFatal = errors.New("fatal")
)

type config struct {
	url         string
	userAgent   string
	stateFile   string
	idleTimeout time.Duration
	dropAfter   time.Duration // debug only: cut the connection on purpose
	since       time.Time     // backfill only; zero in live mode
	until       time.Time
}

// topicPos is how far the backfill has come in one stream topic.
type topicPos struct {
	last time.Time // meta.dt of its last event
	seen time.Time // wall time of its last event
}

type collector struct {
	cfg            config
	topics         map[string]*topicPos // backfill only
	wikis          event.Filter
	arch           *archive.Writer
	kw             *kafka.Writer // nil in backfill mode
	lastID         string        // ID of the last event handled
	lastCheckpoint time.Time
}

func (c *collector) live() bool { return c.cfg.since.IsZero() }

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func loadLastID(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// write to a temp file, then rename, so a crash never leaves a half-written file
func saveLastID(path, id string) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(id), 0o644); err != nil {
		log.Printf("save state: %v", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Printf("save state: %v", err)
	}
}

// resumeID is where the next connection continues: the last event of this
// process, or in live mode the one saved by the previous run.
func (c *collector) resumeID() string {
	if c.lastID != "" {
		return c.lastID
	}
	if c.live() {
		return loadLastID(c.cfg.stateFile)
	}
	return "" // first backfill connection: the since parameter decides
}

func (c *collector) handle(ctx context.Context, id string, raw []byte) error {
	received.Add(1)
	ev, err := event.Parse(raw)
	switch {
	case err != nil:
		badEvents.Add(1)
		log.Printf("skipping unreadable event %s: %v", id, err)
	case !c.live() && (ev.Time.Before(c.cfg.since) || !ev.Time.Before(c.cfg.until)):
		outside.Add(1)
	case !c.wikis.Match(ev.Wiki):
		skipped.Add(1)
	default:
		if err := c.arch.Write(ev.Time, raw); err != nil {
			return fmt.Errorf("%w: archive: %v", errFatal, err)
		}
		archived.Add(1)
		if c.kw != nil {
			msg := kafka.Message{
				Key:     ev.Key(),
				Value:   raw,
				Headers: []kafka.Header{{Key: event.SourceHeader, Value: []byte("live")}},
			}
			if err := c.kw.WriteMessages(ctx, msg); err != nil && ctx.Err() == nil {
				kafkaErrors.Add(1)
				log.Printf("kafka enqueue error: %v", err)
			}
		}
	}
	if err == nil {
		streamPos.Store(ev.Time.UnixMilli())
	}
	if id != "" {
		c.lastID = id
	}
	if time.Since(c.lastCheckpoint) >= checkpointEvery {
		if err := c.checkpoint(); err != nil {
			return err
		}
	}
	if err == nil && !c.live() && c.backfillDone(id, ev) {
		return errDone
	}
	return nil
}

// backfillDone reports whether every stream topic has reached -until. The
// stream interleaves one topic per datacenter, and a quiet topic runs ahead
// of a busy one, so a single event past -until does not mean the end. A topic
// that has been silent for a while has delivered all it had.
func (c *collector) backfillDone(id string, ev event.Event) bool {
	now := time.Now()
	// the SSE id lists every topic of the stream, also those not seen yet
	var positions []struct {
		Topic string `json:"topic"`
	}
	_ = json.Unmarshal([]byte(id), &positions)
	for _, p := range positions {
		if c.topics[p.Topic] == nil {
			c.topics[p.Topic] = &topicPos{seen: now}
		}
	}
	tp := c.topics[ev.Meta.Topic]
	if tp == nil {
		tp = &topicPos{}
		c.topics[ev.Meta.Topic] = tp
	}
	tp.seen = now
	if ev.Time.After(tp.last) {
		tp.last = ev.Time
	}
	if tp.last.Before(c.cfg.until) {
		return false
	}
	for _, p := range c.topics {
		if p.last.Before(c.cfg.until) && now.Sub(p.seen) < topicQuietAfter {
			return false
		}
	}
	return true
}

// checkpoint flushes the archive to disk and only then saves the resume point,
// so a restart never skips events that did not reach the archive.
func (c *collector) checkpoint() error {
	c.lastCheckpoint = time.Now()
	if err := c.arch.Flush(); err != nil {
		return fmt.Errorf("%w: archive flush: %v", errFatal, err)
	}
	if err := c.arch.CloseIdle(closeIdleAfter); err != nil {
		return fmt.Errorf("%w: archive close: %v", errFatal, err)
	}
	if c.live() && c.lastID != "" {
		saveLastID(c.cfg.stateFile, c.lastID)
	}
	return nil
}

func (c *collector) streamURL() (string, error) {
	if c.live() {
		return c.cfg.url, nil
	}
	u, err := url.Parse(c.cfg.url)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("since", c.cfg.since.Format(time.RFC3339))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// streamOnce opens one connection and reads until it ends or fails.
// It returns how many events it read, so the caller knows if the connection worked.
func streamOnce(ctx context.Context, c *collector) (int, error) {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	u, err := c.streamURL()
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", c.cfg.userAgent)
	req.Header.Set("Accept", "text/event-stream")
	if id := c.resumeID(); id != "" {
		req.Header.Set("Last-Event-ID", id) // takes precedence over since
		log.Println("resuming from Last-Event-ID", id)
	} else if !c.live() {
		log.Println("starting backfill from", c.cfg.since.Format(time.RFC3339))
	}

	resp, err := (&http.Client{}).Do(req) // no client timeout: this is a long-lived stream
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, errors.New("unexpected status: " + resp.Status)
	}
	log.Println("connected, reading events (Ctrl+C to stop)")

	// watchdog: if no line arrives for idleTimeout, cancel the connection
	idle := time.AfterFunc(c.cfg.idleTimeout, cancel)
	defer idle.Stop()
	if c.cfg.dropAfter > 0 {
		drop := time.AfterFunc(c.cfg.dropAfter, func() {
			log.Println("DEBUG: dropping the connection on purpose")
			cancel()
		})
		defer drop.Stop()
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var data []string
	var curID string
	n := 0
	for sc.Scan() {
		idle.Reset(c.cfg.idleTimeout)
		line := sc.Text()
		switch {
		case line == "": // blank line = end of one event
			if len(data) > 0 {
				payload := []byte(strings.Join(data, "\n"))
				data = data[:0]
				if err := c.handle(ctx, curID, payload); err != nil {
					return n, err
				}
				n++
			}
		case strings.HasPrefix(line, "id:"):
			curID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
		// lines starting with ":" (keep-alive) and "event:" are ignored
	}
	if err := sc.Err(); err != nil {
		return n, err
	}
	return n, errors.New("stream closed by server")
}

// serveMetrics exposes the counters in the Prometheus text format.
func serveMetrics(addr string) {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, m := range []struct {
			name, typ, help string
			v               float64
		}{
			{"wikipulse_collector_events_received_total", "counter", "Events read from EventStreams, all wikis.", float64(received.Load())},
			{"wikipulse_collector_events_archived_total", "counter", "Events of the selected wikis written to the archive.", float64(archived.Load())},
			{"wikipulse_collector_events_bad_total", "counter", "Events that could not be parsed.", float64(badEvents.Load())},
			{"wikipulse_collector_kafka_sent_total", "counter", "Events delivered to Kafka.", float64(kafkaSent.Load())},
			{"wikipulse_collector_kafka_errors_total", "counter", "Events that failed to reach Kafka.", float64(kafkaErrors.Load())},
			{"wikipulse_collector_reconnects_total", "counter", "Reconnections to EventStreams.", float64(reconnects.Load())},
			{"wikipulse_collector_stream_position_seconds", "gauge", "meta.dt of the last event read; delay = time() - this.", float64(streamPos.Load()) / 1000},
		} {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %s\n", m.name, m.help, m.name, m.typ, m.name, strconv.FormatFloat(m.v, 'f', -1, 64))
		}
	})
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("metrics server: %v", err)
		}
	}()
}

func main() {
	since := flag.String("since", "", "backfill instead of listening live: start at this time (RFC 3339) or this long ago (7d, 36h)")
	until := flag.String("until", "", "backfill only: stop at this time (default: when the backfill started)")
	metrics := flag.String("metrics", ":9101", "address for Prometheus /metrics (empty = off)")
	flag.Parse()
	_ = godotenv.Load() // reads .env if present

	ua := os.Getenv("WIKIPULSE_USER_AGENT")
	if ua == "" || strings.Contains(ua, "example.com") {
		log.Fatal("set WIKIPULSE_USER_AGENT in .env with a real contact email")
	}
	stateDir := getenv("STATE_DIR", ".state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		log.Fatalf("create state dir: %v", err)
	}
	dropAfter, _ := time.ParseDuration(os.Getenv("DEBUG_DROP_AFTER")) // empty = 0 = off
	now := time.Now()
	cfg := config{
		url:         getenv("EVENTSTREAMS_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		userAgent:   ua,
		stateFile:   filepath.Join(stateDir, "last_event_id"),
		idleTimeout: 60 * time.Second,
		dropAfter:   dropAfter,
	}
	var err error
	switch {
	case *since != "":
		if cfg.since, err = archive.ParseTime(*since, now); err != nil {
			log.Fatalf("-since: %v", err)
		}
		cfg.until = now.UTC()
		if *until != "" {
			if cfg.until, err = archive.ParseTime(*until, now); err != nil {
				log.Fatalf("-until: %v", err)
			}
		}
	case *until != "":
		log.Fatal("-until only works together with -since")
	}

	c := &collector{
		cfg:            cfg,
		topics:         map[string]*topicPos{},
		wikis:          event.ParseFilter(getenv("WIKIS", "rowiki")),
		arch:           archive.NewWriter(getenv("ARCHIVE_DIR", "data/bronze/recentchange")),
		lastCheckpoint: now,
	}
	if c.live() {
		c.kw = &kafka.Writer{
			Addr:         kafka.TCP(getenv("KAFKA_BOOTSTRAP", "localhost:29092")),
			Topic:        getenv("KAFKA_TOPIC_RAW", "wiki.recentchange"),
			Balancer:     &kafka.Hash{},
			BatchSize:    100,
			BatchTimeout: 50 * time.Millisecond,
			Async:        true,
			Completion: func(msgs []kafka.Message, err error) {
				if err != nil {
					kafkaErrors.Add(int64(len(msgs)))
					log.Printf("kafka write error (%d events, still in the archive): %v", len(msgs), err)
					return
				}
				kafkaSent.Add(int64(len(msgs)))
			},
		}
		log.Printf("live mode: wikis=%s -> archive + Kafka", c.wikis)
	} else {
		log.Printf("backfill mode: wikis=%s, %s to %s -> archive only",
			c.wikis, cfg.since.Format(time.RFC3339), cfg.until.Format(time.RFC3339))
	}
	if *metrics != "" {
		serveMetrics(*metrics)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM) // SIGTERM: docker stop
	defer stop()

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n := received.Load()
				pos := time.UnixMilli(streamPos.Load()).UTC().Format(time.RFC3339)
				log.Printf("read=%d (%.1f ev/s) archived=%d kafka sent=%d errors=%d reconnects=%d at=%s",
					n, float64(n-last)/10, archived.Load(), kafkaSent.Load(), kafkaErrors.Load(), reconnects.Load(), pos)
				last = n
			}
		}
	}()

	var runErr error
	backoff := time.Second
	for ctx.Err() == nil {
		n, err := streamOnce(ctx, c)
		if errors.Is(err, errDone) {
			log.Println("backfill complete")
			break
		}
		if errors.Is(err, errFatal) {
			runErr = err
			break
		}
		if ctx.Err() != nil {
			break
		}
		if n > 0 {
			backoff = time.Second // the connection worked, so start the backoff over
		}
		r := reconnects.Add(1)
		log.Printf("stream ended after %d events (%v); reconnecting in %s (reconnect #%d)", n, err, backoff, r)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Minute)
	}

	if err := c.checkpoint(); err != nil && runErr == nil {
		runErr = err
	}
	if err := c.arch.Close(); err != nil && runErr == nil {
		runErr = err
	}
	if c.kw != nil {
		c.kw.Close() // flushes pending messages
	}
	log.Printf("stopped: read=%d archived=%d other wikis=%d outside window=%d unreadable=%d kafka sent=%d errors=%d",
		received.Load(), archived.Load(), skipped.Load(), outside.Load(), badEvents.Load(), kafkaSent.Load(), kafkaErrors.Load())
	if runErr != nil {
		log.Fatal(runErr)
	}
}
