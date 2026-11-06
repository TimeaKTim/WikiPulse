package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

var sent, reconnects atomic.Int64

type config struct {
	url         string
	userAgent   string
	stateFile   string
	idleTimeout time.Duration
	dropAfter   time.Duration // debug only: cut the connection on purpose
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// partition key: wiki|title, so all edits of a page go to the same partition
func keyOf(data []byte) []byte {
	var e struct {
		Wiki  string `json:"wiki"`
		Title string `json:"title"`
	}
	if err := json.Unmarshal(data, &e); err != nil {
		return nil
	}
	return []byte(e.Wiki + "|" + e.Title)
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

// streamOnce opens one connection and reads until it ends or fails.
// It returns how many events it read, so the caller knows if the connection worked.
func streamOnce(ctx context.Context, cfg config, w *kafka.Writer) (int, error) {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, cfg.url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", cfg.userAgent)
	req.Header.Set("Accept", "text/event-stream")
	if id := loadLastID(cfg.stateFile); id != "" {
		req.Header.Set("Last-Event-ID", id)
		log.Println("resuming from Last-Event-ID", id)
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
	idle := time.AfterFunc(cfg.idleTimeout, cancel)
	defer idle.Stop()
	if cfg.dropAfter > 0 {
		drop := time.AfterFunc(cfg.dropAfter, func() {
			log.Println("DEBUG: dropping the connection on purpose")
			cancel()
		})
		defer drop.Stop()
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var data []string
	var curID, doneID string
	n := 0
	for sc.Scan() {
		idle.Reset(cfg.idleTimeout)
		line := sc.Text()
		switch {
		case line == "": // blank line = end of one event
			if len(data) > 0 {
				payload := []byte(strings.Join(data, "\n"))
				data = data[:0]
				if err := w.WriteMessages(ctx, kafka.Message{Key: keyOf(payload), Value: payload}); err != nil {
					if ctx.Err() != nil {
						return n, ctx.Err()
					}
					log.Printf("enqueue error: %v", err)
				}
				doneID = curID
				n++
				if sent.Add(1)%100 == 0 && doneID != "" {
					saveLastID(cfg.stateFile, doneID)
				}
			}
		case strings.HasPrefix(line, "id:"):
			curID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
		// lines starting with ":" (keep-alive) and "event:" are ignored
	}
	if doneID != "" {
		saveLastID(cfg.stateFile, doneID)
	}
	if err := sc.Err(); err != nil {
		return n, err
	}
	return n, errors.New("stream closed by server")
}

func main() {
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
	cfg := config{
		url:         getenv("EVENTSTREAMS_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		userAgent:   ua,
		stateFile:   filepath.Join(stateDir, "last_event_id"),
		idleTimeout: 60 * time.Second,
		dropAfter:   dropAfter,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	w := &kafka.Writer{
		Addr:         kafka.TCP(getenv("KAFKA_BOOTSTRAP", "localhost:29092")),
		Topic:        getenv("KAFKA_TOPIC_RAW", "wiki.recentchange"),
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 50 * time.Millisecond,
		Async:        true,
		Completion: func(_ []kafka.Message, err error) {
			if err != nil {
				log.Printf("kafka write error: %v", err)
			}
		},
	}
	defer w.Close() // flushes pending messages

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n := sent.Load()
				log.Printf("sent=%d rate=%.1f ev/s reconnects=%d", n, float64(n-last)/10, reconnects.Load())
				last = n
			}
		}
	}()

	backoff := time.Second
	for ctx.Err() == nil {
		n, err := streamOnce(ctx, cfg, w)
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
	log.Println("stopped")
}
