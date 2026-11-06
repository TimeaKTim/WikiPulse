package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

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

func main() {
	_ = godotenv.Load() // reads .env if present

	broker := getenv("KAFKA_BOOTSTRAP", "localhost:29092")
	topic := getenv("KAFKA_TOPIC_RAW", "wiki.recentchange")
	url := getenv("EVENTSTREAMS_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
	ua := os.Getenv("WIKIPULSE_USER_AGENT")
	if ua == "" || strings.Contains(ua, "example.com") {
		log.Fatal("set WIKIPULSE_USER_AGENT in .env with a real contact email")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	w := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
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

	var sent atomic.Int64
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
				log.Printf("sent=%d rate=%.1f ev/s", n, float64(n-last)/10)
				last = n
			}
		}
	}()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "text/event-stream")

	log.Printf("connecting to %s", url)
	resp, err := (&http.Client{}).Do(req) // no timeout: this is a long-lived stream
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("unexpected status: %s", resp.Status)
	}
	log.Println("connected, reading events (Ctrl+C to stop)")

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var data []string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "": // blank line = end of one event
			if len(data) > 0 {
				payload := []byte(strings.Join(data, "\n"))
				data = data[:0]
				err := w.WriteMessages(ctx, kafka.Message{Key: keyOf(payload), Value: payload})
				if err != nil && ctx.Err() == nil {
					log.Printf("enqueue error: %v", err)
				}
				sent.Add(1)
			}
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
		// lines starting with ":" (keep-alive), "id:" and "event:" are ignored for now
	}
	if err := sc.Err(); err != nil && ctx.Err() == nil {
		log.Fatalf("stream error: %v", err)
	}
}
