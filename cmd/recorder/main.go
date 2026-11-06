package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type record struct {
	TS    int64  `json:"ts"` // Kafka timestamp in milliseconds
	Key   string `json:"key"`
	Value string `json:"value"` // original event JSON, unchanged
}

func main() {
	out := flag.String("out", "data/recordings/stream.jsonl.gz", "output file (appended to if it exists)")
	minutes := flag.Float64("minutes", 180, "how long to record")
	broker := flag.String("broker", "localhost:29092", "Kafka bootstrap server")
	topic := flag.String("topic", "wiki.recentchange", "topic to record")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(*minutes*float64(time.Minute)))
	defer cancel()

	f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close() // runs before f.Close, so the file ends cleanly
	enc := json.NewEncoder(gz)
	enc.SetEscapeHTML(false)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{*broker},
		GroupID:     "recorder-" + strconv.FormatInt(time.Now().Unix(), 10), // new group each run
		Topic:       *topic,
		StartOffset: kafka.LastOffset, // record only events that arrive from now on
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
	})
	defer r.Close()

	log.Printf("recording %s to %s for %.0f min (Ctrl+C to stop early)", *topic, *out, *minutes)
	n := 0
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("read error: %v", err)
			}
			break
		}
		if err := enc.Encode(record{TS: m.Time.UnixMilli(), Key: string(m.Key), Value: string(m.Value)}); err != nil {
			log.Printf("write error: %v", err)
			break
		}
		n++
		if n%1000 == 0 {
			gz.Flush() // data up to here survives a crash
			log.Printf("recorded %d events", n)
		}
	}
	log.Printf("done: %d events -> %s", n, *out)
}
