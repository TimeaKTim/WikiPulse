package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/segmentio/kafka-go"
)

type record struct {
	TS    int64  `json:"ts"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func replayOnce(ctx context.Context, w *kafka.Writer, path string, speed float64) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	var first int64
	var t0 time.Time
	n := 0
	for sc.Scan() {
		var rec record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			continue // skip a damaged line
		}
		if speed > 0 {
			if n == 0 {
				first, t0 = rec.TS, time.Now()
			}
			due := t0.Add(time.Duration(float64(rec.TS-first)/speed) * time.Millisecond)
			if d := time.Until(due); d > 0 {
				select {
				case <-ctx.Done():
					return n, ctx.Err()
				case <-time.After(d):
				}
			}
		}
		// no Time set, so Kafka stamps the message with the replay time
		if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(rec.Key), Value: []byte(rec.Value)}); err != nil {
			return n, err
		}
		n++
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) { // a cut-off file is fine
		return n, err
	}
	return n, nil
}

func main() {
	file := flag.String("file", "data/recordings/stream.jsonl.gz", "recording to replay")
	speed := flag.Float64("speed", 1, "1 = original rate, 5 = five times faster, 0 = as fast as possible")
	loop := flag.Bool("loop", false, "repeat the recording until stopped")
	broker := flag.String("broker", "localhost:29092", "Kafka bootstrap server")
	topic := flag.String("topic", "wiki.recentchange", "topic to write to")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	w := &kafka.Writer{
		Addr:         kafka.TCP(*broker),
		Topic:        *topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    500,
		BatchTimeout: 20 * time.Millisecond,
		Async:        true,
		Completion: func(_ []kafka.Message, err error) {
			if err != nil {
				log.Printf("kafka write error: %v", err)
			}
		},
	}
	defer w.Close() // flushes pending messages

	for ctx.Err() == nil {
		n, err := replayOnce(ctx, w, *file, *speed)
		if err != nil && ctx.Err() == nil {
			log.Printf("replay error: %v", err)
		}
		log.Printf("replayed %d events at speed=%g", n, *speed)
		if err != nil || !*loop {
			break
		}
	}
}
