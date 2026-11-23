// replay feeds archived events back into Kafka at the original or an
// accelerated speed. It reads the collector's bronze archive, or a single
// .jsonl.gz file (also the old recorder format).
//
// Pacing follows meta.dt, and consumers window on meta.dt too, so a
// 10-minute window means 10 minutes of real edits at any speed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/TimeaKTim/wikipulse/internal/archive"
	"github.com/TimeaKTim/wikipulse/internal/event"
)

var replayPos atomic.Int64 // meta.dt of the last event sent, Unix ms

type sendFunc func(ctx context.Context, ev event.Event, raw []byte) error

// pacer waits so that events go out at speed times their original rate.
type pacer struct {
	speed        float64
	first, start time.Time
}

func (p *pacer) wait(ctx context.Context, t time.Time) error {
	if p.speed <= 0 {
		return nil
	}
	if p.start.IsZero() {
		p.first, p.start = t, time.Now()
		return nil
	}
	d := time.Until(p.start.Add(time.Duration(float64(t.Sub(p.first)) / p.speed)))
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func replayArchive(ctx context.Context, send sendFunc, root string, from, to time.Time, p *pacer) (int, error) {
	hours, err := archive.Hours(root)
	if err != nil {
		return 0, err
	}
	if len(hours) == 0 {
		return 0, fmt.Errorf("no archive in %s; fetch one with: go run ./cmd/collector -since 7d", root)
	}
	n := 0
	for _, h := range hours {
		if !from.IsZero() && !h.Add(time.Hour).After(from) {
			continue
		}
		if !to.IsZero() && !h.Before(to) {
			break
		}
		lines, err := archive.ReadHour(root, h)
		if err != nil {
			return n, err
		}
		for _, l := range lines {
			t := l.Event.Time
			if (!from.IsZero() && t.Before(from)) || (!to.IsZero() && !t.Before(to)) {
				continue
			}
			if err := p.wait(ctx, t); err != nil {
				return n, err
			}
			if err := send(ctx, l.Event, l.Raw); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

// unwrap returns the raw event of a line. Old recorder files wrap it as
// {"ts":..., "key":..., "value":"<event JSON>"}. The result is a copy,
// because the line buffer is reused and Kafka sends asynchronously.
func unwrap(line []byte) []byte {
	var old struct {
		Value *string `json:"value"`
	}
	if json.Unmarshal(line, &old) == nil && old.Value != nil {
		return []byte(*old.Value)
	}
	return append([]byte(nil), line...)
}

func replayFile(ctx context.Context, send sendFunc, name string, p *pacer) (int, error) {
	n := 0
	err := archive.ReadFile(name, func(line []byte) error {
		raw := unwrap(line)
		ev, err := event.Parse(raw)
		if err != nil {
			return nil // skip a damaged line
		}
		if err := p.wait(ctx, ev.Time); err != nil {
			return err
		}
		if err := send(ctx, ev, raw); err != nil {
			return err
		}
		n++
		return nil
	})
	return n, err
}

func main() {
	root := flag.String("archive", "data/bronze/recentchange", "archive written by the collector")
	fromFlag := flag.String("from", "", "replay events from this time (RFC 3339) or this long ago (7d, 36h); default: start of the archive")
	toFlag := flag.String("to", "", "replay events before this time; default: end of the archive")
	file := flag.String("file", "", "replay this one .jsonl.gz file instead of the archive")
	speed := flag.Float64("speed", 1, "1 = original rate, 60 = sixty times faster, 0 = as fast as possible")
	loop := flag.Bool("loop", false, "repeat until stopped")
	broker := flag.String("broker", "localhost:29092", "Kafka bootstrap server")
	topic := flag.String("topic", "wiki.recentchange", "topic to write to")
	flag.Parse()

	now := time.Now()
	var from, to time.Time
	var err error
	if *fromFlag != "" {
		if from, err = archive.ParseTime(*fromFlag, now); err != nil {
			log.Fatalf("-from: %v", err)
		}
	}
	if *toFlag != "" {
		if to, err = archive.ParseTime(*toFlag, now); err != nil {
			log.Fatalf("-to: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM) // SIGTERM: docker stop
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

	header := []kafka.Header{{Key: event.SourceHeader, Value: []byte("replay")}}
	send := func(ctx context.Context, ev event.Event, raw []byte) error {
		replayPos.Store(ev.Time.UnixMilli())
		// no Time set, so Kafka stamps the message with the replay time; event time is meta.dt
		return w.WriteMessages(ctx, kafka.Message{Key: ev.Key(), Value: raw, Headers: header})
	}

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.Printf("replaying, at %s", time.UnixMilli(replayPos.Load()).UTC().Format(time.RFC3339))
			}
		}
	}()

	for ctx.Err() == nil {
		p := &pacer{speed: *speed}
		var n int
		var err error
		if *file != "" {
			n, err = replayFile(ctx, send, *file, p)
		} else {
			n, err = replayArchive(ctx, send, *root, from, to, p)
		}
		if err != nil && ctx.Err() == nil {
			log.Printf("replay error: %v", err)
		}
		log.Printf("replayed %d events at speed=%g", n, *speed)
		if err != nil || !*loop {
			break
		}
	}
}
