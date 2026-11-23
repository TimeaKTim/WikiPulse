# WikiPulse

Real-time and historical analytics for Wikipedia activity. A Go collector reads the Wikimedia `recentchange` stream (scope: `rowiki`), stores it raw in an hourly archive (bronze) and publishes it to Kafka. For the demo, the last week is fetched into the archive and replayed through Kafka at a chosen speed.

Project proposal and architecture review: see the course documents. Event format: [`docs/event-schema.md`](docs/event-schema.md).

## What works today

| Part                                                     | Status      | Owner                   |
| -------------------------------------------------------- | ----------- | ----------------------- |
| Kafka (Docker Compose) with topic `wiki.recentchange`    | done        | Konya Timea             |
| Go collector: EventStreams to archive + Kafka, reconnect, resume, 7-day backfill | done | Konya Timea |
| Replay from the archive at chosen speed                  | done        | Konya Timea             |
| Event schema doc                                         | done        | Konya Timea             |
| Data lake and historical load                            | not started | Beldean Stefania        |
| Stream processor (windows, rules, Redis)                 | not started | Dragota Robert          |
| Dashboards, Compose integration                          | not started | Stanca Munteanu Patrick |

## Prerequisites

- Docker Desktop (with Compose v2)
- Go 1.22 or newer
- Git

## Quick start

```bash
git clone https://github.com/TimeaKTim/WikiPulse.git && cd WikiPulse
cp .env.example .env     # then edit WIKIPULSE_USER_AGENT: use a real contact email
docker compose up -d     # starts Kafka and creates the topic
go run ./cmd/collector   # live rowiki events -> archive + Kafka (Ctrl+C to stop)
```

`WIKIS` in `.env` picks the wikis (`rowiki` by default, `*` for all). Wikimedia requires a descriptive User-Agent with contact info. The collector refuses to start while `.env` still contains the `example.com` placeholder. Keep the value in double quotes.

Check that events arrive:

```bash
docker exec wp-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic wiki.recentchange --from-beginning --max-messages 3
```

## Demo data: fetch the last week, then replay it

A few days before a demo, fetch the last 7 days into the archive (no Kafka needed). EventStreams cannot filter by wiki, so this reads the whole stream and takes about **3.5 hours**:

```bash
go run ./cmd/collector -since 7d     # -> data/bronze/recentchange/date=.../hour=.../*.jsonl.gz
```

Then replay it through Kafka:

```bash
go run ./cmd/replay -speed 60              # whole archive, sixty times faster
go run ./cmd/replay -speed 1 -from 24h     # last day, original rate
go run ./cmd/replay -speed 0               # as fast as possible (load test)
go run ./cmd/replay -loop -speed 60        # repeat until Ctrl+C
```

Do not run the replay and the live collector at the same time, or both write to the topic. Messages carry a `wp-source` header (`live` or `replay`), and consumers must use `meta.dt` as event time (see the schema doc).

The older 2-hour all-wiki recording (222,141 events, **not in git**, ask Timea) still works as a load test:

```bash
go run ./cmd/replay -file data/recordings/stream.jsonl.gz -speed 0
```

## How to connect to Kafka

| From                        | Address           |
| --------------------------- | ----------------- |
| Go programs on your machine | `localhost:29092` |
| Other containers in Compose | `kafka:9092`      |

Topic `wiki.recentchange`: 6 partitions, key `<wiki>|<title>`, value is the original event JSON, unchanged. Details and consumer rules are in [`docs/event-schema.md`](docs/event-schema.md). Notably, deduplicate by `meta.id` (delivery is at-least-once) and use `meta.dt`, not the Kafka timestamp, as event time.

## Project layout

```
cmd/collector/   EventStreams -> bronze archive + Kafka; -since 7d fetches history
cmd/replay/      archive (or a recording) -> Kafka at original or accelerated speed
internal/event/  the event fields the Go tools read; wiki filter
internal/archive/ hourly bronze files: write, read, de-duplicate
docs/            event schema and other documentation
data/bronze/     the archive (git-ignored)
data/recordings/ old recordings (git-ignored)
.state/          collector state, last event ID (git-ignored)
```

## Measured so far

- Live stream, all wikis: about 30 events/s in the afternoon, about 60 at other times of day. rowiki: about 1.25 edits per minute.
- Backfill: about 1,600 events/s read, so one hour of history in about 75 s and a week in about 3.5 h.
- Replay at maximum speed: 222,141 events in 4.5 s, with the Kafka offsets verified before and after.
- With Kafka stopped, the collector kept archiving every event; the next run resumed without a gap.
- Metrics for Prometheus on `localhost:9101/metrics`.

## Useful commands

```bash
docker compose ps -a                 # container status
docker compose down                  # stop (keeps data)
docker compose down -v               # stop and delete all Kafka data
go build ./... && go test ./...      # compile and run the tests
```

## Responsible use

WikiPulse only reads public data and never edits, reverts or blocks anything on Wikipedia. Alerts are statistical signals for human review, not judgements about editors.
