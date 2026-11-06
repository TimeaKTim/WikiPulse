# WikiPulse

Real-time and historical analytics for Wikipedia activity. The live Wikimedia `recentchange` stream goes into Kafka, is processed in Go, and is shown on dashboards, alongside a historical batch path.

Project proposal and architecture review: see the course documents. Event format: [`docs/event-schema.md`](docs/event-schema.md).

## What works today

| Part                                                     | Status      | Owner                   |
| -------------------------------------------------------- | ----------- | ----------------------- |
| Kafka (Docker Compose) with topic `wiki.recentchange`    | done        | Konya Timea             |
| Go producer: EventStreams to Kafka, reconnect and resume | done        | Konya Timea             |
| Recorder and replay tools                                | done        | Konya Timea             |
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
go run ./cmd/producer    # live events -> Kafka (Ctrl+C to stop)
```

Wikimedia requires a descriptive User-Agent with contact info. The producer refuses to start while `.env` still contains the `example.com` placeholder. Keep the value in double quotes.

Check that events arrive:

```bash
docker exec wp-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic wiki.recentchange --from-beginning --max-messages 3
```

## Using the recorded stream (no internet needed)

A 2-hour recording (222,141 events) is the demo fallback and the test input for everything downstream. It is **not in git**: ask Timea for the file, put it at `data/recordings/stream.jsonl.gz`, then:

```bash
go run ./cmd/replay -speed 1     # original rate
go run ./cmd/replay -speed 10    # ten times faster
go run ./cmd/replay -speed 0     # as fast as possible (load test)
go run ./cmd/replay -loop        # repeat until Ctrl+C
```

Do not run the replay and the live producer at the same time, or both write to the topic.

To make a new recording, start the producer and run:

```bash
go run ./cmd/recorder -minutes 120
```

## How to connect to Kafka

| From                        | Address           |
| --------------------------- | ----------------- |
| Go programs on your machine | `localhost:29092` |
| Other containers in Compose | `kafka:9092`      |

Topic `wiki.recentchange`: 6 partitions, key `<wiki>|<title>`, value is the original event JSON, unchanged. Details and consumer rules are in [`docs/event-schema.md`](docs/event-schema.md). Notably, deduplicate by `meta.id`, because the producer is at-least-once.

## Project layout

```
cmd/producer/    EventStreams -> Kafka
cmd/recorder/    Kafka topic -> data/recordings/*.jsonl.gz
cmd/replay/      recording -> Kafka at original or accelerated speed
cmd/processor/   (planned) stream processing
cmd/ingester/    (planned) historical data loading
internal/        shared Go packages
docs/            event schema and other documentation
data/            local recordings (git-ignored)
.state/          producer state, last event ID (git-ignored)
```

## Measured so far

- Live stream: about 30 events/s in the afternoon, about 60 at other times of day.
- Replay at maximum speed: at least 66,000 events/s on one machine (222,141 events in 3.3 s including compilation), with the Kafka offsets verified before and after.
- The producer reconnects with backoff (1 s up to 60 s) and resumes from the saved event ID.

## Useful commands

```bash
docker compose ps -a                 # container status
docker compose down                  # stop (keeps data)
docker compose down -v               # stop and delete all Kafka data
go build ./...                       # check that everything compiles
```

## Responsible use

WikiPulse only reads public data and never edits, reverts or blocks anything on Wikipedia. Alerts are statistical signals for human review, not judgements about editors.
