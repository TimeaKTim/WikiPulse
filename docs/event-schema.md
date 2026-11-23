# Event schema

Everything downstream (stream processor, data lake, dashboards) builds on this contract.

## Data flow

```
EventStreams ──> collector ──┬──> bronze archive (hourly .jsonl.gz, source of truth)
                             └──> Kafka wiki.recentchange (live mode)
bronze archive ──> replay ──> Kafka wiki.recentchange (demo, reprocessing, filling gaps)
```

The collector keeps only the wikis in `WIKIS` (default `rowiki`). It writes to the archive first and to Kafka second, so if Kafka is down nothing is lost: the gap is replayed from the archive.

## Topic and message

| Item            | Value                                                                                     |
| --------------- | ----------------------------------------------------------------------------------------- |
| Topic           | `wiki.recentchange` (6 partitions, 72 h retention; created by `kafka-init`, no auto-create) |
| Key             | `<wiki>\|<title>` as UTF-8, so all edits of one page go to one partition                  |
| Value           | the original JSON event from Wikimedia EventStreams, **unchanged**                        |
| Header          | `wp-source`: `live` (collector) or `replay` (replay tool)                                 |
| Kafka timestamp | the time the message was written to Kafka. **Not** the event time, see rule 4             |
| Source          | https://stream.wikimedia.org/v2/stream/recentchange                                       |

## Fields we use

| Field                          | Type    | Notes                                                                                                          |
| ------------------------------ | ------- | -------------------------------------------------------------------------------------------------------------- |
| `meta.id`                      | string  | unique event ID; the idempotency key for deduplication                                                         |
| `meta.dt`                      | string  | ISO 8601 UTC time the event was published; **the event time** for windows, archive partitions and replay      |
| `meta.domain`                  | string  | e.g. `en.wikipedia.org`                                                                                        |
| `meta.topic`                   | string  | `eqiad.mediawiki.recentchange` or `codfw.mediawiki.recentchange`, one per Wikimedia datacenter                 |
| `id`                           | integer | recent-changes row ID; unique only within one wiki, so never use it as a global key                            |
| `type`                         | string  | `edit`, `new`, `log`, `categorize` or `external`                                                               |
| `namespace`                    | integer | 0 article, 1 talk, 2 user, 4 project, 6 file, ...                                                              |
| `title`                        | string  | page title, including the namespace prefix                                                                     |
| `user`                         | string  | user name or IP, exactly as in the public stream                                                               |
| `bot`                          | boolean | true if the edit was flagged as a bot edit                                                                     |
| `minor`                        | boolean | minor edit flag                                                                                                |
| `timestamp`                    | integer | Unix seconds of the change                                                                                     |
| `wiki`                         | string  | wiki ID, e.g. `enwiki`, `rowiki`, `commonswiki`, `wikidatawiki`                                                |
| `server_name`                  | string  | e.g. `en.wikipedia.org`                                                                                        |
| `comment`                      | string  | edit summary                                                                                                   |
| `length.old`, `length.new`     | integer | page size in bytes before and after; present only on `edit` and `new` events, absent on `categorize` and `log` |
| `revision.old`, `revision.new` | integer | revision IDs; may be absent                                                                                    |

Derived value: `bytes_delta = length.new - length.old`.

## Event types and wikis (sample)

First 20,000 events of the 2-hour all-wiki recording made on 6 Nov 2026. The mix changes with the time of day.

| type         | share | notes                                |
| ------------ | ----- | ------------------------------------ |
| `edit`       | 55%   | page edits                           |
| `categorize` | 36%   | category changes, not page edits     |
| `log`        | 6%    | log actions (moves, blocks, uploads) |
| `new`        | 3%    | new pages                            |

Top wikis: commonswiki 33%, wikidatawiki 23%, enwiki 10%, frwiki 3.5%, afwiki 2.6%, ruwiki 2.3%, dewiki 2.0%. `rowiki` is about 0.1%: 236 of 222,141 events, of which 150 `edit` or `new` (about 1.25 per minute).

## Rules for consumers

1. Ignore fields you do not know. Wikimedia may add fields at any time.
2. Treat `length` and `revision` as optional.
3. Deduplicate by `meta.id`. Delivery is at-least-once: after a crash the collector can resend up to about 5 seconds of events, and replaying a period twice sends it twice.
4. Use `meta.dt` as event time, for windows and watermarks as well as historical joins. Replay can run 60 times faster than real time, so the Kafka timestamp says nothing about when an edit happened; with `meta.dt` a 10-minute window is 10 minutes of edits at any replay speed. Events are not strictly ordered by `meta.dt` (two datacenter topics are interleaved), so allow a watermark delay of a few minutes. The Kafka timestamp is only useful for ingestion delay (`kafka timestamp - meta.dt`, live messages only).
5. Never modify the raw events. Store them as they are and derive everything else.
6. Alerts are statistical signals for human review, not judgements about editors.
7. Edit-rate metrics should count only `edit` and `new` events; `categorize` and `log` events are not page edits.

## Bronze archive

```
data/bronze/recentchange/date=YYYY-MM-DD/hour=HH/part-<run start>-<seq>.jsonl.gz
```

- One raw event JSON per line, exactly as received, gzipped.
- Partitioned by `meta.dt` in UTC, so a backfill lands in the right hours.
- Every collector run writes its own part files. Overlapping runs can store an event twice: deduplicate by `meta.id`.
- A file is finished (valid gzip end) when its hour has been quiet for 10 minutes or the collector stops. Batch jobs should read hours older than the current one; a file cut off by a crash is readable up to the cut.
- The layout is the same as S3 keys, so the folder can be synced to the object store as is.

## Collector behaviour

- **Live** (`go run ./cmd/collector`): archive + Kafka. Resumes from `.state/last_event_id` with `Last-Event-ID`.
- **Backfill** (`go run ./cmd/collector -since 7d`): archive only, from `since` (a time or a duration back, at most about 7 days) until the moment it started (or `-until`), then exits. EventStreams cannot filter by wiki, so a backfill reads the whole stream: about 1,600 events/s, so one hour of history takes about 75 s and **a week about 3.5 hours**.
- Sends a descriptive User-Agent (`WIKIPULSE_USER_AGENT` in `.env`).
- Reconnects with backoff (1 s up to 60 s) and treats 60 s of silence as a dead connection.
- Every 5 s it flushes the archive to disk and only then saves the resume point, so a restart never skips events that are not in the archive.
- Prometheus metrics on `:9101/metrics` (`-metrics ""` turns them off): `wikipulse_collector_events_received_total`, `_events_archived_total`, `_events_bad_total`, `_kafka_sent_total`, `_kafka_errors_total`, `_reconnects_total`, and `wikipulse_collector_stream_position_seconds` (`meta.dt` of the last event read; delay = `time() - position`).

## Replay

- `go run ./cmd/replay -speed 60` sends the whole archive to Kafka, paced by `meta.dt`: 1 is the original rate, 60 is sixty times faster, 0 is as fast as possible.
- `-from` and `-to` limit the period (a time or a duration back, e.g. `-from 7d`); `-loop` repeats.
- Within each hour events are deduplicated by `meta.id` and sorted by `meta.dt`.
- `-file path.jsonl.gz` replays one file instead, also the old recorder format (`{"ts", "key", "value"}` lines), e.g. the 2-hour all-wiki recording.

## Measured values

- Live rate, all wikis: about 30 events per second in the afternoon (about 60 at other times); 35 events/s on 23 Nov 2026, 12:30 local time.
- rowiki: about 1.25 `edit`/`new` events per minute (6 Nov afternoon), 84 events of all types between 05:00 and 07:00 UTC on 23 Nov.
- Backfill: 117,475 events read for one hour of history in 74 s, 84 kept for rowiki.
- Replay at maximum speed: 222,141 events (old recording) in 4.5 s; total count verified via topic offsets.
- Kafka down during live collection: 1,269 of 1,269 events archived, collector kept running; the next run resumed without gap or duplicates.
