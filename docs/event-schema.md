# Event schema

Everything downstream (processor, archive, dashboards) builds on this contract.

## Topic and message

| Item            | Value                                                                              |
| --------------- | ---------------------------------------------------------------------------------- |
| Topic           | `wiki.recentchange` (6 partitions)                                                 |
| Key             | `<wiki>\|<title>` as UTF-8, so all edits of one page go to one partition           |
| Value           | the original JSON event from Wikimedia EventStreams, **unchanged**                 |
| Kafka timestamp | roughly the time the producer wrote the event (live) or the replay time (replayed) |
| Source          | https://stream.wikimedia.org/v2/stream/recentchange                                |

## Fields we use

| Field                          | Type    | Notes                                                                                                          |
| ------------------------------ | ------- | -------------------------------------------------------------------------------------------------------------- |
| `meta.id`                      | string  | unique event ID; the idempotency key for deduplication                                                         |
| `meta.dt`                      | string  | ISO 8601 time the event was published                                                                          |
| `meta.domain`                  | string  | e.g. `en.wikipedia.org`                                                                                        |
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

First 20,000 events of the 2-hour recording made on 6 Nov 2026. The mix changes with the time of day.

| type         | share | notes                                |
| ------------ | ----- | ------------------------------------ |
| `edit`       | 55%   | page edits                           |
| `categorize` | 36%   | category changes, not page edits     |
| `log`        | 6%    | log actions (moves, blocks, uploads) |
| `new`        | 3%    | new pages                            |

Top wikis: commonswiki 33%, wikidatawiki 23%, enwiki 10%, frwiki 3.5%, afwiki 2.6%, ruwiki 2.3%, dewiki 2.0%. `rowiki` is below 2%.

## Rules for consumers

1. Ignore fields you do not know. Wikimedia may add fields at any time.
2. Treat `length` and `revision` as optional.
3. Deduplicate by `meta.id`. The producer is at-least-once: after a crash or reconnect it can send up to about 100 events twice.
4. For windows, use the Kafka message timestamp as event time, so live and replayed streams behave the same. The payload `timestamp` is the original time of the change and is the one to use for historical joins.
5. Never modify the raw events. Store them as they are and derive everything else.
6. Alerts are statistical signals for human review, not judgements about editors.
7. Edit-rate metrics should count only `edit` and `new` events; `categorize` and `log` events are not page edits.

## Producer behaviour

- Sends a descriptive User-Agent (`WIKIPULSE_USER_AGENT` in `.env`).
- Reconnects with backoff (1 s up to 60 s) and treats 60 s of silence as a dead connection.
- Saves the last event ID to `.state/last_event_id` and resumes with `Last-Event-ID`.
- Counts reconnections in its log line.

## Recording and replay

- `go run ./cmd/recorder -minutes N` writes `data/recordings/stream.jsonl.gz`.
- Each line is `{"ts": <ms>, "key": "...", "value": "<original event JSON as a string>"}`.
- `go run ./cmd/replay -speed S` writes it back to Kafka: 1 is the original rate, 10 is ten times faster, 0 is as fast as possible.
- Recordings are not in git. Ask Timea for a copy.

## Measured values

- Live rate: about 30 events per second in the afternoon (about 60 at other times).
- Recording: 222,141 events over about 2 hours, verified by line count.
- Replay at maximum speed: at least 66,000 events per second (222,141 events in 3.3 s including compilation, single local broker); total count verified via topic offsets.
