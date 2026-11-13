# Event ingest API (v1)

This document is the contract between the FlightRecorder plug-in (the client) and the
companion server. The Rust types of the contract are in `src/wire.rs` and `src/eventlog.rs`
([Shared types](#shared-types)).

- The plug-in side: `src/sender.rs`, `src/spool.rs` and `src/http.rs`.
- The server side: `server/src/api.rs` and `server/src/store.rs` (`fr-server`).
  The server tests post the examples of this document (`server/tests/ingest.rs`).
- The plug-in tests use a small local receiver (`tests/common/mod.rs`).

## Summary

| Method and path | Token | Use |
|---|---|---|
| `POST /api/v1/events` | yes | A batch of events from one client and one session |
| `POST /api/v1/ping` | yes | The client is alive, with its counters |
| `GET /api/v1/health` | no | The server is up |

- Transport: plain HTTP on the LAN. HTTPS is not built yet ([#55](https://github.com/solutionscay/fm-flight-recorder/issues/55)).
- Body: JSON, UTF-8, `Content-Type: application/json`.
- Times: UTC, ISO-8601 with milliseconds and `Z`, for example `2026-11-08T14:03:07.215Z`.
  The JSONL log uses the same format.
- The server listens on port 8787 by default. The plug-in gets the address from the
  `endpoint=` option, for example `endpoint=http://192.168.1.10:8787`.

## Authentication

Each request to `/api/v1/events` and `/api/v1/ping` has this header:

```
Authorization: Bearer <event token>
```

The server makes one Event token. The FileMaker startup script gives it to the plug-in:

```
FlightRecorder_Start ( "endpoint=http://192.168.1.10:8787; token=<event token>" )
```

A missing or wrong token gives `401`. The token is the same for all clients of one server.
Over plain HTTP, other computers on the LAN can read the token and the events.

## Client identity

Each batch and each ping has a `client` object:

| Field | Example | Note |
|---|---|---|
| `install_id` | `"3f9c2a7e5b0d4e1f8a6c9b2d7e4f1a03"` | 32 hex characters. The plug-in makes it once and keeps it on disk. The server keys clients on it. |
| `name` | `"Front Desk iMac"` | The `client=` option, or else the macOS computer name. It can change. |
| `machine` | `"Front-Desk-iMac.local"` | Host name. |
| `os` | `"macOS 26.0.1"` | |
| `app` | `"Pro 26.0.3"` | The FileMaker version text that FileMaker gives the plug-in. |
| `plugin` | `"0.1.0"` | Plug-in version. |

All fields are strings. A field that the plug-in cannot read is an empty string.

## Sessions and event ids

- Each start of the recorder (FileMaker start, or `FlightRecorder_Start`) makes a new
  `session_id`: 32 random hex characters.
- Each event in a session has a `seq` number. The first event (`session_start`) has `seq` 1.
  The numbers go up by 1 in the order the plug-in writes the events to its log.
- The pair (`session_id`, `seq`) is the id of the event. The server keeps one copy of each id.
- A gap in `seq` is possible: the plug-in can drop events when its queue or its spool is full.
  The server must not wait for missing numbers.

## POST /api/v1/events

Request:

```json
{
  "v": 1,
  "session_id": "b6d0c1e2f3a4958677a8b9c0d1e2f3a4",
  "client": {
    "install_id": "3f9c2a7e5b0d4e1f8a6c9b2d7e4f1a03",
    "name": "Front Desk iMac",
    "machine": "Front-Desk-iMac.local",
    "os": "macOS 26.0.1",
    "app": "Pro 26.0.3",
    "plugin": "0.1.0"
  },
  "events": [
    {"v":1,"session_id":"b6d0c1e2f3a4958677a8b9c0d1e2f3a4","seq":7,"type":"freeze",
     "ts":"2026-11-08T16:45:01.012Z","end_ts":"2026-11-08T16:45:03.013Z","duration_ms":2001,
     "level":"user_not_idle","before":{"file":"Invoices","layout":"Customers | List"},
     "net":{"round_trips":1,"tx_bytes":4,"rx_bytes":3000,"tx_packets":1,"rx_packets":2,
            "srtt_ms":1,"wait_ms":2001,"connected":true}},
    {"v":1,"session_id":"b6d0c1e2f3a4958677a8b9c0d1e2f3a4","seq":8,"type":"mark",
     "ts":"2026-11-08T16:45:04.100Z","label":"go to Invoices","context":{"layout":"Invoices"}}
  ]
}
```

- `v` (top level) is the version of this envelope. It is 1.
- `events[]` items are the lines of the JSONL log, with the same fields
  ([Log format](https://github.com/solutionscay/fm-flight-recorder/wiki/Design#log-format) in the wiki). Each line has `seq` and `session_id`.
  The `session_id` of each line must be the same as the top-level `session_id`.
- The events in a batch are in `seq` order. A batch can repeat events that the server has
  (a retry).
- `session_start` has the client fields too, so a log file alone also identifies the client.

Replies:

| Status | Body | Meaning |
|---|---|---|
| `200` | `{"accepted": 2, "duplicates": 0}` | `accepted`: new events stored. `duplicates`: events that the server had already and dropped. |
| `400` | `{"error": "..."}` | The body is not valid JSON or does not follow this document. Do not send this batch again. |
| `401` | `{"error": "bad token"}` | Missing or wrong token. |
| `413` | `{"error": "batch too large"}` | Over the limits below. |
| `5xx` | any | Server error. Send the batch again later. |

A batch is all or nothing: on a status other than `200` the server stores no event of it.

### Limits

| Limit | Value |
|---|---|
| Events in one batch | 500 maximum |
| Bytes in one request body | 1 MiB (1 048 576) maximum |

The plug-in sends 200 events or 512 KiB in one batch at most, so it stays below the limits.

### When the plug-in sends

Two options of `FlightRecorder_Start` set the timing:

| Option | Default | Range | Meaning |
|---|---|---|---|
| `send_ms` | `5000` | 250 to 60000 | A batch goes out at most this long after its first event, or at once at 200 events. |
| `ping_ms` | `60000` | 5000 to 600000 | The interval of the ping. |

A value out of the range changes to the nearest limit. `FlightRecorder_Status` shows the
values in use (`send_ms`, `ping_ms`).

The plug-in writes a freeze when the freeze ends and the plug-in has the context after the
freeze (in less than 1.5 s, usually at once). Then the freeze waits for `send_ms`. For a dashboard
that is as live as possible, use `send_ms=250`:

```
FlightRecorder_Start ( "endpoint=http://192.168.1.10:8787; token=<event token>; send_ms=250; ping_ms=15000" )
```

A plug-in before these options ignores them (the result shows `ignored: send_ms, ping_ms`) and
uses the defaults.

### Long freezes: one event that grows

FileMaker can run the run loop for a moment in a long operation (about once a second in a long
script step, or between the steps of a long script). The plug-in joins these parts into one
freeze: a part that starts at most 300 ms after the end of the last part is the same freeze
(`join_ms=`, 0 to 1000). A sleep of the run loop that is shorter than this does not split the
freeze; a longer sleep (an idle UI) closes it.

While a freeze lasts, the plug-in sends a `freeze_progress` event about once a second, after
the first second. The option `progress_ms=` changes the interval: 250 to 30000 ms, or 0 for
no progress events. A larger value is set to 30000. A server closes an open freeze that has
had no update for 60 s (the companion server times this by when it received the update), so
the interval stays well inside that.

| Field | Meaning |
|---|---|
| `ts` | The start of the freeze. |
| `now_ts` | The time of this update. |
| `id` | The number of the freeze in the session: 1, 2, ... |
| `duration_ms` | From `ts` to `now_ts`. |
| `level`, `before` | As in `freeze`. |
| `parts` | The parts joined so far. |

When the freeze ends, the plug-in sends the usual `freeze` event, with `parts` (the parts
joined; an older plug-in leaves it out). Its `ts` is the same text as the `ts` of the
`freeze_progress` events of that freeze. A server that knows `freeze_progress`
shows one open freeze that grows, and the final `freeze` replaces it. An older server keeps
the `freeze_progress` events as unknown events, and stores the final `freeze` as before.

### Unknown fields and types

- The server ignores fields that it does not know, in the envelope and in the events.
- The server stores an event with an unknown `type` as it is and counts it in `accepted`.
  This lets an older server take events from a newer plug-in.

## POST /api/v1/ping

The plug-in sends a ping about every 60 s (`ping_ms`) while an endpoint is set. A client that has no
freezes sends no events, so the ping tells the server that the client is still connected.

Request:

```json
{
  "v": 1,
  "session_id": "b6d0c1e2f3a4958677a8b9c0d1e2f3a4",
  "client": { "install_id": "3f9c2a7e5b0d4e1f8a6c9b2d7e4f1a03", "name": "Front Desk iMac",
              "machine": "Front-Desk-iMac.local", "os": "macOS 26.0.1",
              "app": "Pro 26.0.3", "plugin": "0.1.0" },
  "ts": "2026-11-08T16:46:00.000Z",
  "running": true,
  "heartbeats": 123456,
  "idle_messages": 2345,
  "freezes": 4,
  "last_seq": 8
}
```

- The counters are the same as in `FlightRecorder_Status`, for this session.
- `last_seq` is the last `seq` that the plug-in wrote. The server can compare it with the
  events it has.
- Reply: `200` with `{}`, or `400`, `401` as for events.
- The plug-in does not send a failed ping again. The next ping replaces it.
- The server sets "last seen" for the client from a ping or a batch.

## GET /api/v1/health

No token. Reply `200` with `{"ok": true, "version": "<server version>"}`. Use it to check the
address from a browser or with `curl`.

## Retries and de-duplication

- The plug-in sends a batch again when it gets no reply, a `5xx`, or a network error.
  It waits longer after each failure (1 s, then 2 s, 4 s and so on, up to 5 min).
- When the server cannot be reached, the plug-in keeps the batches in a spool folder on disk,
  next to the log. It sends the spool first when the server can be reached again, also after
  a FileMaker restart.
- `401` stops the sending until the next `FlightRecorder_Start`. `FlightRecorder_Status` shows it.
- `400` and `413`: the plug-in drops the batch and counts it. It does not send it again.
- A resend is always safe: the server drops each event whose (`session_id`, `seq`) it has.

## Schema versions

There are two version numbers:

- `v` in each event line (`SCHEMA_VERSION` in `src/eventlog.rs`). It is 1.
- `v` in the request envelope, and `/v1/` in the path. It is 1.

Rules:

- A new field that readers can ignore does not change `v`. For example, `session_id`, `seq`
  and the client fields of `session_start` are optional, and the log is `v: 1`.
- A new event `type` does not change `v`.
- A change that an old reader would read wrongly (a field with a new meaning, a removed
  field, a new unit) increments the event `v`. The server must read all event versions that
  it knows.
- A change to the envelope or to the replies makes `/api/v2/`. The server keeps `/api/v1/`
  for older plug-ins.

## Shared types

The serde types of the contract are in the plug-in crate:

| Type | File | What |
|---|---|---|
| `Event` | `src/eventlog.rs` | The event body (`type` and its fields) |
| `Context`, `NetWindow` | `src/context.rs`, `src/net.rs` | Parts of the events |
| `EventLine` | `src/wire.rs` | One log line and one item of `events[]`: `v`, `session_id`, `seq` and the `Event` |
| `EventBatch`, `Ping`, `ClientInfo` | `src/wire.rs` | The request bodies |
| `IngestReply`, `ErrorReply` | `src/wire.rs` | The reply bodies |
| `MAX_BATCH_EVENTS`, `MAX_BATCH_BYTES` | `src/wire.rs` | The limits |

The tests in `src/wire.rs` parse the JSON examples in this document. If the document and the
types do not agree, the tests fail.

The server (`server/`, a member of the Cargo workspace) uses the same types from the plug-in
crate, so the format cannot drift.

## Other server calls

These calls are for the dashboard and for scripts. They need no token.

| Method and path | Use |
|---|---|
| `GET /api/v1/status` | Listen address, clients and connected clients, last event, server time zone and its UTC offset, versions, open live streams (`live_streams`) |
| `GET /api/v1/live` | Server-Sent Events for open pages: `hello` when the stream opens, `events` (data: the number of new events) after each stored batch, `client` when a client connects or goes offline. The stream has no event data. A comment line goes out each 15 s. 64 streams at most, then `503`. |
| `GET /api/v1/token` | The Event token |
| `POST /api/v1/token` | Make a new Event token. The old token gets `401` from then on. |
| `GET /api/v1/flags?range=&file=` | The flagged layouts, clients and plug-in versions |
| `GET /api/v1/freezes?range=&file=&layout=&account=&script=&client=&before=` | One page of freezes (100), with the link to the next page |

`range` is `today`, `7d`, or `custom` with `from=YYYY-MM-DD&to=YYYY-MM-DD`.
