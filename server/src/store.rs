//! Store batches and pings (docs/ingest-api.md).
//!
//! A batch is all or nothing: it is checked first, then written in one
//! transaction. `(session_id, seq)` is the id of an event; a known id is a
//! duplicate and is dropped.

use crate::db::Db;
use crate::tz::{parse_ts, Zone};
use chrono::NaiveDate;
use fm_flight_recorder::context::Context;
use fm_flight_recorder::eventlog::Event;
use fm_flight_recorder::net::NetWindow;
use fm_flight_recorder::wire::{
    ClientInfo, EventLine, IngestReply, Ping, API_VERSION, MAX_BATCH_EVENTS,
};
use rusqlite::{params, OptionalExtension};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, PartialEq)]
pub enum StoreError {
    /// 400: the body does not follow the API.
    Bad(String),
    /// 413: over the limits.
    TooLarge,
    /// 5xx: the database failed.
    Db(String),
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Db(e.to_string())
    }
}

/// The envelope of `POST /api/v1/events`. The events stay JSON values
/// first, so an unknown event type can be stored as it is.
#[derive(Debug, Deserialize)]
pub struct Batch {
    pub v: u32,
    pub session_id: String,
    pub client: ClientInfo,
    pub events: Vec<Value>,
}

/// Where a batch comes from and what "now" is.
#[derive(Debug, Clone, Copy)]
pub struct Meta<'a> {
    /// The client's IP address ("" when not known).
    pub ip: &'a str,
    /// "Last seen" for the client and the session.
    pub seen_ms: i64,
    pub zone: Zone,
    /// Events before this day are past the event retention: they are kept
    /// for de-duplication, but do not change the daily totals.
    pub cutoff_day: NaiveDate,
}

pub fn parse_batch(body: &[u8]) -> Result<Batch, StoreError> {
    let b: Batch =
        serde_json::from_slice(body).map_err(|e| StoreError::Bad(format!("bad JSON: {e}")))?;
    if b.v != API_VERSION {
        return Err(StoreError::Bad(format!("unknown envelope version {}", b.v)));
    }
    if b.events.len() > MAX_BATCH_EVENTS {
        return Err(StoreError::TooLarge);
    }
    if b.session_id.is_empty() {
        return Err(StoreError::Bad("session_id is required".into()));
    }
    Ok(b)
}

pub fn parse_ping(body: &[u8]) -> Result<Ping, StoreError> {
    let p: Ping =
        serde_json::from_slice(body).map_err(|e| StoreError::Bad(format!("bad JSON: {e}")))?;
    if p.v != API_VERSION {
        return Err(StoreError::Bad(format!("unknown envelope version {}", p.v)));
    }
    if p.session_id.is_empty() {
        return Err(StoreError::Bad("session_id is required".into()));
    }
    Ok(p)
}

const KNOWN: [&str; 6] = [
    "session_start",
    "freeze",
    "freeze_progress",
    "script_busy",
    "mark",
    "session_stop",
];

/// One event, checked and ready for the `events` table.
#[derive(Debug, Default, Clone)]
pub struct Row {
    pub seq: u64,
    pub kind: String,
    pub ts_ms: i64,
    pub end_ms: Option<i64>,
    pub duration_ms: u64,
    pub net: NetWindow,
    pub level: String,
    pub file: String,
    pub layout: String,
    pub layout_table: String,
    pub from_layout: String,
    pub script: String,
    pub account: String,
    pub user: String,
    pub label: String,
    pub marks: Vec<String>,
    pub before: Option<Context>,
    pub after: Option<Context>,
    pub raw: String,
    pub event: Option<Event>,
    /// `freeze_progress`: the freeze id (from 1) and the parts so far.
    pub freeze_id: u64,
    pub parts: u32,
}

fn pick(a: &str, b: &str) -> String {
    if a.is_empty() { b } else { a }.to_string()
}

/// Check one item of `events[]` and turn it into a row.
pub fn to_row(i: usize, v: &Value, session_id: &str, fallback_ms: i64) -> Result<Row, StoreError> {
    let bad = |m: &str| StoreError::Bad(format!("events[{i}]: {m}"));
    let o = v.as_object().ok_or_else(|| bad("not an object"))?;
    let seq = o
        .get("seq")
        .and_then(Value::as_u64)
        .filter(|s| *s >= 1)
        .ok_or_else(|| bad("seq must be a number from 1"))?;
    match o.get("session_id").and_then(Value::as_str) {
        Some(s) if s == session_id => {}
        _ => return Err(bad("session_id is not the batch session_id")),
    }
    let kind = o
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| bad("type is required"))?
        .to_string();
    let mut row = Row {
        seq,
        kind: kind.clone(),
        raw: v.to_string(),
        ..Default::default()
    };
    if !KNOWN.contains(&kind.as_str()) {
        // A newer plug-in: keep it as it is.
        row.ts_ms = o
            .get("ts")
            .and_then(Value::as_str)
            .and_then(parse_ts)
            .unwrap_or(fallback_ms);
        return Ok(row);
    }
    let line: EventLine =
        serde_json::from_value(v.clone()).map_err(|e| bad(&format!("{kind}: {e}")))?;
    let ts = |s: &str| parse_ts(s).ok_or_else(|| bad(&format!("bad time {s:?}")));
    match &line.event {
        Event::SessionStart { ts: t, .. }
        | Event::SessionStop { ts: t, .. }
        | Event::Mark { ts: t, .. } => row.ts_ms = ts(t)?,
        Event::Freeze { ts: t, end_ts, .. }
        | Event::ScriptBusy { ts: t, end_ts, .. }
        | Event::FreezeProgress {
            ts: t,
            now_ts: end_ts,
            ..
        } => {
            row.ts_ms = ts(t)?;
            row.end_ms = Some(ts(end_ts)?);
        }
    }
    match &line.event {
        Event::Freeze {
            duration_ms,
            level,
            before,
            after,
            net,
            marks,
            parts,
            ..
        } => {
            let a = after.clone().unwrap_or_default();
            row.parts = *parts;
            row.duration_ms = *duration_ms;
            row.level = level.clone();
            row.net = *net;
            row.file = pick(&a.file, &before.file);
            row.layout = pick(&a.layout, &before.layout);
            row.layout_table = if a.layout.is_empty() {
                before.layout_table.clone()
            } else {
                a.layout_table.clone()
            };
            row.from_layout = before.layout.clone();
            row.script = pick(&before.script, &a.script);
            row.account = pick(&a.account, &before.account);
            row.user = pick(&a.user, &before.user);
            row.marks = marks.clone();
            row.before = Some(before.clone());
            row.after = after.clone();
        }
        Event::ScriptBusy {
            duration_ms,
            before,
            net,
            ..
        } => {
            row.duration_ms = *duration_ms;
            row.net = *net;
            row.file = before.file.clone();
            row.layout = before.layout.clone();
            row.layout_table = before.layout_table.clone();
            row.from_layout = before.layout.clone();
            row.script = before.script.clone();
            row.account = before.account.clone();
            row.user = before.user.clone();
            row.before = Some(before.clone());
        }
        Event::FreezeProgress {
            id,
            duration_ms,
            level,
            before,
            parts,
            ..
        } => {
            if *id == 0 {
                return Err(bad("id must be a number from 1"));
            }
            row.freeze_id = *id;
            row.parts = *parts;
            row.duration_ms = *duration_ms;
            row.level = level.clone();
            row.file = before.file.clone();
            row.layout = before.layout.clone();
            row.layout_table = before.layout_table.clone();
            row.from_layout = before.layout.clone();
            row.script = before.script.clone();
            row.account = before.account.clone();
            row.user = before.user.clone();
            row.before = Some(before.clone());
        }
        Event::Mark { label, context, .. } => {
            row.label = label.clone();
            row.file = context.file.clone();
            row.layout = context.layout.clone();
            row.layout_table = context.layout_table.clone();
            row.script = context.script.clone();
            row.account = context.account.clone();
            row.user = context.user.clone();
            row.before = Some(context.clone());
        }
        _ => {}
    }
    row.event = Some(line.event);
    Ok(row)
}

/// The key the server uses for a client: its install id, or its name when
/// the plug-in could not make an install id.
fn client_key(c: &ClientInfo) -> Result<String, StoreError> {
    if !c.install_id.is_empty() {
        Ok(c.install_id.clone())
    } else if !c.name.is_empty() {
        Ok(format!("name:{}", c.name))
    } else {
        Err(StoreError::Bad("client.install_id is required".into()))
    }
}

/// `last_seen_ms` of a client, if the server knows it.
pub fn client_last_seen(db: &Db, c: &ClientInfo) -> Option<i64> {
    let key = client_key(c).ok()?;
    db.query_row(
        "SELECT last_seen_ms FROM clients WHERE install_id = ?1",
        [key],
        |r| r.get(0),
    )
    .optional()
    .ok()
    .flatten()
}

pub fn upsert_client(
    tx: &rusqlite::Transaction,
    c: &ClientInfo,
    ip: &str,
    seen_ms: i64,
) -> Result<i64, StoreError> {
    let key = client_key(c)?;
    let id = tx.query_row(
        "INSERT INTO clients(install_id, name, machine, os, app, plugin, ip, first_seen_ms, last_seen_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
         ON CONFLICT(install_id) DO UPDATE SET
           name    = CASE WHEN excluded.name    <> '' THEN excluded.name    ELSE name    END,
           machine = CASE WHEN excluded.machine <> '' THEN excluded.machine ELSE machine END,
           os      = CASE WHEN excluded.os      <> '' THEN excluded.os      ELSE os      END,
           app     = CASE WHEN excluded.app     <> '' THEN excluded.app     ELSE app     END,
           plugin  = CASE WHEN excluded.plugin  <> '' THEN excluded.plugin  ELSE plugin  END,
           ip      = CASE WHEN excluded.ip      <> '' THEN excluded.ip      ELSE ip      END,
           first_seen_ms = min(first_seen_ms, excluded.first_seen_ms),
           last_seen_ms  = max(last_seen_ms, excluded.last_seen_ms)
         RETURNING id",
        params![key, c.name, c.machine, c.os, c.app, c.plugin, ip, seen_ms],
        |r| r.get(0),
    )?;
    Ok(id)
}

fn upsert_session(
    tx: &rusqlite::Transaction,
    session_id: &str,
    client_id: i64,
    c: &ClientInfo,
    seen_ms: i64,
) -> Result<(), StoreError> {
    tx.execute(
        "INSERT INTO sessions(session_id, client_id, first_seen_ms, last_seen_ms, app, plugin)
         VALUES (?1, ?2, ?3, ?3, ?4, ?5)
         ON CONFLICT(session_id) DO UPDATE SET
           last_seen_ms = max(last_seen_ms, excluded.last_seen_ms),
           stop_ms = CASE WHEN closed = 'offline' THEN NULL ELSE stop_ms END,
           closed  = CASE WHEN closed = 'offline' THEN '' ELSE closed END,
           app    = CASE WHEN app    = '' THEN excluded.app    ELSE app    END,
           plugin = CASE WHEN plugin = '' THEN excluded.plugin ELSE plugin END",
        params![session_id, client_id, seen_ms, c.app, c.plugin],
    )?;
    Ok(())
}

/// Store a parsed batch. Returns the counts for the `200` reply.
pub fn store_batch(db: &mut Db, b: &Batch, m: Meta) -> Result<IngestReply, StoreError> {
    let rows = b
        .events
        .iter()
        .enumerate()
        .map(|(i, v)| to_row(i, v, &b.session_id, m.seen_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let tx = db.transaction()?;
    let client_id = upsert_client(&tx, &b.client, m.ip, m.seen_ms)?;
    upsert_session(&tx, &b.session_id, client_id, &b.client, m.seen_ms)?;
    let mut reply = IngestReply::default();
    {
        let mut ins = tx.prepare_cached(
            "INSERT OR IGNORE INTO events(session_id, seq, client_id, type, ts_ms, end_ms,
               duration_ms, wait_ms, round_trips, srtt_ms, tx_bytes, rx_bytes, tx_packets,
               rx_packets, connected, level, file, layout, layout_table, from_layout, script,
               account, user, label, marks, before_json, after_json, raw, parts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
               ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29)",
        )?;
        for r in &rows {
            // A long freeze: one row that grows, then the final replaces it.
            let open = match r.kind.as_str() {
                "freeze_progress" => {
                    Some(if progress(&tx, &b.session_id, client_id, r, m.seen_ms)? {
                        Open::Done
                    } else {
                        Open::Duplicate
                    })
                }
                "freeze" => replace_open(&tx, &b.session_id, r)?,
                _ => None,
            };
            match open {
                Some(Open::Done) => {
                    reply.accepted += 1;
                    mark_dirty(&tx, &m, r.ts_ms)?;
                    continue;
                }
                Some(Open::Duplicate) => {
                    reply.duplicates += 1;
                    continue;
                }
                None => {}
            }
            let n = &r.net;
            let added = ins.execute(params![
                b.session_id,
                r.seq as i64,
                client_id,
                r.kind,
                r.ts_ms,
                r.end_ms,
                r.duration_ms as i64,
                n.wait_ms as i64,
                n.round_trips as i64,
                n.srtt_ms as i64,
                n.tx_bytes as i64,
                n.rx_bytes as i64,
                n.tx_packets as i64,
                n.rx_packets as i64,
                n.connected,
                r.level,
                r.file,
                r.layout,
                r.layout_table,
                r.from_layout,
                r.script,
                r.account,
                r.user,
                r.label,
                serde_json::to_string(&r.marks).unwrap(),
                r.before
                    .as_ref()
                    .map(|c| serde_json::to_string(c).unwrap())
                    .unwrap_or_else(|| "{}".into()),
                r.after
                    .as_ref()
                    .map(|c| serde_json::to_string(c).unwrap())
                    .unwrap_or_default(),
                r.raw,
                r.parts as i64,
            ])?;
            if added == 0 {
                reply.duplicates += 1;
                continue;
            }
            reply.accepted += 1;
            apply_to_session(&tx, &b.session_id, r)?;
            if r.kind == "freeze" || r.kind == "script_busy" {
                mark_dirty(&tx, &m, r.ts_ms)?;
            }
        }
    }
    tx.commit()?;
    Ok(reply)
}

/// The day of `ts_ms` needs a new rollup (if it is in the event retention).
fn mark_dirty(tx: &rusqlite::Transaction, m: &Meta, ts_ms: i64) -> Result<(), StoreError> {
    let day = m.zone.date_of(ts_ms);
    if day >= m.cutoff_day {
        tx.execute(
            "INSERT OR IGNORE INTO rollup_dirty(day) VALUES (?1)",
            [day.to_string()],
        )?;
    }
    Ok(())
}

enum Open {
    /// The open freeze row changed.
    Done,
    /// Nothing changed (a resend, or the freeze is closed already).
    Duplicate,
}

/// `freeze_progress`: make the open row of the freeze, or let it grow. A
/// closed freeze (final event stored, or closed by the server) stays as it
/// is. The open row counts once in the session (freezes + 1).
fn progress(
    tx: &rusqlite::Transaction,
    session_id: &str,
    client_id: i64,
    r: &Row,
    seen_ms: i64,
) -> Result<bool, StoreError> {
    let id = r.freeze_id as i64;
    let d = r.duration_ms as i64;
    let found: Option<(i64, i64)> = tx
        .query_row(
            "SELECT open, duration_ms FROM events WHERE session_id = ?1 AND freeze_id = ?2",
            params![session_id, id],
            |x| Ok((x.get(0)?, x.get(1)?)),
        )
        .optional()?;
    match found {
        None => {
            // The final freeze can be here already (an old log read again).
            let done = tx
                .query_row(
                    "SELECT 1 FROM events WHERE session_id = ?1 AND type = 'freeze'
                       AND ts_ms = ?2 AND seq > 0",
                    params![session_id, r.ts_ms],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if done {
                return Ok(false);
            }
            tx.execute(
                "INSERT INTO events(session_id, seq, client_id, type, ts_ms, end_ms, duration_ms,
                   level, file, layout, layout_table, from_layout, script, account, user,
                   before_json, raw, freeze_id, open, parts, seen_ms)
                 VALUES (?1, ?2, ?3, 'freeze', ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                   ?15, ?16, ?17, 1, ?18, ?19)",
                params![
                    session_id,
                    -id,
                    client_id,
                    r.ts_ms,
                    r.end_ms,
                    d,
                    r.level,
                    r.file,
                    r.layout,
                    r.layout_table,
                    r.from_layout,
                    r.script,
                    r.account,
                    r.user,
                    r.before
                        .as_ref()
                        .map(|c| serde_json::to_string(c).unwrap())
                        .unwrap_or_else(|| "{}".into()),
                    r.raw,
                    id,
                    r.parts as i64,
                    seen_ms,
                ],
            )?;
            tx.execute(
                "UPDATE sessions SET freezes = freezes + 1, frozen_ms = frozen_ms + ?2
                 WHERE session_id = ?1",
                params![session_id, d],
            )?;
            Ok(true)
        }
        Some((1, old)) if d > old => {
            tx.execute(
                "UPDATE events SET duration_ms = ?3, end_ms = ?4, parts = ?5, raw = ?6, seen_ms = ?7
                 WHERE session_id = ?1 AND freeze_id = ?2",
                params![session_id, id, d, r.end_ms, r.parts as i64, r.raw, seen_ms],
            )?;
            tx.execute(
                "UPDATE sessions SET frozen_ms = frozen_ms + ?2 WHERE session_id = ?1",
                params![session_id, d - old],
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// A final `freeze`: if progress events made a row for it (same session,
/// same start), the final replaces that row (real seq, open = 0). None: no
/// such row, store it as usual.
fn replace_open(
    tx: &rusqlite::Transaction,
    session_id: &str,
    r: &Row,
) -> Result<Option<Open>, StoreError> {
    let found: Option<(i64, i64, i64)> = tx
        .query_row(
            "SELECT id, seq, duration_ms FROM events
             WHERE session_id = ?1 AND freeze_id > 0 AND type = 'freeze' AND ts_ms = ?2",
            params![session_id, r.ts_ms],
            |x| Ok((x.get(0)?, x.get(1)?, x.get(2)?)),
        )
        .optional()?;
    let Some((id, seq, old)) = found else {
        return Ok(None);
    };
    if seq > 0 {
        return Ok(Some(Open::Duplicate));
    }
    let taken = tx
        .query_row(
            "SELECT 1 FROM events WHERE session_id = ?1 AND seq = ?2",
            params![session_id, r.seq as i64],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if taken {
        return Ok(Some(Open::Duplicate));
    }
    let n = &r.net;
    tx.execute(
        "UPDATE events SET seq = ?2, open = 0, end_ms = ?3, duration_ms = ?4, wait_ms = ?5,
           round_trips = ?6, srtt_ms = ?7, tx_bytes = ?8, rx_bytes = ?9, tx_packets = ?10,
           rx_packets = ?11, connected = ?12, level = ?13, file = ?14, layout = ?15,
           layout_table = ?16, from_layout = ?17, script = ?18, account = ?19, user = ?20,
           marks = ?21, before_json = ?22, after_json = ?23, raw = ?24,
           parts = MAX(parts, ?25)
         WHERE id = ?1",
        params![
            id,
            r.seq as i64,
            r.end_ms,
            r.duration_ms as i64,
            n.wait_ms as i64,
            n.round_trips as i64,
            n.srtt_ms as i64,
            n.tx_bytes as i64,
            n.rx_bytes as i64,
            n.tx_packets as i64,
            n.rx_packets as i64,
            n.connected,
            r.level,
            r.file,
            r.layout,
            r.layout_table,
            r.from_layout,
            r.script,
            r.account,
            r.user,
            serde_json::to_string(&r.marks).unwrap(),
            r.before
                .as_ref()
                .map(|c| serde_json::to_string(c).unwrap())
                .unwrap_or_else(|| "{}".into()),
            r.after
                .as_ref()
                .map(|c| serde_json::to_string(c).unwrap())
                .unwrap_or_default(),
            r.raw,
            r.parts as i64,
        ],
    )?;
    tx.execute(
        "UPDATE sessions SET frozen_ms = frozen_ms + ?2 WHERE session_id = ?1",
        params![session_id, r.duration_ms as i64 - old],
    )?;
    Ok(Some(Open::Done))
}

/// A new event changes its session: start, stop and freeze counts.
fn apply_to_session(
    tx: &rusqlite::Transaction,
    session_id: &str,
    r: &Row,
) -> Result<(), StoreError> {
    match &r.event {
        Some(Event::SessionStart {
            pid,
            app,
            plugin,
            threshold_ms,
            heartbeat,
            port,
            ..
        }) => {
            let opts = serde_json::from_str::<Value>(&r.raw)
                .map(|mut v| {
                    if let Some(o) = v.as_object_mut() {
                        for k in ["v", "session_id", "seq", "type", "ts"] {
                            o.remove(k);
                        }
                    }
                    v.to_string()
                })
                .unwrap_or_default();
            tx.execute(
                "UPDATE sessions SET start_ms = ?2, pid = ?3, app = ?4, plugin = ?5,
                   threshold_ms = ?6, heartbeat = ?7, port = ?8, options = ?9
                 WHERE session_id = ?1",
                params![
                    session_id,
                    r.ts_ms,
                    pid,
                    app,
                    plugin,
                    *threshold_ms as i64,
                    heartbeat,
                    port,
                    opts
                ],
            )?;
        }
        Some(Event::SessionStop {
            heartbeats,
            idle_messages,
            ..
        }) => {
            tx.execute(
                "UPDATE sessions SET stop_ms = ?2, closed = 'stop',
                   heartbeats = max(heartbeats, ?3), idle_messages = max(idle_messages, ?4)
                 WHERE session_id = ?1",
                params![
                    session_id,
                    r.ts_ms,
                    *heartbeats as i64,
                    *idle_messages as i64
                ],
            )?;
            // The session ended: an open freeze stays at its last update.
            tx.execute(
                "UPDATE events SET open = 0 WHERE session_id = ?1 AND open = 1",
                [session_id],
            )?;
        }
        Some(Event::Freeze { duration_ms, .. }) => {
            tx.execute(
                "UPDATE sessions SET freezes = freezes + 1, frozen_ms = frozen_ms + ?2
                 WHERE session_id = ?1",
                params![session_id, *duration_ms as i64],
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// Store a ping: the client is alive, with its counters.
pub fn store_ping(db: &mut Db, p: &Ping, ip: &str, now_ms: i64) -> Result<(), StoreError> {
    let tx = db.transaction()?;
    let client_id = upsert_client(&tx, &p.client, ip, now_ms)?;
    upsert_session(&tx, &p.session_id, client_id, &p.client, now_ms)?;
    tx.execute(
        "UPDATE sessions SET heartbeats = max(heartbeats, ?2), idle_messages = max(idle_messages, ?3),
           ping_freezes = ?4, last_seq = max(last_seq, ?5)
         WHERE session_id = ?1",
        params![
            p.session_id,
            p.heartbeats as i64,
            p.idle_messages as i64,
            p.freezes as i64,
            p.last_seq as i64
        ],
    )?;
    tx.commit()?;
    Ok(())
}

/// Time of the newest event, if any.
pub fn last_event_ms(db: &Db) -> Option<i64> {
    db.query_row("SELECT max(ts_ms) FROM events", [], |r| r.get(0))
        .optional()
        .ok()
        .flatten()
        .flatten()
}
