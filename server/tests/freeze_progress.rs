//! A long freeze that still lasts: `freeze_progress` events make one open
//! row that grows, the final `freeze` (same session, same ts) replaces it,
//! and the pages show the open row with an "ongoing" mark.

mod common;

use common::*;
use fm_flight_recorder::wire::{IngestReply, PATH_EVENTS};
use fr_server::db::{self, Db};
use fr_server::rollup;
use fr_server::store::{store_batch, Batch, Meta};
use fr_server::tz::{iso, Zone};
use serde_json::{json, Value};

const SID: &str = "5e55105e55105e55105e55105e551055";
const START: &str = "2026-11-11T22:35:29.748Z";

fn zone() -> Zone {
    Zone::new("America/Chicago").unwrap()
}

fn client() -> Value {
    json!({"install_id": "00112233445566778899aabbccddeeff", "name": "Test Mac"})
}

fn ctx() -> Value {
    json!({"file": "Accounting", "layout": "Ticket Report Data", "script": "Ticket Report - post ARC to AR", "account": "jl"})
}

fn progress(seq: u64, ms: u64, parts: u32) -> Value {
    json!({"v": 1, "session_id": SID, "seq": seq, "type": "freeze_progress", "ts": START,
           "now_ts": "2026-11-11T22:35:40.000Z", "id": 1, "duration_ms": ms,
           "level": "unsafe", "before": ctx(), "parts": parts})
}

fn final_freeze(seq: u64, ms: u64) -> Value {
    json!({"v": 1, "session_id": SID, "seq": seq, "type": "freeze", "ts": START,
           "end_ts": "2026-11-11T22:35:37.196Z", "duration_ms": ms, "level": "unsafe",
           "before": ctx(), "after": ctx(),
           "net": {"round_trips": 3, "tx_bytes": 1, "rx_bytes": 1, "tx_packets": 1,
                   "rx_packets": 1, "srtt_ms": 1, "wait_ms": 400, "connected": true}})
}

fn stop(seq: u64) -> Value {
    json!({"v": 1, "session_id": SID, "seq": seq, "type": "session_stop",
           "ts": "2026-11-11T22:36:00.000Z", "freezes": 1, "heartbeats": 10})
}

fn batch(events: Vec<Value>) -> Batch {
    serde_json::from_value(json!({"v": 1, "session_id": SID, "client": client(), "events": events}))
        .unwrap()
}

fn put(db: &mut Db, events: Vec<Value>) -> IngestReply {
    put_at(db, events, fr_server::tz::parse_ts(START).unwrap())
}

/// Store a batch that the server got at `now` (its clock).
fn put_at(db: &mut Db, events: Vec<Value>, now: i64) -> IngestReply {
    let m = Meta {
        ip: "10.0.0.2",
        seen_ms: now,
        zone: zone(),
        cutoff_day: rollup::cutoff_day(zone(), 30, now),
    };
    store_batch(db, &batch(events), m).unwrap()
}

fn one(db: &Db, sql: &str) -> i64 {
    db.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn row(db: &Db) -> (i64, i64, i64, i64) {
    db.query_row(
        "SELECT count(*), max(open), max(seq), max(duration_ms) FROM events WHERE type = 'freeze'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
    .unwrap()
}

#[test]
fn progress_makes_one_open_row_that_grows_and_the_final_replaces_it() {
    let mut db = db::open_memory();
    let r = put(&mut db, vec![progress(3, 1000, 1)]);
    assert_eq!((r.accepted, r.duplicates), (1, 0));
    assert_eq!(row(&db), (1, 1, -1, 1000));
    put(&mut db, vec![progress(4, 2000, 2), progress(5, 3000, 3)]);
    assert_eq!(row(&db), (1, 1, -1, 3000));
    assert_eq!(one(&db, "SELECT parts FROM events"), 3);
    // The session counts the open freeze once.
    assert_eq!(one(&db, "SELECT freezes FROM sessions"), 1);
    assert_eq!(one(&db, "SELECT frozen_ms FROM sessions"), 3000);

    // The final freeze replaces the row: real seq, closed, full data.
    let r = put(&mut db, vec![final_freeze(6, 7448)]);
    assert_eq!((r.accepted, r.duplicates), (1, 0));
    assert_eq!(row(&db), (1, 0, 6, 7448));
    assert_eq!(one(&db, "SELECT wait_ms FROM events"), 400);
    // A final with no `parts` (older plug-in) keeps the parts of the progress.
    assert_eq!(one(&db, "SELECT parts FROM events"), 3);
    assert_eq!(one(&db, "SELECT freezes FROM sessions"), 1);
    assert_eq!(one(&db, "SELECT frozen_ms FROM sessions"), 7448);

    // A resend of the final, and a late progress: no change.
    let r = put(&mut db, vec![final_freeze(6, 7448), progress(5, 3000, 3)]);
    assert_eq!((r.accepted, r.duplicates), (0, 2));
    assert_eq!(row(&db), (1, 0, 6, 7448));
}

#[test]
fn a_smaller_or_repeated_progress_changes_nothing() {
    let mut db = db::open_memory();
    put(&mut db, vec![progress(3, 2000, 2)]);
    let r = put(&mut db, vec![progress(3, 2000, 2), progress(2, 1000, 1)]);
    assert_eq!((r.accepted, r.duplicates), (0, 2));
    assert_eq!(row(&db), (1, 1, -1, 2000));
}

#[test]
fn a_freeze_with_no_progress_is_stored_as_before() {
    let mut db = db::open_memory();
    put(&mut db, vec![final_freeze(3, 900)]);
    assert_eq!(row(&db), (1, 0, 3, 900));
    assert_eq!(one(&db, "SELECT freeze_id FROM events"), 0);
}

#[test]
fn session_stop_closes_the_open_freeze_at_its_last_update() {
    let mut db = db::open_memory();
    put(&mut db, vec![progress(3, 4000, 4), stop(4)]);
    assert_eq!(row(&db), (1, 0, -1, 4000));
    // The final can still come later (from the spool): it replaces the row.
    put(&mut db, vec![final_freeze(5, 4100)]);
    assert_eq!(row(&db), (1, 0, 5, 4100));
}

#[test]
fn a_silent_open_freeze_closes_after_the_stale_time() {
    let mut db = db::open_memory();
    put(&mut db, vec![progress(3, 4000, 4)]);
    let last = fr_server::tz::parse_ts("2026-11-11T22:35:40.000Z").unwrap();
    assert_eq!(rollup::close_open_freezes(&db, last + 30_000).unwrap(), 0);
    assert_eq!(
        rollup::close_open_freezes(&db, last + rollup::OPEN_STALE_MS + 1).unwrap(),
        1
    );
    assert_eq!(row(&db).1, 0);
}

#[test]
fn the_stale_time_is_by_the_server_clock_not_the_client_clock() {
    // The client's clock is 2 minutes slow: its now_ts (22:35:40) is 2 min
    // before the server got the update.
    let mut db = db::open_memory();
    let client_now = fr_server::tz::parse_ts("2026-11-11T22:35:40.000Z").unwrap();
    let got = client_now + 120_000;
    put_at(&mut db, vec![progress(3, 4000, 4)], got);
    assert_eq!(rollup::close_open_freezes(&db, got + 30_000).unwrap(), 0);
    assert_eq!(row(&db).1, 1, "still open 30 s after the last update");
    // The next update (server clock) moves the stale time on.
    put_at(&mut db, vec![progress(4, 5000, 5)], got + 50_000);
    assert_eq!(
        rollup::close_open_freezes(&db, got + 50_000 + rollup::OPEN_STALE_MS - 1).unwrap(),
        0
    );
    assert_eq!(
        rollup::close_open_freezes(&db, got + 50_000 + rollup::OPEN_STALE_MS + 1).unwrap(),
        1
    );
}

#[test]
fn rollups_count_the_open_freeze_once_then_the_final() {
    let mut db = db::open_memory();
    let day = zone().date_of(fr_server::tz::parse_ts(START).unwrap());
    put(&mut db, vec![progress(3, 2000, 2)]);
    rollup::rollup_day(&mut db, zone(), day).unwrap();
    let tot = |db: &Db| -> (i64, i64) {
        db.query_row(
            "SELECT sum(n), sum(total_ms) FROM daily_totals WHERE kind = 'freeze'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    };
    assert_eq!(tot(&db), (1, 2000));
    put(&mut db, vec![final_freeze(4, 7448)]);
    rollup::rollup_day(&mut db, zone(), day).unwrap();
    assert_eq!(tot(&db), (1, 7448));
}

/// Over HTTP, with today's time: the overview and the Freezes page show the
/// open freeze with the "ongoing" mark, and one row only after the final.
#[test]
fn the_pages_show_an_ongoing_freeze_then_the_final_row() {
    let (app, base, _d) = server("ongoing");
    let token = app.token();
    let now = fr_server::tz::now_ms();
    let start = iso(now - 5_000);
    let post = |e: Value| {
        let mut e = e;
        e["ts"] = json!(start);
        let body = json!({"v": 1, "session_id": SID, "client": client(), "events": [e]});
        let r = post_json(&base, PATH_EVENTS, Some(&token), &body.to_string());
        assert_eq!(r.status, 200, "{}", r.body);
    };
    post(progress(3, 4000, 4));
    for path in ["/", "/freezes"] {
        let p = get(&base, path);
        assert!(p.body.contains("ongoing"), "{path}: no ongoing mark");
        assert!(p.body.contains("Ticket Report Data"), "{path}");
    }
    post(final_freeze(4, 4500));
    assert_eq!(
        count(&app, "SELECT count(*) FROM events WHERE type = 'freeze'"),
        1
    );
    for path in ["/", "/freezes"] {
        let p = get(&base, path);
        assert!(!p.body.contains(">ongoing<"), "{path}: still ongoing");
    }
}

#[test]
fn the_parts_of_the_final_freeze_are_stored() {
    let mut db = db::open_memory();
    put(&mut db, vec![progress(3, 1000, 2)]);
    let mut f = final_freeze(4, 7448);
    f["parts"] = json!(7);
    put(&mut db, vec![f]);
    assert_eq!(one(&db, "SELECT parts FROM events"), 7);

    // A freeze with no progress row: the plain insert stores its parts too.
    let mut db = db::open_memory();
    let mut f = final_freeze(4, 300);
    f["parts"] = json!(1);
    put(&mut db, vec![f]);
    assert_eq!(
        one(&db, "SELECT parts FROM events WHERE type = 'freeze'"),
        1
    );
}
