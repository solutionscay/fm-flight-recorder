//! Synthetic data for development and demos (`fr-server fake`): several
//! clients, files and accounts, shared clients, user names that differ from
//! account names, script_busy and marks. All names are made up.
//!
//! A fixed seed and a fixed `--end` time give the same data each time.

use crate::db::Db;
use crate::store::{self, Batch, Meta};
use crate::tz::Zone;
use chrono::{Datelike, Days, NaiveDate, Weekday};
use fm_flight_recorder::context::Context;
use fm_flight_recorder::eventlog::{iso_from_unix_ms, Event};
use fm_flight_recorder::net::NetWindow;
use fm_flight_recorder::wire::{ClientInfo, EventLine, Ping};

#[derive(Debug, Clone)]
pub struct Opts {
    pub clients: usize,
    pub files: usize,
    pub accounts: usize,
    pub days: u32,
    pub seed: u64,
    /// Last moment of the data (Unix ms).
    pub end_ms: i64,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            clients: 6,
            files: 2,
            accounts: 7,
            days: 10,
            seed: 7,
            end_ms: crate::tz::now_ms(),
        }
    }
}

/// One generated session of one client.
#[derive(Debug, Clone)]
pub struct Session {
    pub client: ClientInfo,
    pub session_id: String,
    pub lines: Vec<EventLine>,
    /// The session is still open at the end (no session_stop).
    pub open: bool,
    /// The client still pings at the end (connected).
    pub live: bool,
    pub heartbeats: u64,
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn f(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, a: f64, b: f64) -> f64 {
        a + (b - a) * self.f()
    }
    fn pick<'a, T>(&mut self, v: &'a [T]) -> &'a T {
        &v[(self.next() % v.len() as u64) as usize]
    }
    fn chance(&mut self, p: f64) -> bool {
        self.f() < p
    }
    /// About normal (sum of uniforms).
    fn normal(&mut self) -> f64 {
        (0..6).map(|_| self.f()).sum::<f64>() - 3.0
    }
    fn hex32(&mut self) -> String {
        format!("{:016x}{:016x}", self.next(), self.next())
    }
}

/// (layout, table, mean ms, server share, mean round trips, weight)
type L = (&'static str, &'static str, f64, f64, f64, u32);

const FILES: [(&str, &[L], &[&str]); 4] = [
    (
        "Invoices",
        &[
            ("Invoices | Detail", "Invoices", 2800.0, 0.72, 40.0, 10),
            ("Customers | List", "Customers", 2400.0, 0.80, 50.0, 8),
            ("Dashboard", "Dashboard", 4400.0, 0.24, 20.0, 4),
            ("Orders | Detail", "Orders", 1700.0, 0.63, 10.0, 5),
            ("Invoices | List", "Invoices", 2000.0, 0.47, 12.0, 4),
            ("Reports | Aging", "Reports", 7600.0, 0.88, 90.0, 1),
            ("Products | List", "Products", 1300.0, 0.55, 6.0, 2),
            ("Customers | Detail", "Customers", 900.0, 0.40, 4.0, 2),
        ],
        &[
            "Go To Invoice",
            "OnRecordLoad: Refresh Totals",
            "OnLayoutEnter: Build Charts",
            "Sort By Balance",
            "Commit Order",
            "Print Aging Report",
        ],
    ),
    (
        "Inventory",
        &[
            ("Stock | Counts", "Stock", 2900.0, 0.82, 55.0, 6),
            ("Items | List", "Items", 1300.0, 0.56, 7.0, 4),
            ("Receiving | Detail", "Receiving", 1900.0, 0.42, 18.0, 2),
        ],
        &["Recount Stock", "Post Receipt"],
    ),
    (
        "Orders",
        &[
            ("Order Entry", "Orders", 1500.0, 0.60, 15.0, 5),
            ("Shipments | List", "Shipments", 2200.0, 0.70, 30.0, 3),
        ],
        &["Ship Order", "Price Check"],
    ),
    (
        "Projects",
        &[
            ("Timeline", "Tasks", 3100.0, 0.50, 25.0, 3),
            ("Tasks | List", "Tasks", 1200.0, 0.60, 8.0, 4),
        ],
        &["Recalc Schedule"],
    ),
];

const ACCOUNTS: [(&str, &str); 9] = [
    ("arivera", "A. Rivera"),
    ("dbrooks", "D. Brooks"),
    ("warehouse", "Warehouse"),
    ("kchen", "K. Chen"),
    ("mokafor", "M. Okafor"),
    ("admin", "Admin"),
    ("lmoreau", "L. Moreau"),
    ("tnguyen", "T. Nguyen"),
    ("bsilva", "B. Silva"),
];

const CLIENTS: [&str; 8] = [
    "Accounts MacBook",
    "Front desk iMac",
    "Warehouse Mac mini",
    "Sales MacBook Air",
    "Office iMac 2",
    "Dev MacBook Pro",
    "Reception iMac",
    "Shipping Mac mini",
];

const BUSY_SCRIPT: &str = "Month End: Recalculate Balances";

fn ctx(file: &str, layout: &str, table: &str, script: &str, acct: (&str, &str)) -> Context {
    Context {
        file: file.into(),
        layout: layout.into(),
        layout_table: table.into(),
        script: script.into(),
        account: acct.0.into(),
        user: acct.1.into(),
        window: file.into(),
        host: "fms.example.lan".into(),
        mode: "0".into(),
        ..Default::default()
    }
}

/// Make the sessions.
pub fn generate(o: &Opts, z: Zone) -> Vec<Session> {
    let mut rng = Rng(o.seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);
    let nfiles = o.files.clamp(1, FILES.len());
    let naccts = o.accounts.clamp(1, ACCOUNTS.len());
    let nclients = o.clients.clamp(1, CLIENTS.len());
    let end_day = z.date_of(o.end_ms);
    let first_day = end_day - Days::new(o.days.max(1) as u64 - 1);
    let mut out = Vec::new();
    for ci in 0..nclients {
        let mut info = ClientInfo {
            install_id: rng.hex32(),
            name: CLIENTS[ci].into(),
            machine: format!("{}.local", CLIENTS[ci].replace(' ', "-")),
            os: "macOS 26.0.1".into(),
            app: "Pro 26.0.3".into(),
            plugin: fm_flight_recorder::VERSION.into(),
        };
        if ci == 4 {
            info.plugin = "0.0.9".into();
            info.app = "Pro 26.0.2".into();
        }
        if ci == 5 {
            info.app = "Pro 26.0.1".into();
        }
        // Accounts on this client: one, a second on client 1 (a shared
        // front desk), and account 3 also works on client 5.
        let mut accts = vec![ACCOUNTS[ci % naccts]];
        if ci == 1 && naccts > 6 {
            accts.push(ACCOUNTS[6]);
        }
        if ci == 5 && naccts > 3 {
            accts.push(ACCOUNTS[3]);
        }
        let file_ix: Vec<usize> = if ci == 2 && nfiles > 1 {
            vec![1]
        } else if ci == 5 {
            (0..nfiles).collect()
        } else {
            vec![ci % nfiles.min(2)]
        };
        let mut day = first_day;
        while day <= end_day {
            let weekend = matches!(day.weekday(), Weekday::Sat | Weekday::Sun);
            let today = day == end_day;
            let skip = (weekend && !rng.chance(0.15)) || (today && ci == 5);
            if !skip {
                if let Some(s) = make_day(&mut rng, o, z, day, ci, &info, &accts, &file_ix, today) {
                    out.push(s);
                }
            }
            day = day.succ_opt().unwrap();
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn make_day(
    rng: &mut Rng,
    o: &Opts,
    z: Zone,
    day: NaiveDate,
    ci: usize,
    info: &ClientInfo,
    accts: &[(&'static str, &'static str)],
    file_ix: &[usize],
    today: bool,
) -> Option<Session> {
    let base = z.day_start(day);
    let start = base + ((8.0 + rng.range(0.0, 1.2)) * 3_600_000.0) as i64;
    let mut stop = base + ((17.0 + rng.range(0.0, 1.5)) * 3_600_000.0) as i64;
    if start >= o.end_ms {
        return None;
    }
    let mut open = false;
    let mut live = false;
    if today && stop >= o.end_ms {
        open = true;
        live = ci != 4;
        stop = if ci == 4 {
            o.end_ms - 40 * 60_000
        } else {
            o.end_ms
        };
    }
    let sid = rng.hex32();
    let mut ev: Vec<Event> = Vec::new();
    ev.push(Event::SessionStart {
        ts: iso_from_unix_ms(start),
        pid: 4000 + (rng.next() % 5000) as u32,
        app: info.app.clone(),
        plugin: info.plugin.clone(),
        threshold_ms: 250,
        heartbeat: "idle+runloop".into(),
        port: 5003,
        client: info.name.clone(),
        install_id: info.install_id.clone(),
        machine: info.machine.clone(),
        os: info.os.clone(),
    });
    let hours = ((stop - start) as f64 / 3_600_000.0).max(0.1);
    let n = (hours * rng.range(2.0, 5.5)) as usize;
    let mut times: Vec<i64> = (0..n)
        .map(|_| start + (rng.f() * (stop - start) as f64) as i64)
        .collect();
    times.sort_unstable();
    let mut prev_layout = String::new();
    let mut freezes = 0u64;
    for t in times {
        let fi = *rng.pick(file_ix);
        let (file, layouts, scripts) = FILES[fi];
        let total: u32 = layouts.iter().map(|l| l.5).sum();
        let mut w = (rng.next() % total as u64) as u32;
        let l = layouts
            .iter()
            .find(|l| {
                if w < l.5 {
                    true
                } else {
                    w -= l.5;
                    false
                }
            })
            .unwrap();
        let acct = *rng.pick(accts);
        let script = if rng.chance(0.65) {
            *rng.pick(scripts)
        } else {
            ""
        };
        let dur = (l.2 * (0.55 * rng.normal()).exp()).clamp(260.0, 40_000.0) as u64;
        let mut wait = (dur as f64 * (l.3 * rng.range(0.8, 1.15)).min(1.0)) as u64;
        if rng.chance(0.02) {
            wait = dur + 30; // the net window runs a little past the freeze
        }
        let rt = (l.4 * dur as f64 / l.2 * rng.range(0.5, 1.5)).round() as u64;
        let from = if prev_layout.is_empty() || rng.chance(0.3) {
            layouts[(rng.next() % layouts.len() as u64) as usize]
                .0
                .to_string()
        } else {
            prev_layout.clone()
        };
        let from_table = layouts
            .iter()
            .find(|x| x.0 == from)
            .map(|x| x.1)
            .unwrap_or("");
        let mut marks = vec![];
        if !script.is_empty() && rng.chance(0.25) {
            let label = format!("{script} start");
            ev.push(Event::Mark {
                ts: iso_from_unix_ms(t - 120 - (rng.next() % 300) as i64),
                label: label.clone(),
                context: ctx(file, &from, from_table, script, acct),
            });
            marks.push(label);
        }
        let level = if script.is_empty() {
            "user_not_idle"
        } else {
            "script_running"
        };
        ev.push(Event::Freeze {
            ts: iso_from_unix_ms(t),
            end_ts: iso_from_unix_ms(t + dur as i64),
            duration_ms: dur,
            level: level.into(),
            before: ctx(file, &from, from_table, script, acct),
            after: Some(ctx(file, l.0, l.1, "", acct)),
            net: NetWindow {
                round_trips: rt,
                tx_bytes: rt * 400,
                rx_bytes: rt * (2000 + rng.next() % 9000),
                tx_packets: rt * 2,
                rx_packets: rt * 6,
                srtt_ms: 8 + (rng.next() % 30) as u32,
                wait_ms: wait,
                connected: true,
            },
            marks,
            parts: 1,
        });
        freezes += 1;
        prev_layout = l.0.to_string();
    }
    // A long script run that keeps the UI alive: script_busy.
    if file_ix.contains(&0) && (ci == 0 || ci == 5) && rng.chance(0.5) {
        let t = start + ((stop - start) as f64 * rng.range(0.2, 0.8)) as i64;
        let dur = (rng.range(15.0, 45.0) * 1000.0) as u64;
        ev.push(Event::ScriptBusy {
            ts: iso_from_unix_ms(t),
            end_ts: iso_from_unix_ms(t + dur as i64),
            duration_ms: dur,
            before: ctx(
                "Invoices",
                "Invoices | List",
                "Invoices",
                BUSY_SCRIPT,
                accts[0],
            ),
            net: NetWindow {
                round_trips: dur / 300,
                wait_ms: dur / 3,
                srtt_ms: 15,
                connected: true,
                ..Default::default()
            },
        });
    }
    let heartbeats = ((stop - start) / 1000) as u64 * 48;
    if !open {
        ev.push(Event::SessionStop {
            ts: iso_from_unix_ms(stop),
            freezes,
            heartbeats,
            idle_messages: heartbeats / 6,
        });
    }
    let ts_of = |e: &Event| match e {
        Event::SessionStart { ts, .. }
        | Event::Freeze { ts, .. }
        | Event::FreezeProgress { ts, .. }
        | Event::ScriptBusy { ts, .. }
        | Event::Mark { ts, .. }
        | Event::SessionStop { ts, .. } => ts.clone(),
    };
    // session_start first, then time order (ISO strings sort by time).
    let first = ev.remove(0);
    ev.sort_by_key(&ts_of);
    ev.insert(0, first);
    ev.retain(|e| crate::tz::parse_ts(&ts_of(e)).unwrap_or(0) <= o.end_ms);
    let lines = ev
        .into_iter()
        .enumerate()
        .map(|(i, e)| EventLine {
            v: 1,
            session_id: sid.clone(),
            seq: i as u64 + 1,
            event: e,
        })
        .collect();
    Some(Session {
        client: info.clone(),
        session_id: sid,
        lines,
        open,
        live,
        heartbeats,
    })
}

/// JSONL text (the plug-in log format, one session after the other).
pub fn to_jsonl(sessions: &[Session]) -> String {
    let mut s = String::new();
    for x in sessions {
        for l in &x.lines {
            s.push_str(&serde_json::to_string(l).unwrap());
            s.push('\n');
        }
    }
    s
}

fn ping_of(s: &Session, now: i64) -> Ping {
    let freezes = s
        .lines
        .iter()
        .filter(|l| matches!(l.event, Event::Freeze { .. }))
        .count() as u64;
    Ping {
        v: 1,
        session_id: s.session_id.clone(),
        client: s.client.clone(),
        ts: iso_from_unix_ms(now),
        running: true,
        heartbeats: s.heartbeats,
        idle_messages: s.heartbeats / 6,
        freezes,
        last_seq: s.lines.len() as u64,
    }
}

/// Store the sessions in the database, as if the plug-ins had sent them.
/// Live clients also get a ping at `now`.
pub fn store(
    db: &mut Db,
    z: Zone,
    cutoff: NaiveDate,
    sessions: &[Session],
    now: i64,
) -> Result<u64, String> {
    let mut n = 0;
    for s in sessions {
        for chunk in s.lines.chunks(200) {
            let last = chunk
                .iter()
                .filter_map(|l| serde_json::to_value(l).ok())
                .filter_map(|v| {
                    v.get("ts")
                        .and_then(|t| t.as_str())
                        .and_then(crate::tz::parse_ts)
                })
                .max()
                .unwrap_or(now);
            let b = Batch {
                v: 1,
                session_id: s.session_id.clone(),
                client: s.client.clone(),
                events: chunk
                    .iter()
                    .map(|l| serde_json::to_value(l).unwrap())
                    .collect(),
            };
            let ip = format!(
                "192.168.1.{}",
                20 + (s.client.install_id.as_bytes()[0] % 200)
            );
            let m = Meta {
                ip: &ip,
                seen_ms: last,
                zone: z,
                cutoff_day: cutoff,
            };
            n += store::store_batch(db, &b, m)
                .map_err(|e| format!("{e:?}"))?
                .accepted;
        }
        if s.live {
            let ip = format!(
                "192.168.1.{}",
                20 + (s.client.install_id.as_bytes()[0] % 200)
            );
            store::store_ping(db, &ping_of(s, now), &ip, now).map_err(|e| format!("{e:?}"))?;
        }
    }
    Ok(n)
}

/// Post the sessions to a running server with the plug-in's own HTTP
/// client and the exact batch body that the plug-in builds.
pub fn post(sessions: &[Session], url: &str, token: &str, now: i64) -> Result<u64, String> {
    use fm_flight_recorder::http::{post_json, Abort, Timeouts};
    use fm_flight_recorder::recorder::Endpoint;
    use fm_flight_recorder::sender::batch_body;
    use fm_flight_recorder::wire::{IngestReply, PATH_EVENTS, PATH_PING};
    let ep = Endpoint::parse(url)?;
    let t = Timeouts {
        connect: std::time::Duration::from_secs(2),
        io: std::time::Duration::from_secs(10),
    };
    let abort = Abort::default();
    let mut n = 0;
    for s in sessions {
        for chunk in s.lines.chunks(200) {
            let lines: Vec<(u64, String)> = chunk
                .iter()
                .map(|l| (l.seq, serde_json::to_string(l).unwrap()))
                .collect();
            let body = batch_body(&s.session_id, &s.client, &lines);
            let r = post_json(&ep, PATH_EVENTS, token, body.as_bytes(), t, &abort)
                .map_err(|e| e.to_string())?;
            if r.status != 200 {
                return Err(format!("{}: {}", r.status, r.body));
            }
            let rep: IngestReply = serde_json::from_str(&r.body).map_err(|e| e.to_string())?;
            n += rep.accepted;
        }
        if s.live {
            let body = serde_json::to_string(&ping_of(s, now)).unwrap();
            let r = post_json(&ep, PATH_PING, token, body.as_bytes(), t, &abort)
                .map_err(|e| e.to_string())?;
            if r.status != 200 {
                return Err(format!("ping {}: {}", r.status, r.body));
            }
        }
    }
    Ok(n)
}
