//! fr-fakelog: write a realistic fake flight log, for demos and tests
//! without FileMaker.
//!
//!   cargo run --bin fr-fakelog -- [--seed N] [--hours H] [--out FILE]
//!
//! Without --out it prints JSONL to stdout. It uses the same Event types as
//! the plug-in, so the schema always matches.

use fm_flight_recorder::context::Context;
use fm_flight_recorder::eventlog::{iso_from_unix_ms, to_line, Event};
use fm_flight_recorder::net::NetWindow;
use std::io::Write;

/// xorshift64*: deterministic, no dependency.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn f(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, a: u64, b: u64) -> u64 {
        a + self.next() % (b - a + 1)
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[(self.next() % xs.len() as u64) as usize]
    }
}

/// (layout, table, script, typical freeze ms, round trips per second of freeze, weight)
const HOTSPOTS: &[(&str, &str, &str, u64, f64, u32)] = &[
    (
        "Invoices | Detail",
        "Invoices",
        "OnRecordLoad: Refresh Totals",
        4200,
        9.0,
        6,
    ),
    ("Customers | List", "Customers", "", 2600, 14.0, 5),
    (
        "Dashboard",
        "Dashboard",
        "OnLayoutEnter: Build Charts",
        6800,
        4.5,
        3,
    ),
    ("Orders | Detail", "Orders", "Commit Order", 1900, 2.0, 4),
    ("Products | Portal", "Products", "", 1200, 22.0, 4),
    (
        "Invoices | List",
        "Invoices",
        "Sort By Balance",
        3100,
        1.5,
        2,
    ),
    ("Settings", "Prefs", "Startup: Load Globals", 900, 11.0, 1),
];
const ACCOUNTS: &[&str] = &["jose", "maria", "front.desk"];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let seed: u64 = arg("--seed").and_then(|s| s.parse().ok()).unwrap_or(5003);
    let hours: f64 = arg("--hours").and_then(|s| s.parse().ok()).unwrap_or(3.0);
    let out = arg("--out");

    let mut rng = Rng(seed.max(1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    // Session starts at 09:00 UTC on a fixed day so the demo is stable.
    let t0: i64 = 1_793_955_600_000; // 2026-11-06T09:00:00Z
    let end = t0 + (hours * 3_600_000.0) as i64;
    let account = rng.pick(ACCOUNTS).to_string();
    let mut lines = Vec::new();
    let ctx = |layout: &str, table: &str, script: &str| Context {
        file: "Invoices".into(),
        layout: layout.into(),
        layout_table: table.into(),
        script: script.into(),
        account: account.clone(),
        user: account.clone(),
        window: "Invoices".into(),
        host: "fms.example.lan".into(),
        mode: "0".into(),
        captured_ms: 0,
        capture_us: 0,
    };
    let ctx_with_cost = |c: Context, rng: &mut Rng| Context {
        capture_us: rng.range(18, 90),
        ..c
    };

    lines.push(Event::SessionStart {
        ts: iso_from_unix_ms(t0),
        pid: 4242,
        app: "Pro 26.0.3".into(),
        plugin: fm_flight_recorder::VERSION.into(),
        threshold_ms: 250,
        heartbeat: "idle+runloop".into(),
        port: 5003,
        client: "Front Desk iMac".into(),
        install_id: "3f9c2a7e5b0d4e1f8a6c9b2d7e4f1a03".into(),
        machine: "Front-Desk-iMac.local".into(),
        os: "macOS 26.0.1".into(),
    });

    let total_weight: u32 = HOTSPOTS.iter().map(|h| h.5).sum();
    let mut t = t0 + 20_000;
    let mut freezes = 0;
    let mut prev_layout = "Dashboard".to_string();
    // A slow VPN between 10:30 and 11:00 makes everything worse.
    let vpn = (t0 + 5_400_000, t0 + 7_200_000);
    while t < end {
        t += (rng.f() * 240_000.0) as i64 + 15_000;
        let mut w = rng.range(1, total_weight as u64) as u32;
        let h = HOTSPOTS
            .iter()
            .find(|h| {
                if w <= h.5 {
                    true
                } else {
                    w -= h.5;
                    false
                }
            })
            .unwrap();
        let slow = t > vpn.0 && t < vpn.1;
        let rtt: u32 = if slow {
            rng.range(70, 120) as u32
        } else {
            rng.range(8, 28) as u32
        };
        let jitter = 0.45 + rng.f() * 1.2;
        let mut dur = (h.3 as f64 * jitter * if slow { 1.8 } else { 1.0 }) as u64;
        dur = dur.max(260);
        let rts = ((dur as f64 / 1000.0) * h.4 * (0.7 + rng.f() * 0.6)).round() as u64;
        let (layout, table, script) = (h.0, h.1, h.2);

        if !script.is_empty() && rng.f() < 0.25 {
            lines.push(Event::Mark {
                ts: iso_from_unix_ms(t - 120),
                label: format!("{script} start"),
                context: ctx_with_cost(ctx(&prev_layout, table, script), &mut rng),
            });
        }
        let before = ctx_with_cost(
            ctx(&prev_layout, "", if rng.f() < 0.5 { script } else { "" }),
            &mut rng,
        );
        let after = ctx_with_cost(ctx(layout, table, ""), &mut rng);
        // Request -> reply time: RTT plus a few ms of server work per call,
        // and now and then a commit that waits on a record lock.
        let mut wait = rts * (rtt as u64 + rng.range(2, 12));
        if script == "Commit Order" && rng.f() < 0.4 {
            wait += dur * 3 / 4;
        }
        let wait = wait.min(dur);
        lines.push(Event::Freeze {
            ts: iso_from_unix_ms(t),
            end_ts: iso_from_unix_ms(t + dur as i64),
            duration_ms: dur,
            level: if script.is_empty() {
                "user_not_idle"
            } else {
                "script_running"
            }
            .into(),
            before: Context {
                script: script.into(),
                ..before
            },
            after: Some(after),
            net: NetWindow {
                round_trips: rts,
                tx_bytes: rts * rng.range(180, 600),
                rx_bytes: rts * rng.range(900, 24_000),
                tx_packets: rts * 2,
                rx_packets: rts * rng.range(2, 9),
                srtt_ms: rtt,
                wait_ms: wait,
                connected: true,
            },
            marks: vec![],
            parts: 1,
        });
        freezes += 1;
        prev_layout = layout.to_string();
        t += dur as i64;

        if rng.f() < 0.08 {
            let d = rng.range(5_000, 45_000);
            lines.push(Event::ScriptBusy {
                ts: iso_from_unix_ms(t + 2_000),
                end_ts: iso_from_unix_ms(t + 2_000 + d as i64),
                duration_ms: d,
                before: ctx(
                    "Invoices | List",
                    "Invoices",
                    "Month End: Recalculate Balances",
                ),
                net: NetWindow {
                    round_trips: d / 40,
                    tx_bytes: d * 30,
                    rx_bytes: d * 900,
                    srtt_ms: rtt,
                    wait_ms: d / 3,
                    connected: true,
                    ..Default::default()
                },
            });
            t += d as i64;
        }
    }
    lines.push(Event::SessionStop {
        ts: iso_from_unix_ms(end),
        freezes,
        heartbeats: (hours * 3600.0 * 40.0) as u64,
        idle_messages: (hours * 3600.0 * 10.0) as u64,
    });

    let text: String = lines.iter().map(|e| to_line(e) + "\n").collect();
    match out {
        Some(p) => {
            std::fs::write(&p, &text).expect("write");
            eprintln!("wrote {} events ({} freezes) to {p}", lines.len(), freezes);
        }
        None => {
            std::io::stdout().write_all(text.as_bytes()).unwrap();
        }
    }
}
