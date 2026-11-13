//! JSONL event log in ~/Library/Logs/FlightRecorder/.
//!
//! One JSON object per line. The schema is versioned (`v`) and documented in
//! the wiki (https://github.com/solutionscay/fm-flight-recorder/wiki/Design#log-format). Writing happens on a
//! background thread, never on FileMaker's main thread.

use crate::context::Context;
use crate::net::NetWindow;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // events are short-lived, one per freeze
pub enum Event {
    SessionStart {
        ts: String,
        pid: u32,
        app: String,
        plugin: String,
        threshold_ms: u64,
        heartbeat: String,
        port: u16,
        /// Client name: `client=` or the macOS computer name.
        #[serde(default)]
        client: String,
        /// Random id kept on disk; the server keys clients on it.
        #[serde(default)]
        install_id: String,
        /// Host name.
        #[serde(default)]
        machine: String,
        /// e.g. "macOS 26.0.1".
        #[serde(default)]
        os: String,
    },
    /// The main thread did not respond for `duration_ms`.
    Freeze {
        ts: String,
        end_ts: String,
        duration_ms: u64,
        /// Idle level reported by the last heartbeat before the freeze.
        level: String,
        /// Context read at the last safe idle before the freeze.
        before: Context,
        /// Context read at the first safe idle after the freeze.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<Context>,
        net: NetWindow,
        /// FlightRecorder_Mark labels set during or just before the freeze.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        marks: Vec<String>,
        /// Parts joined into this freeze (see `freeze_progress`). 0 = not
        /// known (an older plug-in).
        #[serde(default, skip_serializing_if = "is_zero")]
        parts: u32,
    },
    /// A freeze that still lasts. The plug-in sends one about once a second
    /// while a long freeze lasts. `ts` is the start of the freeze. The final
    /// `freeze` event of the same session has the same `ts` (the same text)
    /// and replaces it. A server that does not know this type keeps it as
    /// an unknown event.
    FreezeProgress {
        ts: String,
        /// The time of this update.
        now_ts: String,
        /// Number of the freeze in the session: 1, 2, ...
        id: u64,
        /// From `ts` to `now_ts`.
        duration_ms: u64,
        level: String,
        before: Context,
        /// Parts joined so far. FileMaker ran the run loop for a moment
        /// between them, but did not sleep.
        parts: u32,
    },
    /// FileMaker reported "script running" continuously for `duration_ms`.
    ScriptBusy {
        ts: String,
        end_ts: String,
        duration_ms: u64,
        before: Context,
        net: NetWindow,
    },
    Mark {
        ts: String,
        label: String,
        context: Context,
    },
    SessionStop {
        ts: String,
        freezes: u64,
        heartbeats: u64,
        /// Of which kFMXT_Idle messages.
        #[serde(default)]
        idle_messages: u64,
    },
}

#[derive(Serialize)]
struct Line<'a> {
    v: u32,
    #[serde(flatten)]
    event: &'a Event,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

pub fn to_line(e: &Event) -> String {
    serde_json::to_string(&Line {
        v: SCHEMA_VERSION,
        event: e,
    })
    .expect("event serializes")
}

pub fn parse_line(s: &str) -> Option<Event> {
    serde_json::from_str::<Event>(s).ok()
}

/// Default directory: $FLIGHT_RECORDER_LOG_DIR, else ~/Library/Logs/FlightRecorder.
pub fn default_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("FLIGHT_RECORDER_LOG_DIR") {
        return PathBuf::from(d);
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    home.join("Library/Logs/FlightRecorder")
}

pub struct LogWriter {
    dir: PathBuf,
}

impl LogWriter {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// One file per local day: flight-YYYY-MM-DD.jsonl. The date is the
    /// local date, so a work day stays in one file. The `ts` in each record
    /// stays UTC.
    pub fn path_for_now(&self) -> PathBuf {
        self.path_for(unix_ms_now())
    }

    pub fn path_for(&self, unix_ms: i64) -> PathBuf {
        let date = local_date(unix_ms);
        self.dir.join(format!("flight-{date}.jsonl"))
    }

    pub fn write(&self, e: &Event) -> std::io::Result<PathBuf> {
        self.write_str(&to_line(e))
    }

    /// Append one JSON line (without its newline).
    pub fn write_str(&self, line: &str) -> std::io::Result<PathBuf> {
        fs::create_dir_all(&self.dir)?;
        let path = self.path_for_now();
        let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
        let mut buf = String::with_capacity(line.len() + 1);
        buf.push_str(line);
        buf.push('\n');
        f.write_all(buf.as_bytes())?;
        Ok(path)
    }
}

/// Wall clock as ISO-8601 UTC with ms, e.g. 2026-11-08T14:03:07.215Z.
pub fn iso_now() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    iso_from_unix_ms(d.as_millis() as i64)
}

pub fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn iso_from_unix_ms(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let milli = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{milli:03}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

/// Local calendar date (YYYY-MM-DD) of a Unix time in ms.
pub fn local_date(unix_ms: i64) -> String {
    date_with_offset(unix_ms, local_utc_offset_secs(unix_ms.div_euclid(1000)))
}

/// Calendar date of `unix_ms` in a zone `offset_secs` east of UTC.
fn date_with_offset(unix_ms: i64, offset_secs: i64) -> String {
    iso_from_unix_ms(unix_ms + offset_secs * 1000)[..10].to_string()
}

/// Local UTC offset in seconds (east positive) at `secs`, from localtime_r.
/// Falls back to 0 (UTC) if localtime_r fails.
fn local_utc_offset_secs(secs: i64) -> i64 {
    let t = secs as libc::time_t;
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    // SAFETY: both pointers point to valid, owned values for the call.
    let ok = unsafe { !libc::localtime_r(&t, &mut tm).is_null() };
    if ok {
        // c_long is i32 on 32-bit targets.
        #[allow(clippy::useless_conversion)]
        i64::from(tm.tm_gmtoff)
    } else {
        0
    }
}

/// Howard Hinnant's days -> civil date algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_dates() {
        assert_eq!(iso_from_unix_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            iso_from_unix_ms(1_762_610_587_215),
            "2025-11-08T14:03:07.215Z"
        );
        assert_eq!(
            iso_from_unix_ms(951_782_400_000),
            "2000-02-29T00:00:00.000Z"
        );
    }

    #[test]
    fn file_day_uses_offset() {
        // 2025-11-09T05:30:00Z: still Nov 8 in Chicago (UTC-6), Nov 9 in UTC.
        let ms = 1_762_666_200_000;
        assert_eq!(iso_from_unix_ms(ms), "2025-11-09T05:30:00.000Z");
        assert_eq!(date_with_offset(ms, 0), "2025-11-09");
        assert_eq!(date_with_offset(ms, -6 * 3600), "2025-11-08");
        assert_eq!(date_with_offset(ms, 9 * 3600), "2025-11-09");
        // Chicago 23:59:59.999 -> Nov 8; one ms later -> Nov 9.
        let midnight_chicago = 1_762_668_000_000; // 2025-11-09T06:00:00Z
        assert_eq!(
            date_with_offset(midnight_chicago - 1, -6 * 3600),
            "2025-11-08"
        );
        assert_eq!(date_with_offset(midnight_chicago, -6 * 3600), "2025-11-09");
    }

    #[test]
    fn file_day_rolls_over_at_local_midnight() {
        let dir = std::env::temp_dir().join(format!("fr-roll-{}", std::process::id()));
        let w = LogWriter::new(&dir);
        // Find the next local midnight from now with the real local offset.
        let now = unix_ms_now();
        let off_ms = local_utc_offset_secs(now / 1000) * 1000;
        let local_ms = now + off_ms;
        let local_midnight = (local_ms.div_euclid(86_400_000) + 1) * 86_400_000;
        // Use the offset that applies at midnight (DST may change before it).
        let guess = local_midnight - off_ms;
        let next_local_midnight = local_midnight - local_utc_offset_secs(guess / 1000) * 1000;
        let before = w.path_for(next_local_midnight - 1);
        let after = w.path_for(next_local_midnight);
        assert_ne!(before, after, "file must change at local midnight");
        assert_eq!(
            before.file_name().unwrap().to_string_lossy(),
            format!("flight-{}.jsonl", local_date(now))
        );
        // One hour before local midnight is still the same file.
        assert_eq!(w.path_for(next_local_midnight - 3_600_000), before);
    }

    #[test]
    fn freeze_line_round_trips() {
        let e = Event::Freeze {
            ts: "2026-11-08T14:03:07.215Z".into(),
            end_ts: "2026-11-08T14:03:11.415Z".into(),
            duration_ms: 4200,
            level: "user_not_idle".into(),
            before: Context {
                layout: "Invoices".into(),
                script: "OnRecordLoad".into(),
                ..Default::default()
            },
            after: None,
            net: NetWindow {
                round_trips: 38,
                connected: true,
                ..Default::default()
            },
            marks: vec!["load invoice".into()],
            parts: 3,
        };
        let line = to_line(&e);
        assert!(line.starts_with("{\"v\":1,\"type\":\"freeze\""), "{line}");
        assert!(!line.contains("\"after\""));
        assert_eq!(parse_line(&line), Some(e));
    }

    #[test]
    fn writer_appends_lines() {
        let dir = std::env::temp_dir().join(format!("fr-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let w = LogWriter::new(&dir);
        let e = Event::SessionStop {
            ts: iso_now(),
            freezes: 2,
            heartbeats: 10,
            idle_messages: 4,
        };
        let p = w.write(&e).unwrap();
        w.write(&e).unwrap();
        let text = fs::read_to_string(&p).unwrap();
        assert_eq!(text.lines().count(), 2);
        assert!(p
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("flight-"));
        let _ = fs::remove_dir_all(&dir);
    }
}
