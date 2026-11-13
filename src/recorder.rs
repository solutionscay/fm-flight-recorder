//! The recorder: connects heartbeats, the watchdog, the round-trip sampler
//! and the log writer.
//!
//! Threads:
//! * main thread (FileMaker's): calls [`Recorder::beat`], [`Recorder::set_waiting`],
//!   [`Recorder::offer_context`], [`Recorder::mark`]. These only touch
//!   atomics or a mutex that nothing holds for long; no I/O.
//! * watchdog: polls the heartbeat every `poll_ms`, runs the [`StallDetector`].
//! * net sampler: reads TCP counters every `net_sample_ms`.
//! * writer: gives each event its `seq`, appends JSONL lines. It also reads
//!   the client identity (install id on disk, computer name) at start. With
//!   an endpoint, it then hands each line to the sender queue.
//! * sender (only with an endpoint): posts batches, see [`crate::sender`].
//!
//! The channel to the writer is bounded ([`EVENT_QUEUE_MAX`]). The main
//! thread uses `try_send`: if the writer cannot keep up, the event is lost
//! and counted, and FileMaker never waits.

use crate::context::{CaptureGate, Context};
use crate::detector::{BusySpan, JoinOut, Joiner, Sample, SleepCheck, StallDetector, Transition};
use crate::eventlog::{iso_from_unix_ms, Event, LogWriter, SCHEMA_VERSION};
use crate::net::{NetHistory, NetProbe};
use crate::wire::{ClientInfo, EventLine};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering::*};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// FileMaker idle levels (FMXExtern.h, FMX_IdleLevel).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Level {
    UserIdle = 0,
    UserNotIdle = 1,
    ScriptPaused = 2,
    ScriptRunning = 3,
    Unsafe = 4,
    /// Heartbeat from the run-loop observer, not from FileMaker.
    RunLoop = 9,
}

impl Level {
    pub fn from_u8(v: u8) -> Level {
        match v {
            0 => Level::UserIdle,
            1 => Level::UserNotIdle,
            2 => Level::ScriptPaused,
            3 => Level::ScriptRunning,
            4 => Level::Unsafe,
            _ => Level::RunLoop,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Level::UserIdle => "user_idle",
            Level::UserNotIdle => "user_not_idle",
            Level::ScriptPaused => "script_paused",
            Level::ScriptRunning => "script_running",
            Level::Unsafe => "unsafe",
            Level::RunLoop => "run_loop",
        }
    }
    /// Levels at which the plug-in may call the calc engine during idle.
    pub fn safe_for_calls(self, allow_script_running: bool) -> bool {
        match self {
            Level::UserIdle | Level::UserNotIdle | Level::ScriptPaused => true,
            Level::ScriptRunning => allow_script_running,
            Level::Unsafe | Level::RunLoop => false,
        }
    }
}

/// Monotonic ns since the first call in this process.
pub fn mono_ns() -> u64 {
    static T0: OnceLock<Instant> = OnceLock::new();
    // +1 so that 0 can mean "never".
    T0.get_or_init(Instant::now).elapsed().as_nanos() as u64 + 1
}

#[derive(Debug, Clone)]
pub struct Config {
    pub threshold_ms: u64,
    pub poll_ms: u64,
    /// TCP peer port to watch. 5003 = FileMaker Server.
    pub net_port: Option<u16>,
    pub net_sample_ms: u64,
    pub context_interval_ms: u64,
    /// Also read Get() values while a script runs (level 3). Off by default.
    pub context_during_scripts: bool,
    pub log_dir: PathBuf,
    /// Free text that goes in session_start, e.g. "idle+runloop".
    pub heartbeat: String,
    pub app: String,
    /// How long a freeze waits for its "after" context before it is written.
    pub after_wait_ms: u64,
    /// Join two parts of a freeze when the second starts at most this long
    /// after the first ends, with no real sleep between (`join_ms=`).
    pub join_ms: u64,
    /// Send a `freeze_progress` event this often while a freeze lasts
    /// (`progress_ms=`). 0 = never; otherwise 250 to [`PROGRESS_MS_MAX`].
    pub progress_ms: u64,
    /// Companion server (`endpoint=http://host:8787`). None = do not send.
    pub endpoint: Option<Endpoint>,
    /// Event token (`token=`). Never shown in Status.
    pub token: String,
    /// Client name (`client=`). Empty = the macOS computer name.
    pub client: String,
    /// Folder for the install id.
    pub state_dir: PathBuf,
    /// Sending (with `endpoint`): a batch goes out at most this long after
    /// its first event, or at `send_batch_max` events.
    pub send_interval_ms: u64,
    pub send_batch_max: usize,
    /// Events kept in memory for sending; the oldest go first when full.
    pub send_queue_max: usize,
    pub send_connect_ms: u64,
    /// Read/write timeout of one post.
    pub send_io_ms: u64,
    /// Wait after the first failed post; it doubles up to `send_retry_max_ms`.
    pub send_retry_ms: u64,
    pub send_retry_max_ms: u64,
    /// Size cap of the offline spool (`<log_dir>/spool/`); 0 = no spool.
    pub send_spool_max_bytes: u64,
    /// Ping the server this often while an endpoint is set.
    pub send_ping_ms: u64,
}

/// Events waiting for the writer thread. Full = lost and counted.
pub const EVENT_QUEUE_MAX: usize = 4096;
/// Range of `send_ms=` (FlightRecorder_Start). Below 250 ms a burst of
/// freezes would make one post per freeze with no gain; the threshold is
/// 250 ms by default, so freezes cannot come faster than that anyway.
pub const SEND_MS_MIN: u64 = 250;
pub const SEND_MS_MAX: u64 = 60_000;
/// The longest `progress_ms`. The server closes an open freeze that had no
/// update for 60 s (`OPEN_STALE_MS`, by its own receive time), so updates
/// must come well inside that, or a row could close between two updates
/// and stop growing.
pub const PROGRESS_MS_MAX: u64 = 30_000;
/// Range of `ping_ms=`.
pub const PING_MS_MIN: u64 = 5_000;
pub const PING_MS_MAX: u64 = 600_000;
/// The longest that stop waits for the sender thread before it detaches it.
pub const SENDER_STOP_WAIT: Duration = Duration::from_millis(20);

/// A parsed `endpoint=` value. Plain HTTP only (#7); HTTPS is #55.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    /// Path prefix without a trailing '/', usually empty.
    pub base: String,
}

impl Endpoint {
    pub fn parse(text: &str) -> Result<Endpoint, String> {
        let t = text.trim();
        let lower = t.to_ascii_lowercase();
        if lower.starts_with("https://") {
            return Err(format!(
                "endpoint {t}: https is not supported yet, use http://"
            ));
        }
        if !lower.starts_with("http://") {
            return Err(format!("endpoint {t}: must start with http://"));
        }
        let rest = &t["http://".len()..];
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        let (host, port) = if let Some(h) = authority.strip_prefix('[') {
            // [IPv6]:port
            let (h, after) = h
                .split_once(']')
                .ok_or_else(|| format!("endpoint {t}: bad host"))?;
            (h, after.strip_prefix(':'))
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h, Some(p)),
                None => (authority, None),
            }
        };
        if host.is_empty() || host.contains(['@', ' ']) {
            return Err(format!("endpoint {t}: bad host"));
        }
        let port = match port {
            None => 80,
            Some(p) => p
                .parse::<u16>()
                .ok()
                .filter(|&p| p != 0)
                .ok_or_else(|| format!("endpoint {t}: bad port"))?,
        };
        if path.contains(['?', '#']) {
            return Err(format!("endpoint {t}: no query or fragment allowed"));
        }
        Ok(Endpoint {
            host: host.to_string(),
            port,
            base: path.trim_end_matches('/').to_string(),
        })
    }

    /// "host:port" for TcpStream and the Host header.
    pub fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// The endpoint as text, for Status.
    pub fn url(&self) -> String {
        format!("http://{}{}", self.authority(), self.base)
    }

    /// 16 hex characters that name this endpoint's spool folder
    /// (FNV-1a 64 of the URL with the host in lower case).
    pub fn spool_key(&self) -> String {
        let text = format!(
            "http://{}:{}{}",
            self.host.to_ascii_lowercase(),
            self.port,
            self.base
        );
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in text.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        format!("{h:016x}")
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            threshold_ms: 250,
            poll_ms: 10,
            net_port: Some(5003),
            net_sample_ms: 2,
            context_interval_ms: 250,
            context_during_scripts: false,
            log_dir: crate::eventlog::default_dir(),
            heartbeat: "idle".into(),
            app: String::new(),
            after_wait_ms: 1500,
            join_ms: crate::detector::JOIN_MS,
            progress_ms: 1000,
            endpoint: None,
            token: String::new(),
            client: String::new(),
            state_dir: crate::identity::default_state_dir(),
            send_interval_ms: 5000,
            send_batch_max: 200,
            send_queue_max: 5000,
            send_connect_ms: 1000,
            send_io_ms: 3000,
            send_retry_ms: 1000,
            send_retry_max_ms: 300_000,
            send_spool_max_bytes: 20 << 20,
            send_ping_ms: 60_000,
        }
    }
}

/// What [`Config::apply_options`] could not use.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OptionReport {
    /// Unknown keys, and known keys with a value that is not valid
    /// (as "key=value"). The rest of the options still apply.
    pub ignored: Vec<String>,
    /// Problems that must stop FlightRecorder_Start (bad endpoint, no token).
    pub errors: Vec<String>,
}

impl OptionReport {
    /// The text that FlightRecorder_Start returns when the options are good:
    /// the log path, then "ignored: a, b" on a second line if needed.
    pub fn start_result(&self, log_path: &str) -> String {
        if self.ignored.is_empty() {
            log_path.to_string()
        } else {
            format!("{log_path}\nignored: {}", self.ignored.join(", "))
        }
    }

    /// The text that FlightRecorder_Start returns on errors.
    pub fn error_result(&self) -> String {
        format!("error: {}", self.errors.join("; "))
    }
}

impl Config {
    /// Parse "threshold=250;port=5003;scripts=1;dir=/path;endpoint=http://h:8787;token=…;send_ms=250;ping_ms=15000"
    /// (FlightRecorder_Start option text). `port=0` disables the round-trip
    /// counter. Unknown keys and bad values are reported, not applied.
    pub fn apply_options(&mut self, text: &str) -> OptionReport {
        let mut rep = OptionReport::default();
        for part in text.split([';', '\n', '\r', ',']) {
            if part.trim().is_empty() {
                continue;
            }
            let Some((k, v)) = part.split_once('=') else {
                rep.ignored.push(part.trim().to_string());
                continue;
            };
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
            let ok = match k.as_str() {
                "threshold" | "threshold_ms" => v
                    .parse::<u64>()
                    .map(|n| self.threshold_ms = n.clamp(20, 600_000))
                    .is_ok(),
                "join_ms" => v
                    .parse::<u64>()
                    .map(|n| self.join_ms = n.min(1_000))
                    .is_ok(),
                "progress_ms" => v
                    .parse::<u64>()
                    .map(|n| {
                        self.progress_ms = if n == 0 {
                            0
                        } else {
                            n.clamp(250, PROGRESS_MS_MAX)
                        }
                    })
                    .is_ok(),
                "port" => v
                    .parse::<u16>()
                    .map(|n| self.net_port = (n != 0).then_some(n))
                    .is_ok(),
                "sample_ms" => v
                    .parse::<u64>()
                    .map(|n| self.net_sample_ms = n.clamp(1, 1000))
                    .is_ok(),
                "context_ms" => v
                    .parse::<u64>()
                    .map(|n| self.context_interval_ms = n.clamp(0, 60_000))
                    .is_ok(),
                "send_ms" => v
                    .parse::<u64>()
                    .map(|n| self.send_interval_ms = n.clamp(SEND_MS_MIN, SEND_MS_MAX))
                    .is_ok(),
                "ping_ms" => v
                    .parse::<u64>()
                    .map(|n| self.send_ping_ms = n.clamp(PING_MS_MIN, PING_MS_MAX))
                    .is_ok(),
                "scripts" => {
                    self.context_during_scripts = matches!(v, "1" | "true" | "yes");
                    true
                }
                "dir" => {
                    if !v.is_empty() {
                        self.log_dir = PathBuf::from(v);
                    }
                    true
                }
                "endpoint" => {
                    if v.is_empty() {
                        self.endpoint = None;
                    } else {
                        match Endpoint::parse(v) {
                            Ok(e) => self.endpoint = Some(e),
                            Err(e) => rep.errors.push(e),
                        }
                    }
                    true
                }
                "token" => {
                    self.token = v.to_string();
                    true
                }
                "client" => {
                    self.client = v.chars().take(100).collect();
                    true
                }
                _ => {
                    rep.ignored.push(k);
                    continue;
                }
            };
            if !ok {
                rep.ignored.push(format!("{k}={v}"));
            }
        }
        if self.endpoint.is_some() && self.token.is_empty() {
            rep.errors.push("token is required with endpoint".into());
        }
        rep
    }
}

struct Shared {
    last_beat_ns: AtomicU64,
    waiting: AtomicBool,
    /// Mach port of the main thread, for the watchdog's sleep check
    /// ([`SleepCheck`]). 0 = no check (only the run loop's word).
    main_thread: AtomicU32,
    level: AtomicU8,
    beats: AtomicU64,
    /// Heartbeats that came from kFMXT_Idle (not from the run loop).
    idle_msgs: AtomicU64,
    freezes: AtomicU64,
    running: AtomicBool,
    /// Set by the watchdog after a freeze: capture context at the next safe idle.
    want_context: AtomicBool,
    /// ns of the start of the stall in progress, 0 if none.
    stall_since_ns: AtomicU64,
    /// Events lost because the writer channel was full.
    events_lost: AtomicU64,
    /// Failed disk writes of the writer thread (log lines, install id).
    write_errors: AtomicU64,
    last_write_error: Mutex<String>,
}

struct Pending {
    event: Event,
    deadline_ns: u64,
}

struct Inner {
    last_context: Context,
    /// Monotonic ns when `last_context` came (0 = never).
    last_context_ns: u64,
    gate: CaptureGate,
    marks: Vec<(u64, String)>,
    pending: Vec<Pending>,
}

/// Mono ns -> wall clock. It reads the wall clock at each call and goes back
/// by the monotonic time since `mono`. A wall time fixed at start is wrong
/// after the Mac sleeps (Instant does not count sleep on macOS) or after a
/// clock change.
#[derive(Clone, Copy)]
struct Clock;
impl Clock {
    fn now() -> Self {
        Self
    }
    fn iso(&self, mono: u64) -> String {
        iso_from_unix_ms(wall_ms_at(mono, mono_ns(), crate::eventlog::unix_ms_now()))
    }
}

/// Wall ms of the monotonic instant `mono`, given the current monotonic and
/// wall times.
fn wall_ms_at(mono: u64, now_mono: u64, now_unix_ms: i64) -> i64 {
    let back_ms = (now_mono as i128 - mono as i128) / 1_000_000;
    now_unix_ms - back_ms as i64
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub running: bool,
    pub heartbeats: u64,
    pub idle_messages: u64,
    pub freezes: u64,
    pub threshold_ms: u64,
    pub in_freeze_ms: u64,
    /// `send_ms` and `ping_ms` in use (they matter only with an endpoint).
    pub send_ms: u64,
    pub ping_ms: u64,
    pub log: String,
    /// The companion server, empty if not set. The token is never shown.
    pub endpoint: String,
    /// The client name: `client=`, else the macOS computer name.
    pub client: String,
    pub session_id: String,
    /// `seq` of the last event written.
    pub last_seq: u64,
    /// Events lost because the writer thread could not keep up.
    pub events_lost: u64,
    /// Failed disk writes (log lines, install id), and the last error.
    pub write_errors: u64,
    pub last_write_error: String,
    // Sending (all 0 or empty without an endpoint).
    /// Events in memory waiting to be sent.
    pub queued: u64,
    /// Events the server accepted.
    pub sent: u64,
    /// Events dropped: queue full, spool full, or refused by the server.
    pub dropped: u64,
    /// Events in the offline spool.
    pub spooled: u64,
    /// Events the server already had (resends).
    pub duplicates: u64,
    /// Failed posts (network error, timeout, 401, 5xx).
    pub send_failures: u64,
    /// Time of the last accepted batch, UTC ISO-8601, empty = never.
    pub last_send: String,
    /// The server answered 401: nothing is sent until the next start.
    pub auth_failed: bool,
    pub pings: u64,
    /// Time of the last accepted ping, empty = never.
    pub last_ping: String,
    /// The last send or ping error, empty if none.
    pub last_send_error: String,
    pub last_context: Context,
}

fn iso_or_empty(unix_ms: i64) -> String {
    if unix_ms > 0 {
        iso_from_unix_ms(unix_ms)
    } else {
        String::new()
    }
}

pub struct Recorder {
    cfg: Config,
    shared: Arc<Shared>,
    inner: Arc<Mutex<Inner>>,
    tx: Mutex<Option<SyncSender<Event>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// Joined first at stop, so its last freeze comes before session_stop.
    watchdog: Mutex<Option<JoinHandle<()>>>,
    clock: Clock,
    writer_dir: PathBuf,
    session_id: String,
    /// Set by the writer thread once it has read the identity.
    client_info: Arc<OnceLock<ClientInfo>>,
    last_seq: Arc<AtomicU64>,
    sender: Option<Arc<crate::sender::Sender>>,
}

impl Recorder {
    pub fn start(cfg: Config, probe: Box<dyn NetProbe>) -> Recorder {
        let clock = Clock::now();
        let shared = Arc::new(Shared {
            last_beat_ns: AtomicU64::new(0),
            waiting: AtomicBool::new(false),
            main_thread: AtomicU32::new(0),
            level: AtomicU8::new(Level::UserNotIdle as u8),
            beats: AtomicU64::new(0),
            idle_msgs: AtomicU64::new(0),
            freezes: AtomicU64::new(0),
            running: AtomicBool::new(true),
            want_context: AtomicBool::new(true),
            stall_since_ns: AtomicU64::new(0),
            events_lost: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            last_write_error: Mutex::new(String::new()),
        });
        let inner = Arc::new(Mutex::new(Inner {
            last_context: Context::default(),
            last_context_ns: 0,
            gate: CaptureGate::new(cfg.context_interval_ms),
            marks: Vec::new(),
            pending: Vec::new(),
        }));
        let net = Arc::new(Mutex::new(NetHistory::new(120)));
        let (tx, rx) = sync_channel::<Event>(EVENT_QUEUE_MAX);
        let mut threads = Vec::new();
        let watchdog_thread;

        // Writer.
        let session_id = crate::identity::random_id();
        let client_info = Arc::new(OnceLock::new());
        let last_seq = Arc::new(AtomicU64::new(0));
        let sender = cfg.endpoint.clone().map(|endpoint| {
            let spool_key = endpoint.spool_key();
            Arc::new(crate::sender::Sender::start(
                crate::sender::SendSettings {
                    endpoint,
                    token: cfg.token.clone(),
                    interval: Duration::from_millis(cfg.send_interval_ms),
                    batch_max: cfg.send_batch_max.clamp(1, crate::wire::MAX_BATCH_EVENTS),
                    batch_bytes: crate::wire::MAX_BATCH_BYTES / 2,
                    queue_max: cfg.send_queue_max,
                    timeouts: crate::http::Timeouts {
                        connect: Duration::from_millis(cfg.send_connect_ms),
                        io: Duration::from_millis(cfg.send_io_ms),
                    },
                    retry: Duration::from_millis(cfg.send_retry_ms),
                    retry_max: Duration::from_millis(cfg.send_retry_max_ms),
                    // One folder for each endpoint: a batch is only ever sent
                    // to the server it was made for.
                    spool_dir: Some(cfg.log_dir.join("spool").join(spool_key)),
                    spool_max_bytes: cfg.send_spool_max_bytes,
                    ping_interval: Duration::from_millis(cfg.send_ping_ms.max(100)),
                },
                session_id.clone(),
                client_info.clone(),
                {
                    let (sh, ls) = (shared.clone(), last_seq.clone());
                    Arc::new(move || crate::sender::PingCounters {
                        running: sh.running.load(Relaxed),
                        heartbeats: sh.beats.load(Relaxed),
                        idle_messages: sh.idle_msgs.load(Relaxed),
                        freezes: sh.freezes.load(Relaxed),
                        last_seq: ls.load(Relaxed),
                    })
                },
            ))
        });
        {
            let w = WriterThread {
                writer: LogWriter::new(cfg.log_dir.clone()),
                session_id: session_id.clone(),
                state_dir: cfg.state_dir.clone(),
                client: cfg.client.clone(),
                app: cfg.app.clone(),
                client_info: client_info.clone(),
                last_seq: last_seq.clone(),
                sender: sender.clone(),
                shared: shared.clone(),
            };
            threads.push(
                thread::Builder::new()
                    .name("fr-writer".into())
                    .spawn(move || w.run(rx))
                    .expect("spawn writer"),
            );
        }

        // Net sampler.
        if cfg.net_port.is_some() {
            let (s, net, every) = (
                shared.clone(),
                net.clone(),
                Duration::from_millis(cfg.net_sample_ms),
            );
            let mut probe = probe;
            threads.push(
                thread::Builder::new()
                    .name("fr-net".into())
                    .spawn(move || {
                        while s.running.load(Relaxed) {
                            if let Some(c) = probe.sample() {
                                net.lock().unwrap().push(mono_ns(), c);
                            }
                            thread::sleep(every);
                        }
                    })
                    .expect("spawn net"),
            );
        }

        // Watchdog.
        {
            let (s, inner, net, tx, cfg2) = (
                shared.clone(),
                inner.clone(),
                net.clone(),
                tx.clone(),
                cfg.clone(),
            );
            watchdog_thread = Some(
                thread::Builder::new()
                    .name("fr-watchdog".into())
                    .spawn(move || watchdog(cfg2, s, inner, net, tx, clock))
                    .expect("spawn watchdog"),
            );
        }

        let _ = tx.try_send(Event::SessionStart {
            ts: clock.iso(mono_ns()),
            pid: std::process::id(),
            app: cfg.app.clone(),
            plugin: crate::VERSION.into(),
            threshold_ms: cfg.threshold_ms,
            heartbeat: cfg.heartbeat.clone(),
            port: cfg.net_port.unwrap_or(0),
            // Filled in by the writer thread (it reads the disk).
            client: String::new(),
            install_id: String::new(),
            machine: String::new(),
            os: String::new(),
        });

        let writer_dir = cfg.log_dir.clone();
        Recorder {
            cfg,
            shared,
            inner,
            tx: Mutex::new(Some(tx)),
            threads: Mutex::new(threads),
            watchdog: Mutex::new(watchdog_thread),
            clock,
            writer_dir,
            session_id,
            client_info,
            last_seq,
            sender,
        }
    }

    /// The sender, when an endpoint is set.
    pub fn sender(&self) -> Option<&crate::sender::Sender> {
        self.sender.as_deref()
    }

    /// Events lost because the writer could not keep up.
    pub fn events_lost(&self) -> u64 {
        self.shared.events_lost.load(Relaxed)
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The client identity, once the writer thread has read it.
    pub fn client_info(&self) -> Option<&ClientInfo> {
        self.client_info.get()
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn log_path(&self) -> PathBuf {
        LogWriter::new(self.writer_dir.clone()).path_for_now()
    }

    // ---- main-thread API (cheap) ----

    /// A heartbeat: FileMaker idle message or run-loop activity.
    #[inline]
    pub fn beat(&self, level: Level) {
        self.shared.last_beat_ns.store(mono_ns(), Release);
        if level != Level::RunLoop {
            self.shared.level.store(level as u8, Relaxed);
            self.shared.idle_msgs.fetch_add(1, Relaxed);
        }
        self.shared.beats.fetch_add(1, Relaxed);
    }

    /// The main run loop is about to sleep (true) or has woken up (false).
    #[inline]
    pub fn set_waiting(&self, waiting: bool) {
        if !waiting {
            self.shared.last_beat_ns.store(mono_ns(), Release);
        }
        self.shared.waiting.store(waiting, Release);
    }

    /// Watch the calling thread (call it on the main thread, after start).
    /// Then the watchdog does not trust "waiting" for longer than the
    /// threshold if the thread still runs: see [`SleepCheck`].
    #[cfg(target_os = "macos")]
    pub fn watch_current_thread(&self) {
        let port = crate::machthread::ThreadWatch::current().port();
        self.shared.main_thread.store(port, Release);
    }

    /// Should the glue read Get() values now? Call at a safe idle.
    pub fn wants_context(&self, level: Level) -> bool {
        if !level.safe_for_calls(self.cfg.context_during_scripts) {
            return false;
        }
        if self.shared.want_context.swap(false, AcqRel) {
            return true;
        }
        let now_ms = mono_ns() / 1_000_000;
        self.inner
            .lock()
            .map(|mut i| i.gate.should_capture(now_ms))
            .unwrap_or(false)
    }

    /// Hand over a freshly captured context (main thread).
    pub fn offer_context(&self, ctx: Context) {
        let mut ready = Vec::new();
        if let Ok(mut i) = self.inner.lock() {
            for p in i.pending.drain(..) {
                ready.push(p.event);
            }
            i.last_context = ctx.clone();
            i.last_context_ns = mono_ns();
        }
        for mut e in ready {
            if let Event::Freeze { after, .. } = &mut e {
                *after = Some(ctx.clone());
            }
            self.send(e);
        }
    }

    /// FlightRecorder_Mark(label). Does NOT count as a heartbeat: a script
    /// that calls Mark may be the very thing that keeps the UI frozen.
    pub fn mark(&self, label: &str, ctx: Context) {
        let now = mono_ns();
        if let Ok(mut i) = self.inner.lock() {
            i.marks.push((now, label.to_string()));
            let excess = i.marks.len().saturating_sub(64);
            i.marks.drain(..excess);
            if !ctx.is_empty() {
                i.last_context = ctx.clone();
                i.last_context_ns = mono_ns();
            }
        }
        self.send(Event::Mark {
            ts: self.clock.iso(now),
            label: label.to_string(),
            context: ctx,
        });
    }

    /// Atomics and short locks only (never a lock held during I/O), so
    /// FlightRecorder_Status is safe on the main thread.
    pub fn status(&self) -> Status {
        let since = self.shared.stall_since_ns.load(Acquire);
        let send = self.sender().map(|s| &*s.stats);
        Status {
            running: self.shared.running.load(Relaxed),
            heartbeats: self.shared.beats.load(Relaxed),
            idle_messages: self.shared.idle_msgs.load(Relaxed),
            freezes: self.shared.freezes.load(Relaxed),
            threshold_ms: self.cfg.threshold_ms,
            in_freeze_ms: if since == 0 {
                0
            } else {
                (mono_ns() - since) / 1_000_000
            },
            send_ms: self.cfg.send_interval_ms,
            ping_ms: self.cfg.send_ping_ms,
            log: self.log_path().display().to_string(),
            endpoint: self
                .cfg
                .endpoint
                .as_ref()
                .map(Endpoint::url)
                .unwrap_or_default(),
            client: self
                .client_info
                .get()
                .map(|c| c.name.clone())
                .unwrap_or_else(|| self.cfg.client.clone()),
            session_id: self.session_id.clone(),
            last_seq: self.last_seq.load(Relaxed),
            events_lost: self.shared.events_lost.load(Relaxed),
            write_errors: self.shared.write_errors.load(Relaxed),
            last_write_error: self.shared.last_write_error.lock().unwrap().clone(),
            queued: self.sender().map_or(0, |s| s.queue.len() as u64),
            sent: send.map_or(0, |s| s.sent.load(Relaxed)),
            dropped: send.map_or(0, |s| s.dropped.load(Relaxed)),
            spooled: send.map_or(0, |s| s.spooled.load(Relaxed)),
            duplicates: send.map_or(0, |s| s.duplicates.load(Relaxed)),
            send_failures: send.map_or(0, |s| s.failures.load(Relaxed)),
            last_send: iso_or_empty(send.map_or(0, |s| s.last_send_unix_ms.load(Relaxed))),
            auth_failed: send.is_some_and(|s| s.auth_failed.load(Relaxed)),
            pings: send.map_or(0, |s| s.pings.load(Relaxed)),
            last_ping: iso_or_empty(send.map_or(0, |s| s.last_ping_unix_ms.load(Relaxed))),
            last_send_error: send
                .map(|s| s.last_error.lock().unwrap().clone())
                .unwrap_or_default(),
            last_context: self
                .inner
                .lock()
                .map(|i| i.last_context.clone())
                .unwrap_or_default(),
        }
    }

    fn send(&self, e: Event) {
        if let Some(tx) = self.tx.lock().unwrap().as_ref() {
            send_or_count(tx, e, &self.shared);
        }
    }

    /// Stop threads, flush pending freezes, write session_stop.
    pub fn stop(&self) -> Status {
        self.shutdown();
        self.status()
    }

    pub fn is_running(&self) -> bool {
        self.shared.running.load(Acquire)
    }

    fn shutdown(&self) {
        if !self.shared.running.swap(false, AcqRel) {
            return;
        }
        // No network work at stop: from here the sender does not post; the
        // writer puts the unsent events in the spool (local disk, like the
        // log).
        let t0 = Instant::now();
        if let Some(s) = &self.sender {
            s.signal_stop();
        }
        // The watchdog closes its last freeze (into `pending`) as it exits,
        // so that freeze comes before session_stop.
        if let Some(t) = self.watchdog.lock().unwrap().take() {
            let _ = t.join();
        }
        let pending: Vec<Event> = self
            .inner
            .lock()
            .map(|mut i| i.pending.drain(..).map(|p| p.event).collect())
            .unwrap_or_default();
        for e in pending {
            self.send(e);
        }
        self.send(Event::SessionStop {
            ts: self.clock.iso(mono_ns()),
            freezes: self.shared.freezes.load(Relaxed),
            heartbeats: self.shared.beats.load(Relaxed),
            idle_messages: self.shared.idle_msgs.load(Relaxed),
        });
        // The watchdog holds a Sender clone; it exits on `running == false`.
        *self.tx.lock().unwrap() = None;
        let threads: Vec<_> = self.threads.lock().unwrap().drain(..).collect();
        for t in threads {
            let _ = t.join();
        }
        if let Some(s) = &self.sender {
            s.finish(SENDER_STOP_WAIT.saturating_sub(t0.elapsed()));
        }
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The log writer thread: numbers the events and writes them.
struct WriterThread {
    writer: LogWriter,
    session_id: String,
    state_dir: PathBuf,
    client: String,
    app: String,
    client_info: Arc<OnceLock<ClientInfo>>,
    last_seq: Arc<AtomicU64>,
    sender: Option<Arc<crate::sender::Sender>>,
    shared: Arc<Shared>,
}

impl WriterThread {
    fn write_failed(&self, e: String) {
        self.shared.write_errors.fetch_add(1, Relaxed);
        *self.shared.last_write_error.lock().unwrap() = e;
    }

    fn run(self, rx: Receiver<Event>) {
        let (info, id_err) = crate::identity::resolve(&self.state_dir, &self.client, &self.app);
        if let Some(e) = id_err {
            self.write_failed(e);
        }
        let info = self.client_info.get_or_init(|| info).clone();
        let mut seq = 0u64;
        for mut event in rx {
            if let Event::SessionStart {
                client,
                install_id,
                machine,
                os,
                ..
            } = &mut event
            {
                client.clone_from(&info.name);
                install_id.clone_from(&info.install_id);
                machine.clone_from(&info.machine);
                os.clone_from(&info.os);
            }
            seq += 1;
            let line = EventLine {
                v: SCHEMA_VERSION,
                session_id: self.session_id.clone(),
                seq,
                event,
            };
            let text = serde_json::to_string(&line).expect("event serializes");
            if let Err(e) = self.writer.write_str(&text) {
                self.write_failed(format!("log {}: {e}", self.writer.dir().display()));
            }
            self.last_seq.store(seq, Relaxed);
            if let Some(s) = &self.sender {
                let dropped = s.queue.push(seq, text);
                if dropped > 0 {
                    s.stats.dropped.fetch_add(dropped, Relaxed);
                }
            }
        }
        // Stop: the events that were not sent go to the spool, and are sent
        // first at the next start.
        if let Some(s) = &self.sender {
            s.spool_rest(s.queue.close());
        }
    }
}

fn send_or_count(tx: &SyncSender<Event>, e: Event, s: &Shared) {
    if let Err(TrySendError::Full(_)) = tx.try_send(e) {
        s.events_lost.fetch_add(1, Relaxed);
    }
}

/// Is the watched main thread on a CPU (not asleep)? Unknown = no.
fn main_thread_busy(port: u32) -> bool {
    #[cfg(target_os = "macos")]
    {
        crate::machthread::ThreadWatch::from_port(port).is_busy() == Some(true)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = port;
        false
    }
}

fn watchdog(
    cfg: Config,
    s: Arc<Shared>,
    inner: Arc<Mutex<Inner>>,
    net: Arc<Mutex<NetHistory>>,
    tx: SyncSender<Event>,
    clock: Clock,
) {
    let mut det = StallDetector::new(cfg.threshold_ms);
    let mut sleep_check = SleepCheck::default();
    let mut busy = BusySpan::default();
    let mut busy_ctx = Context::default();
    let poll = Duration::from_millis(cfg.poll_ms.max(1));
    let threshold_ns = cfg.threshold_ms * 1_000_000;
    let mut level_before_stall = Level::UserNotIdle;
    // Context at the moment the stall was found. No context can be offered
    // between the last beat and this point (offers happen on beats), so it
    // is the "before" context even if the main thread offers a new one
    // before we see the stall end.
    let mut ctx_before_stall = Context::default();
    let mut start_ts = String::new();
    let mut joiner = Joiner::new(cfg.join_ms, cfg.threshold_ms, cfg.poll_ms, cfg.progress_ms);

    while s.running.load(Acquire) {
        thread::sleep(poll);
        let now = mono_ns();
        let level = Level::from_u8(s.level.load(Relaxed));
        let mut sample = Sample {
            now_ns: now,
            last_beat_ns: s.last_beat_ns.load(Acquire),
            waiting: s.waiting.load(Acquire),
        };
        let port = s.main_thread.load(Acquire);
        if port != 0 {
            sample.waiting = sleep_check.waiting(sample, threshold_ns, || main_thread_busy(port));
        }

        let slept = sample.waiting;
        let transition = det.observe(sample);
        if matches!(transition, Some(Transition::Ended { .. })) {
            // A part ended: ask for the context now. If no new part joins,
            // it is the "after" context of the freeze.
            if let Ok(mut i) = inner.lock() {
                i.gate.reset();
            }
            s.want_context.store(true, Release);
        }
        for out in joiner.step(now, transition, slept) {
            match out {
                JoinOut::Open { start_ns, .. } => {
                    level_before_stall = level;
                    ctx_before_stall = inner
                        .lock()
                        .map(|i| i.last_context.clone())
                        .unwrap_or_default();
                    // One text for the start: the progress events and the
                    // final freeze must have the same `ts`.
                    start_ts = clock.iso(start_ns);
                    s.stall_since_ns.store(start_ns, Release);
                }
                JoinOut::Progress {
                    id,
                    start_ns,
                    now_ns,
                    parts,
                } => send_or_count(
                    &tx,
                    Event::FreezeProgress {
                        ts: start_ts.clone(),
                        now_ts: clock.iso(now_ns),
                        id,
                        duration_ms: now_ns.saturating_sub(start_ns) / 1_000_000,
                        level: level_before_stall.name().into(),
                        before: ctx_before_stall.clone(),
                        parts,
                    },
                    &s,
                ),
                JoinOut::Close {
                    start_ns,
                    end_ns,
                    parts,
                    ..
                } => {
                    s.stall_since_ns.store(0, Release);
                    let mut ev = final_freeze(
                        &cfg,
                        &inner,
                        &net,
                        std::mem::take(&mut start_ts),
                        &clock,
                        (start_ns, end_ns),
                        level_before_stall,
                        std::mem::take(&mut ctx_before_stall),
                    );
                    set_parts(&mut ev, parts);
                    close_freeze(&cfg, &inner, &s, &tx, ev, end_ns);
                }
            }
        }

        // Pending freezes that never got an "after" context.
        {
            let mut i = inner.lock().unwrap();
            if i.pending.iter().any(|p| p.deadline_ns <= now) {
                let (late, keep): (Vec<_>, Vec<_>) =
                    i.pending.drain(..).partition(|p| p.deadline_ns <= now);
                i.pending = keep;
                for p in late {
                    send_or_count(&tx, p.event, &s);
                }
            }
            if busy_ctx.is_empty() || level != Level::ScriptRunning {
                busy_ctx = i.last_context.clone();
            }
        }

        // Long script runs.
        if let Some((a, b)) = busy.on_level(now, level == Level::ScriptRunning) {
            if b - a >= threshold_ns {
                let netw = net.lock().map(|n| n.window(a, b)).unwrap_or_default();
                send_or_count(
                    &tx,
                    Event::ScriptBusy {
                        ts: clock.iso(a),
                        end_ts: clock.iso(b),
                        duration_ms: (b - a) / 1_000_000,
                        before: busy_ctx.clone(),
                        net: netw,
                    },
                    &s,
                );
            }
        }
    }
    // At stop: a freeze whose last part ended but did not settle yet. The
    // "after" queue is already flushed, so send it now.
    if let Some(JoinOut::Close {
        start_ns,
        end_ns,
        parts,
        ..
    }) = joiner.flush()
    {
        let mut ev = final_freeze(
            &cfg,
            &inner,
            &net,
            start_ts,
            &clock,
            (start_ns, end_ns),
            level_before_stall,
            ctx_before_stall,
        );
        set_parts(&mut ev, parts);
        close_freeze(&cfg, &inner, &s, &tx, ev, end_ns);
    }
}

fn set_parts(ev: &mut Event, n: u32) {
    if let Event::Freeze { parts, .. } = ev {
        *parts = n;
    }
}

/// Send a closed freeze with its "after" context: a context that came
/// after `end_ns` if there is one, else the next one (or none after
/// `after_wait_ms`).
fn close_freeze(
    cfg: &Config,
    inner: &Mutex<Inner>,
    s: &Shared,
    tx: &SyncSender<Event>,
    mut ev: Event,
    end_ns: u64,
) {
    s.freezes.fetch_add(1, Relaxed);
    let mut i = inner.lock().unwrap();
    if i.last_context_ns >= end_ns && !i.last_context.is_empty() {
        if let Event::Freeze { after, .. } = &mut ev {
            *after = Some(i.last_context.clone());
        }
        drop(i);
        send_or_count(tx, ev, s);
        return;
    }
    i.pending.push(Pending {
        event: ev,
        deadline_ns: mono_ns() + cfg.after_wait_ms * 1_000_000,
    });
    i.gate.reset();
    drop(i);
    s.want_context.store(true, Release);
}

/// The final `freeze` event of a (joined) freeze. `ts` is the start text
/// that the progress events used ("" if none: made from `start_ns`).
#[allow(clippy::too_many_arguments)]
fn final_freeze(
    cfg: &Config,
    inner: &Mutex<Inner>,
    net: &Mutex<NetHistory>,
    ts: String,
    clock: &Clock,
    (start_ns, end_ns): (u64, u64),
    level: Level,
    before: Context,
) -> Event {
    // The sampler can lag the heartbeat by one interval: the last reply may
    // land just after `end_ns`. Wait, then count a little past the end.
    let grace_ns = (cfg.net_sample_ms * 3 + 2) * 1_000_000;
    let wait = (end_ns + grace_ns).saturating_sub(mono_ns());
    if wait > 0 {
        thread::sleep(Duration::from_nanos(wait));
    }
    let netw = net
        .lock()
        .map(|n| n.window(start_ns, end_ns + grace_ns))
        .unwrap_or_default();
    let marks = inner
        .lock()
        .map(|i| {
            i.marks
                .iter()
                .filter(|(t, _)| *t + 1_000_000_000 >= start_ns && *t <= end_ns)
                .map(|(_, l)| l.clone())
                .collect()
        })
        .unwrap_or_default();
    Event::Freeze {
        ts: if ts.is_empty() {
            clock.iso(start_ns)
        } else {
            ts
        },
        end_ts: clock.iso(end_ns),
        duration_ms: (end_ns - start_ns) / 1_000_000,
        level: level.name().into(),
        before,
        after: None,
        net: netw,
        marks,
        parts: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wall_time_follows_offset_jump() {
        const MS: u64 = 1_000_000;
        // Session start: mono 1 s, wall 2025-11-08T14:00:00.000Z.
        let start_wall = 1_762_610_400_000;
        assert_eq!(wall_ms_at(1_000 * MS, 1_000 * MS, start_wall), start_wall);
        // 10 s awake, then the Mac sleeps 1 h: mono +10 s, wall +3610 s.
        let now_mono = 11_000 * MS;
        let now_wall = start_wall + 3_610_000;
        // An event now gets the real wall time, not start + 10 s.
        assert_eq!(wall_ms_at(now_mono, now_mono, now_wall), now_wall);
        // A freeze that started 4.2 s (mono) ago.
        assert_eq!(
            iso_from_unix_ms(wall_ms_at(now_mono - 4_200 * MS, now_mono, now_wall)),
            "2025-11-08T15:00:05.800Z"
        );
        // A clock change (NTP) back by 2 s is followed too.
        assert_eq!(
            wall_ms_at(now_mono, now_mono, now_wall - 2_000),
            now_wall - 2_000
        );
    }

    #[test]
    fn options_parse() {
        let mut c = Config::default();
        c.apply_options("threshold=400; port=0; scripts=1; dir=/tmp/x; bogus=1");
        assert_eq!(c.threshold_ms, 400);
        assert_eq!(c.net_port, None);
        assert!(c.context_during_scripts);
        assert_eq!(c.log_dir, PathBuf::from("/tmp/x"));
        c.apply_options("threshold=1");
        assert_eq!(c.threshold_ms, 20);
        c.apply_options("port=15003");
        assert_eq!(c.net_port, Some(15003));
    }

    #[test]
    fn options_progress_ms_stays_inside_the_server_stale_time() {
        let mut c = Config::default();
        assert_eq!(c.progress_ms, 1000);
        let r = c.apply_options("progress_ms=5000");
        assert!(r.ignored.is_empty() && r.errors.is_empty(), "{r:?}");
        assert_eq!(c.progress_ms, 5000);
        // At most 30 s: the server closes a row after 60 s with no update.
        c.apply_options("progress_ms=60000");
        assert_eq!(c.progress_ms, PROGRESS_MS_MAX);
        assert_eq!(PROGRESS_MS_MAX, 30_000);
        c.apply_options("progress_ms=10");
        assert_eq!(c.progress_ms, 250);
        c.apply_options("progress_ms=0");
        assert_eq!(c.progress_ms, 0, "0 turns progress updates off");
    }

    #[test]
    fn options_send_ms_and_ping_ms() {
        let mut c = Config::default();
        assert_eq!((c.send_interval_ms, c.send_ping_ms), (5000, 60_000));
        let r = c.apply_options("send_ms=500; ping_ms=15000");
        assert!(r.ignored.is_empty() && r.errors.is_empty(), "{r:?}");
        assert_eq!((c.send_interval_ms, c.send_ping_ms), (500, 15_000));
        // Clamped to the safe range.
        c.apply_options("send_ms=0; ping_ms=1");
        assert_eq!(
            (c.send_interval_ms, c.send_ping_ms),
            (SEND_MS_MIN, PING_MS_MIN)
        );
        c.apply_options("SEND_MS=999999; ping_ms=99999999");
        assert_eq!(
            (c.send_interval_ms, c.send_ping_ms),
            (SEND_MS_MAX, PING_MS_MAX)
        );
        // A bad value is reported and the old value stays.
        let r = c.apply_options("send_ms=fast");
        assert_eq!(r.ignored, vec!["send_ms=fast".to_string()]);
        assert_eq!(c.send_interval_ms, SEND_MS_MAX);
    }

    #[test]
    fn options_report_unknown_keys_and_bad_values() {
        let mut c = Config::default();
        let r = c.apply_options("threshold=400; bogus=1; tresh=9; port=abc; nokey; ;dir=");
        assert_eq!(r.ignored, vec!["bogus", "tresh", "port=abc", "nokey"]);
        assert!(r.errors.is_empty());
        assert_eq!(c.threshold_ms, 400);
        assert_eq!(c.net_port, Some(5003), "bad value keeps the default");
        assert_eq!(
            r.start_result("/l.jsonl"),
            "/l.jsonl\nignored: bogus, tresh, port=abc, nokey"
        );
        assert_eq!(OptionReport::default().start_result("/l.jsonl"), "/l.jsonl");
    }

    #[test]
    fn options_endpoint_token_client() {
        let mut c = Config::default();
        let r =
            c.apply_options("endpoint=http://192.168.1.10:8787; token=ab=cd; client=Front Desk");
        assert_eq!(r, OptionReport::default());
        let e = c.endpoint.clone().unwrap();
        assert_eq!(
            (e.host.as_str(), e.port, e.base.as_str()),
            ("192.168.1.10", 8787, "")
        );
        assert_eq!(e.url(), "http://192.168.1.10:8787");
        assert_eq!(c.token, "ab=cd", "only the first '=' splits");
        assert_eq!(c.client, "Front Desk");

        // Endpoint without token: error.
        let mut c = Config::default();
        let r = c.apply_options("endpoint=http://fr.local:8787");
        assert_eq!(r.errors, vec!["token is required with endpoint"]);
        assert_eq!(r.error_result(), "error: token is required with endpoint");

        // Not http: error.
        for bad in [
            "https://h:8787",
            "h:8787",
            "http://:8787",
            "http://h:0",
            "http://h:99999",
            "http://h/x?y",
        ] {
            let mut c = Config::default();
            let r = c.apply_options(&format!("endpoint={bad};token=t"));
            assert_eq!(r.errors.len(), 1, "{bad}: {r:?}");
        }

        // Token alone is fine; empty endpoint turns sending off.
        let mut c = Config::default();
        assert!(c.apply_options("token=t; endpoint=").errors.is_empty());
        assert!(c.endpoint.is_none());
    }

    #[test]
    fn spool_key_is_per_endpoint() {
        let a = Endpoint::parse("http://FR.local:8787").unwrap();
        let b = Endpoint::parse("http://fr.local:8787/").unwrap();
        let c = Endpoint::parse("http://fr.local:8788").unwrap();
        assert_eq!(a.spool_key(), b.spool_key());
        assert_ne!(a.spool_key(), c.spool_key());
        assert_eq!(a.spool_key().len(), 16);
    }

    #[test]
    fn endpoint_forms() {
        let e = Endpoint::parse("HTTP://fr.local/base/").unwrap();
        assert_eq!(
            (e.host.as_str(), e.port, e.base.as_str()),
            ("fr.local", 80, "/base")
        );
        assert_eq!(e.url(), "http://fr.local:80/base");
        let e = Endpoint::parse("http://[::1]:8787").unwrap();
        assert_eq!((e.host.as_str(), e.port), ("::1", 8787));
        assert_eq!(e.authority(), "[::1]:8787");
    }

    #[test]
    fn status_shows_endpoint_not_token() {
        let dir = std::env::temp_dir().join(format!("fr-status-{}", std::process::id()));
        let mut cfg = Config {
            log_dir: dir.clone(),
            state_dir: dir.join("state"),
            net_port: None,
            ..Default::default()
        };
        cfg.apply_options("endpoint=http://127.0.0.1:9;token=secret-token;client=Desk 4");
        let rec = Recorder::start(cfg, Box::new(crate::net::NoProbe));
        let st = rec.stop();
        let json = serde_json::to_string(&st).unwrap();
        assert!(
            json.contains("\"endpoint\":\"http://127.0.0.1:9\""),
            "{json}"
        );
        assert!(json.contains("\"client\":\"Desk 4\""), "{json}");
        assert!(!json.contains("secret-token"), "{json}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn status_shows_write_errors() {
        let base = std::env::temp_dir().join(format!("fr-werr-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        // A file where the log folder should be: every log write fails.
        let not_a_dir = base.join("file");
        std::fs::write(&not_a_dir, "x").unwrap();
        let rec = Recorder::start(
            Config {
                log_dir: not_a_dir,
                state_dir: base.join("state"),
                net_port: None,
                ..Default::default()
            },
            Box::new(crate::net::NoProbe),
        );
        rec.mark("a", Context::default());
        let st = rec.stop();
        assert_eq!(st.write_errors, 3, "{st:?}");
        assert!(
            st.last_write_error.starts_with("log "),
            "{}",
            st.last_write_error
        );
        assert_eq!((st.sent, st.queued, st.last_send.as_str()), (0, 0, ""));
        let json = serde_json::to_value(&st).unwrap();
        for k in [
            "write_errors",
            "last_write_error",
            "events_lost",
            "endpoint",
            "queued",
            "sent",
            "dropped",
            "spooled",
            "last_send",
            "last_send_error",
            "send_failures",
            "auth_failed",
            "pings",
            "last_ping",
        ] {
            assert!(json.get(k).is_some(), "no {k} in {json}");
        }
        let _ = std::fs::remove_dir_all(&base);
    }

    fn read_lines(dir: &std::path::Path) -> Vec<EventLine> {
        let mut out = Vec::new();
        for f in std::fs::read_dir(dir).unwrap() {
            let p = f.unwrap().path();
            if p.extension().is_some_and(|e| e == "jsonl") {
                let text = std::fs::read_to_string(p).unwrap();
                out.extend(
                    text.lines()
                        .map(|l| serde_json::from_str::<EventLine>(l).unwrap()),
                );
            }
        }
        out.sort_by_key(|l| l.seq);
        out
    }

    #[test]
    fn events_get_session_id_seq_and_identity() {
        let dir = std::env::temp_dir().join(format!("fr-ident-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = Config {
            log_dir: dir.join("a"),
            state_dir: dir.join("state"),
            net_port: None,
            app: "Pro 26.0.3".into(),
            ..Default::default()
        };
        let rec = Recorder::start(cfg.clone(), Box::new(crate::net::NoProbe));
        rec.mark("one", Context::default());
        rec.mark("two", Context::default());
        let st = rec.stop();
        let sid = rec.session_id().to_string();
        assert_eq!(st.session_id, sid);
        assert_eq!(st.last_seq, 4);
        assert_eq!(st.client, crate::identity::computer_name());

        let lines = read_lines(&dir.join("a"));
        assert_eq!(
            lines.iter().map(|l| l.seq).collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        assert!(lines.iter().all(|l| l.session_id == sid && l.v == 1));
        let Event::SessionStart {
            client,
            install_id,
            machine,
            os,
            app,
            ..
        } = &lines[0].event
        else {
            panic!("{:?}", lines[0]);
        };
        let kept = std::fs::read_to_string(dir.join("state/install-id")).unwrap();
        assert_eq!(install_id, kept.trim());
        assert_eq!(client, &crate::identity::computer_name());
        assert!(!machine.is_empty() && !os.is_empty());
        assert_eq!(app, "Pro 26.0.3");
        assert!(matches!(lines[3].event, Event::SessionStop { .. }));

        // A new start: new session, seq from 1, same install id; client= wins.
        let cfg2 = Config {
            log_dir: dir.join("b"),
            client: "Desk 4".into(),
            ..cfg
        };
        let rec2 = Recorder::start(cfg2, Box::new(crate::net::NoProbe));
        rec2.stop();
        assert_ne!(rec2.session_id(), sid);
        let lines2 = read_lines(&dir.join("b"));
        assert_eq!(lines2[0].seq, 1);
        assert!(matches!(
            &lines2[0].event,
            Event::SessionStart { client, install_id: i2, .. } if client == "Desk 4" && i2 == install_id
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn level_safety() {
        assert!(Level::UserIdle.safe_for_calls(false));
        assert!(!Level::ScriptRunning.safe_for_calls(false));
        assert!(Level::ScriptRunning.safe_for_calls(true));
        assert!(!Level::Unsafe.safe_for_calls(true));
        assert_eq!(Level::from_u8(3), Level::ScriptRunning);
    }
    /// The run loop said "waiting", but the thread works (an observer after
    /// ours in the BeforeWaiting pass). With the thread watch: a freeze.
    /// Without it: none (the old behavior).
    #[cfg(target_os = "macos")]
    fn busy_while_waiting(watch: bool, busy: bool) -> u64 {
        let dir = std::env::temp_dir().join(format!(
            "fr-watch-{watch}-{busy}-{}-{}",
            std::process::id(),
            crate::identity::random_id()
        ));
        let rec = Recorder::start(
            Config {
                log_dir: dir.clone(),
                state_dir: dir.join("state"),
                net_port: None,
                ..Default::default()
            },
            Box::new(crate::net::NoProbe),
        );
        if watch {
            rec.watch_current_thread();
        }
        for _ in 0..10 {
            rec.beat(Level::RunLoop);
            thread::sleep(Duration::from_millis(5));
        }
        rec.beat(Level::RunLoop);
        rec.set_waiting(true);
        let t = Instant::now();
        while t.elapsed() < Duration::from_millis(700) {
            if busy {
                std::hint::spin_loop();
            } else {
                thread::sleep(Duration::from_millis(50));
            }
        }
        rec.set_waiting(false);
        rec.beat(Level::RunLoop);
        thread::sleep(Duration::from_millis(100));
        let n = rec.stop().freezes;
        let _ = std::fs::remove_dir_all(&dir);
        n
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn watchdog_sees_work_after_before_waiting() {
        assert_eq!(busy_while_waiting(true, true), 1, "watched, busy");
        assert_eq!(
            busy_while_waiting(false, true),
            0,
            "not watched: old behavior"
        );
        assert_eq!(busy_while_waiting(true, false), 0, "watched, really asleep");
    }
    /// One long wait in which the main thread runs the run loop once a
    /// second (one beat): one freeze in the log, with progress events before
    /// it that have the same `ts` and id 1.
    #[test]
    fn a_wait_with_short_pumps_is_one_freeze_with_progress() {
        let dir = std::env::temp_dir().join(format!(
            "fr-join-{}-{}",
            std::process::id(),
            crate::identity::random_id()
        ));
        let rec = Recorder::start(
            Config {
                log_dir: dir.clone(),
                state_dir: dir.join("state"),
                net_port: None,
                ..Default::default()
            },
            Box::new(crate::net::NoProbe),
        );
        let beats = |ms: u64| {
            let t = Instant::now();
            while t.elapsed() < Duration::from_millis(ms) {
                rec.beat(Level::RunLoop);
                thread::sleep(Duration::from_millis(5));
            }
        };
        beats(200);
        for _ in 0..3 {
            thread::sleep(Duration::from_millis(1_000));
            rec.beat(Level::RunLoop);
        }
        thread::sleep(Duration::from_millis(1_000));
        beats(800);
        let st = rec.stop();
        assert_eq!(st.freezes, 1, "{st:?}");
        let text = std::fs::read_to_string(&st.log).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let of = |t: &str| -> Vec<&serde_json::Value> {
            lines.iter().filter(|v| v["type"] == t).collect()
        };
        let freezes = of("freeze");
        assert_eq!(freezes.len(), 1, "{text}");
        let d = freezes[0]["duration_ms"].as_u64().unwrap();
        assert!((3_900..4_400).contains(&d), "duration {d}");
        let progress = of("freeze_progress");
        assert!(progress.len() >= 2, "{text}");
        for p in &progress {
            assert_eq!(p["id"], 1);
            assert_eq!(p["ts"], freezes[0]["ts"], "same start text");
        }
        let durs: Vec<u64> = progress
            .iter()
            .map(|p| p["duration_ms"].as_u64().unwrap())
            .collect();
        assert!(durs.windows(2).all(|w| w[1] > w[0]), "{durs:?}");
        let parts = progress.last().unwrap()["parts"].as_u64().unwrap();
        assert!(parts >= 3, "parts {parts}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
