//! Stall detector: a pure state machine fed with heartbeat samples.
//!
//! The main thread (FileMaker's UI thread) publishes heartbeats: every
//! `kFMXT_Idle` message and, when the run-loop observer is installed, every
//! run-loop activity. It also says when the run loop goes to sleep
//! ("waiting"). A thread that sleeps in the run loop is not frozen, it has no
//! work to do. A thread that is awake and has not beaten for `threshold` is
//! frozen.
//!
//! The watchdog thread calls [`StallDetector::observe`] every few ms. All
//! times are monotonic nanoseconds, injected, so the logic is testable with
//! no clock and no FileMaker.

/// One reading of the shared heartbeat state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    /// Now, monotonic ns.
    pub now_ns: u64,
    /// Time of the last heartbeat, monotonic ns. 0 = no heartbeat yet.
    pub last_beat_ns: u64,
    /// True when the main run loop is asleep waiting for events.
    pub waiting: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// The main thread has been silent for `threshold`; the stall began at
    /// `start_ns` (the last heartbeat).
    Started { start_ns: u64 },
    /// The main thread is back. `end_ns` is the first heartbeat after the
    /// stall (or the time it went to sleep).
    Ended { start_ns: u64, end_ns: u64 },
}

impl Transition {
    pub fn duration_ns(&self) -> Option<u64> {
        match *self {
            Transition::Ended { start_ns, end_ns } => Some(end_ns.saturating_sub(start_ns)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StallDetector {
    threshold_ns: u64,
    /// Some(start) while a stall is in progress.
    in_stall: Option<u64>,
    /// Last heartbeat value seen, to detect "a new beat arrived".
    seen_beat_ns: u64,
}

impl StallDetector {
    pub fn new(threshold_ms: u64) -> Self {
        Self {
            threshold_ns: threshold_ms.max(1) * 1_000_000,
            in_stall: None,
            seen_beat_ns: 0,
        }
    }

    pub fn threshold_ms(&self) -> u64 {
        self.threshold_ns / 1_000_000
    }

    pub fn set_threshold_ms(&mut self, ms: u64) {
        self.threshold_ns = ms.max(1) * 1_000_000;
    }

    pub fn in_stall(&self) -> Option<u64> {
        self.in_stall
    }

    /// Feed one sample. Returns a transition when the state changes.
    pub fn observe(&mut self, s: Sample) -> Option<Transition> {
        let new_beat = s.last_beat_ns != 0 && s.last_beat_ns != self.seen_beat_ns;
        self.seen_beat_ns = s.last_beat_ns;

        match self.in_stall {
            Some(start_ns) => {
                // A stall ends on the first new heartbeat, or when the run loop
                // goes back to sleep (it finished the long piece of work).
                if new_beat || s.waiting {
                    self.in_stall = None;
                    let end_ns = if new_beat { s.last_beat_ns } else { s.now_ns };
                    return Some(Transition::Ended {
                        start_ns,
                        end_ns: end_ns.max(start_ns),
                    });
                }
                None
            }
            None => {
                if s.waiting || s.last_beat_ns == 0 {
                    return None;
                }
                if s.now_ns.saturating_sub(s.last_beat_ns) >= self.threshold_ns {
                    self.in_stall = Some(s.last_beat_ns);
                    return Some(Transition::Started {
                        start_ns: s.last_beat_ns,
                    });
                }
                None
            }
        }
    }
}

/// Tracks how long FileMaker reports "script running" (idle level 3)
/// without a break. A long script is not a frozen UI (FileMaker still pumps
/// events), but the user still waits through it, so it is worth a record.
#[derive(Debug, Clone, Default)]
pub struct BusySpan {
    since_ns: Option<u64>,
}

impl BusySpan {
    /// Feed the idle level at a heartbeat. Returns (start, end) of a finished
    /// span when the level leaves "script running".
    pub fn on_level(&mut self, now_ns: u64, script_running: bool) -> Option<(u64, u64)> {
        match (self.since_ns, script_running) {
            (None, true) => {
                self.since_ns = Some(now_ns);
                None
            }
            (Some(start), false) => {
                self.since_ns = None;
                Some((start, now_ns))
            }
            _ => None,
        }
    }
}

/// Second check of "the run loop sleeps". The run loop reports
/// BeforeWaiting, but code can still run after that in the same observer
/// pass. If `waiting` has been true for `threshold` and the main thread is
/// still busy at 2 polls in a row (with no new beat), the thread does not
/// sleep: [`SleepCheck::waiting`] then returns false, so the
/// [`StallDetector`] sees the gap as a freeze.
///
/// Two polls, not one: after a real sleep, the thread is busy for a few
/// microseconds before its AfterWaiting beat. One read in that window must
/// not turn a long idle time into a freeze.
#[derive(Debug, Clone, Default)]
pub struct SleepCheck {
    streak: u32,
    beat_ns: u64,
}

impl SleepCheck {
    /// The `waiting` value that the detector must use for this sample.
    /// `thread_busy` is called only when `s.waiting` has lasted `threshold`.
    pub fn waiting(
        &mut self,
        s: Sample,
        threshold_ns: u64,
        thread_busy: impl FnOnce() -> bool,
    ) -> bool {
        if !s.waiting || s.now_ns.saturating_sub(s.last_beat_ns) < threshold_ns {
            self.streak = 0;
            return s.waiting;
        }
        if s.last_beat_ns != self.beat_ns {
            self.beat_ns = s.last_beat_ns;
            self.streak = 0;
        }
        if thread_busy() {
            self.streak = self.streak.saturating_add(1);
        } else {
            self.streak = 0;
        }
        self.streak < 2
    }
}

/// What [`Joiner::step`] asks the watchdog to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOut {
    /// A new freeze starts (its first part). Read its "before" context now.
    Open { id: u64, start_ns: u64 },
    /// The freeze still lasts: send a progress update.
    Progress {
        id: u64,
        start_ns: u64,
        now_ns: u64,
        parts: u32,
    },
    /// The freeze is over: send the final freeze event.
    Close {
        id: u64,
        start_ns: u64,
        end_ns: u64,
        parts: u32,
    },
}

#[derive(Debug, Clone, Copy)]
struct OpenFreeze {
    id: u64,
    start_ns: u64,
    /// End of the last part; None while a part lasts.
    end_ns: Option<u64>,
    parts: u32,
    last_progress_ns: u64,
    /// Since when the run loop sleeps, after the last part ended.
    slept_since: Option<u64>,
}

/// Default join window, ms. FileMaker can pump events for 50-220 ms
/// between the steps of a long script (log of 2026-11-13), so 50 ms is too
/// short. A user pause is longer.
pub const JOIN_MS: u64 = 300;

/// Joins the parts of one long wait into one freeze.
///
/// FileMaker can run the run loop for a moment in a long operation (about
/// once a second in a long script step). Each run is a beat, so the
/// [`StallDetector`] sees parts of about 1 s with no gap between them. A
/// part that starts at most `join` after the end of the last part is the
/// same freeze. A sleep of the run loop that is shorter than `join` does
/// not split it: FileMaker can sleep for a moment between script steps.
///
/// The decision comes late: a new part is known only `threshold` after it
/// starts. So a freeze closes `join + threshold` (plus 2 polls) after its
/// last part, or when the run loop has slept for `join` (an idle UI).
#[derive(Debug, Clone)]
pub struct Joiner {
    join_ns: u64,
    settle_ns: u64,
    progress_ns: u64,
    next_id: u64,
    open: Option<OpenFreeze>,
}

impl Joiner {
    /// `progress_ms` = 0: no progress updates.
    pub fn new(join_ms: u64, threshold_ms: u64, poll_ms: u64, progress_ms: u64) -> Self {
        Self {
            join_ns: join_ms * 1_000_000,
            settle_ns: (join_ms + threshold_ms + 2 * poll_ms.max(1)) * 1_000_000,
            progress_ns: progress_ms * 1_000_000,
            next_id: 1,
            open: None,
        }
    }

    pub fn is_open(&self) -> bool {
        self.open.is_some()
    }

    /// Feed the detector's transition (if any) of this poll. `slept` is
    /// true when the run loop really sleeps now. With `join` = 0, a sleep
    /// closes the freeze at once.
    pub fn step(&mut self, now_ns: u64, t: Option<Transition>, slept: bool) -> Vec<JoinOut> {
        let mut out = Vec::new();
        match t {
            Some(Transition::Started { start_ns }) => match &mut self.open {
                Some(o) if o.end_ns.is_some_and(|e| start_ns <= e + self.join_ns) => {
                    o.end_ns = None;
                    o.parts += 1;
                }
                _ => {
                    if let Some(c) = self.close() {
                        out.push(c);
                    }
                    let id = self.next_id;
                    self.next_id += 1;
                    self.open = Some(OpenFreeze {
                        id,
                        start_ns,
                        end_ns: None,
                        parts: 1,
                        last_progress_ns: start_ns,
                        slept_since: None,
                    });
                    out.push(JoinOut::Open { id, start_ns });
                }
            },
            Some(Transition::Ended { end_ns, .. }) => {
                if let Some(o) = &mut self.open {
                    o.end_ns = Some(end_ns.max(o.start_ns));
                    o.slept_since = None;
                }
            }
            None => {}
        }
        if let Some(o) = &mut self.open {
            match o.end_ns {
                Some(e) => {
                    o.slept_since = if slept {
                        Some(o.slept_since.unwrap_or(now_ns))
                    } else {
                        None
                    };
                    let idle = o
                        .slept_since
                        .is_some_and(|t| now_ns.saturating_sub(t) >= self.join_ns);
                    if idle || now_ns >= e + self.settle_ns {
                        out.extend(self.close());
                    }
                }
                None => {
                    if self.progress_ns > 0
                        && now_ns.saturating_sub(o.start_ns) >= self.progress_ns
                        && now_ns.saturating_sub(o.last_progress_ns) >= self.progress_ns
                    {
                        o.last_progress_ns = now_ns;
                        out.push(JoinOut::Progress {
                            id: o.id,
                            start_ns: o.start_ns,
                            now_ns,
                            parts: o.parts,
                        });
                    }
                }
            }
        }
        out
    }

    /// Close the open freeze if its last part has ended (at stop).
    pub fn flush(&mut self) -> Option<JoinOut> {
        if self.open.is_some_and(|o| o.end_ns.is_some()) {
            self.close()
        } else {
            None
        }
    }

    fn close(&mut self) -> Option<JoinOut> {
        let o = self.open.take()?;
        Some(JoinOut::Close {
            id: o.id,
            start_ns: o.start_ns,
            end_ns: o.end_ns.unwrap_or(o.start_ns),
            parts: o.parts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const MS: u64 = 1_000_000;

    fn s(now_ms: u64, beat_ms: u64, waiting: bool) -> Sample {
        Sample {
            now_ns: now_ms * MS,
            last_beat_ns: beat_ms * MS,
            waiting,
        }
    }

    #[test]
    fn no_beat_yet_is_not_a_stall() {
        let mut d = StallDetector::new(250);
        assert_eq!(
            d.observe(Sample {
                now_ns: 10_000 * MS,
                last_beat_ns: 0,
                waiting: false
            }),
            None
        );
    }

    #[test]
    fn regular_beats_never_stall() {
        let mut d = StallDetector::new(250);
        for t in (0..5_000).step_by(20) {
            assert_eq!(d.observe(s(t + 1, t.max(1), false)), None);
        }
    }

    #[test]
    fn gap_over_threshold_starts_and_next_beat_ends() {
        let mut d = StallDetector::new(250);
        assert_eq!(d.observe(s(1_000, 1_000, false)), None);
        assert_eq!(d.observe(s(1_200, 1_000, false)), None);
        assert_eq!(
            d.observe(s(1_250, 1_000, false)),
            Some(Transition::Started {
                start_ns: 1_000 * MS
            })
        );
        // Still frozen: no second Started.
        assert_eq!(d.observe(s(3_000, 1_000, false)), None);
        let t = d.observe(s(5_210, 5_200, false)).unwrap();
        assert_eq!(
            t,
            Transition::Ended {
                start_ns: 1_000 * MS,
                end_ns: 5_200 * MS
            }
        );
        assert_eq!(t.duration_ns(), Some(4_200 * MS));
        assert!(d.in_stall().is_none());
    }

    #[test]
    fn sleeping_run_loop_is_not_frozen() {
        let mut d = StallDetector::new(250);
        d.observe(s(1_000, 1_000, false));
        // The user walks away: no beats for a minute, but the run loop sleeps.
        assert_eq!(d.observe(s(61_000, 1_000, true)), None);
    }

    #[test]
    fn stall_ends_when_run_loop_goes_to_sleep() {
        let mut d = StallDetector::new(100);
        d.observe(s(1, 1, false));
        assert!(matches!(
            d.observe(s(500, 1, false)),
            Some(Transition::Started { .. })
        ));
        let t = d.observe(s(900, 1, true)).unwrap();
        assert_eq!(
            t,
            Transition::Ended {
                start_ns: MS,
                end_ns: 900 * MS
            }
        );
    }

    #[test]
    fn back_to_back_stalls_are_separate() {
        let mut d = StallDetector::new(100);
        d.observe(s(10, 10, false));
        assert!(d.observe(s(200, 10, false)).is_some());
        assert!(matches!(
            d.observe(s(400, 390, false)),
            Some(Transition::Ended { .. })
        ));
        assert!(
            matches!(d.observe(s(600, 390, false)), Some(Transition::Started { start_ns }) if start_ns == 390 * MS)
        );
    }

    #[test]
    fn threshold_can_change() {
        let mut d = StallDetector::new(1000);
        d.observe(s(1, 1, false));
        assert_eq!(d.observe(s(300, 1, false)), None);
        d.set_threshold_ms(200);
        assert!(d.observe(s(301, 1, false)).is_some());
    }

    #[test]
    fn busy_span() {
        let mut b = BusySpan::default();
        assert_eq!(b.on_level(10, false), None);
        assert_eq!(b.on_level(20, true), None);
        assert_eq!(b.on_level(30, true), None);
        assert_eq!(b.on_level(90, false), Some((20, 90)));
        assert_eq!(b.on_level(95, false), None);
    }
    fn sw(now_ms: u64, beat_ms: u64) -> Sample {
        s(now_ms, beat_ms, true)
    }

    #[test]
    fn sleep_check_trusts_a_real_sleep() {
        let mut c = SleepCheck::default();
        for t in (1_000..5_000).step_by(10) {
            assert!(c.waiting(sw(t, 1_000), 250 * MS, || false));
        }
    }

    #[test]
    fn sleep_check_does_not_ask_before_the_threshold() {
        let mut c = SleepCheck::default();
        assert!(c.waiting(sw(1_100, 1_000), 250 * MS, || panic!("asked")));
        assert!(!c.waiting(s(1_100, 1_000, false), 250 * MS, || panic!("asked")));
    }

    #[test]
    fn sleep_check_needs_two_busy_polls() {
        let mut c = SleepCheck::default();
        // One busy read (a wake before AfterWaiting): still waiting.
        assert!(c.waiting(sw(1_300, 1_000), 250 * MS, || true));
        assert!(c.waiting(sw(1_310, 1_000), 250 * MS, || false));
        // Two in a row: not waiting.
        assert!(c.waiting(sw(1_320, 1_000), 250 * MS, || true));
        assert!(!c.waiting(sw(1_330, 1_000), 250 * MS, || true));
        // A new beat starts a new count.
        assert!(c.waiting(sw(1_600, 1_340), 250 * MS, || true));
    }

    #[test]
    fn busy_in_the_before_waiting_pass_is_a_freeze() {
        // Beat + "waiting" at 1_000, then the thread works for 800 ms in the
        // same observer pass, then really sleeps (not busy) at 1_800.
        let mut d = StallDetector::new(250);
        let mut c = SleepCheck::default();
        let mut got = Vec::new();
        for t in (1_000..2_200).step_by(10) {
            let busy = t < 1_800;
            let raw = sw(t, 1_000);
            let w = c.waiting(raw, 250 * MS, || busy);
            if let Some(tr) = d.observe(Sample { waiting: w, ..raw }) {
                got.push(tr);
            }
        }
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(
            got[0],
            Transition::Started {
                start_ns: 1_000 * MS
            }
        );
        let Transition::Ended { start_ns, end_ns } = got[1] else {
            panic!("{got:?}")
        };
        assert_eq!(start_ns, 1_000 * MS);
        assert_eq!(end_ns, 1_800 * MS);
    }
    /// Run the detector and the joiner on beats at `beats_ms` (and "asleep"
    /// from `sleep_from_ms`), polling every 10 ms up to `until_ms`.
    fn join_run(beats_ms: &[u64], sleep_from_ms: u64, until_ms: u64) -> Vec<JoinOut> {
        join_run_with(JOIN_MS, beats_ms, sleep_from_ms, until_ms)
    }

    fn join_run_with(
        join_ms: u64,
        beats_ms: &[u64],
        sleep_from_ms: u64,
        until_ms: u64,
    ) -> Vec<JoinOut> {
        let mut d = StallDetector::new(250);
        let mut j = Joiner::new(join_ms, 250, 10, 1000);
        let mut out = Vec::new();
        for t in (0..=until_ms).step_by(10) {
            let beat = beats_ms
                .iter()
                .copied()
                .filter(|b| *b <= t)
                .max()
                .unwrap_or(0);
            let waiting = t >= sleep_from_ms;
            let tr = d.observe(s(t, beat, waiting));
            out.extend(j.step(t * MS, tr, waiting));
        }
        out
    }

    #[test]
    fn parts_with_no_gap_are_one_freeze_with_progress() {
        // Busy from 1_000 to 9_000; FileMaker pumps the run loop once a
        // second (one beat), as in the log of 2026-11-11 16:35.
        let mut beats: Vec<u64> = (0..=1_000).step_by(10).collect();
        beats.extend((2_000..=9_000).step_by(1_000));
        beats.extend((9_000..=9_400).step_by(10));
        let out = join_run(&beats, 9_400, 10_000);
        let opens: Vec<_> = out
            .iter()
            .filter(|o| matches!(o, JoinOut::Open { .. }))
            .collect();
        assert_eq!(
            opens,
            [&JoinOut::Open {
                id: 1,
                start_ns: 1_000 * MS
            }]
        );
        let progress: Vec<u64> = out
            .iter()
            .filter_map(|o| match o {
                JoinOut::Progress { id: 1, now_ns, .. } => Some(now_ns / MS),
                _ => None,
            })
            .collect();
        assert!(progress.len() >= 6, "{progress:?}");
        assert!(
            progress.windows(2).all(|w| w[1] - w[0] >= 1_000),
            "{progress:?}"
        );
        let closes: Vec<_> = out
            .iter()
            .filter(|o| matches!(o, JoinOut::Close { .. }))
            .collect();
        assert_eq!(
            closes,
            [&JoinOut::Close {
                id: 1,
                start_ns: 1_000 * MS,
                end_ns: 9_000 * MS,
                parts: 8
            }]
        );
    }

    fn closes(out: &[JoinOut]) -> Vec<(u64, u64, u64, u32)> {
        out.iter()
            .filter_map(|o| match o {
                JoinOut::Close {
                    id,
                    start_ns,
                    end_ns,
                    parts,
                } => Some((*id, start_ns / MS, end_ns / MS, *parts)),
                _ => None,
            })
            .collect()
    }

    /// Busy `parts` (start, end) ms, as in the log of 2026-11-13 18:15
    /// (`Income statements - month over month - collect data`). Between the
    /// parts FileMaker beats every ms; if `sleep_in_gaps`, the run loop
    /// also sleeps there. The run loop sleeps after the last part.
    fn replay(parts: &[(u64, u64)], sleep_in_gaps: bool) -> Vec<JoinOut> {
        let mut d = StallDetector::new(250);
        let mut j = Joiner::new(JOIN_MS, 250, 10, 1000);
        let last_end = parts.last().unwrap().1;
        let busy = |t: u64| parts.iter().any(|&(a, b)| t > a && t < b);
        let mut beat = 0;
        let mut out = Vec::new();
        for t in 0..=last_end + 2_000 {
            if !busy(t) {
                beat = t;
            }
            if t % 10 != 0 {
                continue;
            }
            let waiting = t >= last_end + 100 || (sleep_in_gaps && !busy(t) && t > parts[0].0);
            let tr = d.observe(s(t, beat, waiting));
            out.extend(j.step(t * MS, tr, waiting));
        }
        out
    }

    /// The parts of the log: durations and the gaps (56-214 ms) between them.
    fn logged_parts() -> Vec<(u64, u64)> {
        let durations = [1_855, 1_846, 1_876, 1_934, 1_957, 3_966, 1_939];
        let gaps = [214, 159, 165, 132, 75, 56, 64];
        let mut t = 1_000;
        let mut v = Vec::new();
        for (d, g) in durations.iter().zip(gaps) {
            v.push((t, t + d));
            t += d + g;
        }
        v
    }

    #[test]
    fn logged_script_gaps_make_one_freeze() {
        let parts = logged_parts();
        let (start, end) = (parts[0].0, parts.last().unwrap().1);
        let got = closes(&replay(&parts, false));
        assert_eq!(got.len(), 1, "{got:?}");
        let (id, s0, e0, n) = got[0];
        // The end is the beat seen at the next poll (10 ms).
        assert_eq!((id, s0, n), (1, start, 7));
        assert!((end..=end + 10).contains(&e0), "{got:?}");
    }

    #[test]
    fn a_short_sleep_between_script_steps_does_not_split() {
        let parts = logged_parts();
        let (start, end) = (parts[0].0, parts.last().unwrap().1);
        let got = closes(&replay(&parts, true));
        assert_eq!(got.len(), 1, "{got:?}");
        let (id, s0, e0, n) = got[0];
        // The end is the beat seen at the next poll (10 ms).
        assert_eq!((id, s0, n), (1, start, 7));
        assert!((end..=end + 10).contains(&e0), "{got:?}");
    }

    #[test]
    fn a_user_pause_splits() {
        // Two busy parts with a 1 s pause of an idle UI (the run loop
        // sleeps) between them.
        let got = closes(&replay(&[(1_000, 2_000), (3_000, 4_000)], true));
        assert_eq!(got, [(1, 1_000, 2_000, 1), (2, 3_000, 4_000, 1)]);
        // The same pause, the UI awake (beats, no sleep): also two.
        let got = closes(&replay(&[(1_000, 2_000), (3_000, 4_000)], false));
        assert_eq!(got, [(1, 1_000, 2_000, 1), (2, 3_000, 4_000, 1)]);
    }

    #[test]
    fn a_gap_longer_than_join_makes_two_freezes() {
        // Stall 1_000..1_500, beats every 10 ms to 1_600, stall 1_600..2_100.
        let mut beats: Vec<u64> = (0..=1_000).step_by(10).collect();
        beats.extend((1_500..=1_600).step_by(10));
        beats.extend((2_100..=2_500).step_by(10));
        let out = join_run_with(50, &beats, 10_000, 3_000);
        let closes: Vec<_> = out
            .iter()
            .filter_map(|o| match o {
                JoinOut::Close {
                    id,
                    start_ns,
                    end_ns,
                    parts,
                } => Some((*id, start_ns / MS, end_ns / MS, *parts)),
                _ => None,
            })
            .collect();
        assert_eq!(closes, [(1, 1_000, 1_500, 1), (2, 1_600, 2_100, 1)]);
        assert!(!out.iter().any(|o| matches!(o, JoinOut::Progress { .. })));
    }

    #[test]
    fn a_real_sleep_closes_after_join() {
        let beats: Vec<u64> = (0..=1_000).step_by(10).collect();
        // Busy 1_000..1_600, then the run loop sleeps.
        let mut d = StallDetector::new(250);
        let mut j = Joiner::new(JOIN_MS, 250, 10, 1000);
        let mut closed_at = None;
        for t in (0..=2_000).step_by(10) {
            let beat = beats.iter().copied().filter(|b| *b <= t).max().unwrap_or(0);
            let waiting = t >= 1_600;
            let tr = d.observe(s(t, beat, waiting));
            for o in j.step(t * MS, tr, waiting) {
                if let JoinOut::Close { end_ns, .. } = o {
                    closed_at = Some((t, end_ns / MS));
                }
            }
        }
        assert_eq!(closed_at, Some((1_600 + JOIN_MS, 1_600)));
        assert!(!j.is_open());
    }

    #[test]
    fn with_join_0_a_real_sleep_closes_at_once() {
        let beats: Vec<u64> = (0..=1_000).step_by(10).collect();
        let mut d = StallDetector::new(250);
        let mut j = Joiner::new(0, 250, 10, 1000);
        let mut closed_at = None;
        for t in (0..=2_000).step_by(10) {
            let beat = beats.iter().copied().filter(|b| *b <= t).max().unwrap_or(0);
            let waiting = t >= 1_600;
            let tr = d.observe(s(t, beat, waiting));
            for o in j.step(t * MS, tr, waiting) {
                if let JoinOut::Close { .. } = o {
                    closed_at = Some(t);
                }
            }
        }
        assert_eq!(closed_at, Some(1_600));
    }

    #[test]
    fn flush_closes_only_an_ended_freeze() {
        let mut j = Joiner::new(50, 250, 10, 0);
        let o = j.step(
            1_250 * MS,
            Some(Transition::Started {
                start_ns: 1_000 * MS,
            }),
            false,
        );
        assert_eq!(
            o,
            [JoinOut::Open {
                id: 1,
                start_ns: 1_000 * MS
            }]
        );
        assert_eq!(j.flush(), None, "a part still lasts");
        j.step(
            1_400 * MS,
            Some(Transition::Ended {
                start_ns: 1_000 * MS,
                end_ns: 1_400 * MS,
            }),
            false,
        );
        assert_eq!(
            j.flush(),
            Some(JoinOut::Close {
                id: 1,
                start_ns: 1_000 * MS,
                end_ns: 1_400 * MS,
                parts: 1
            })
        );
    }
}
