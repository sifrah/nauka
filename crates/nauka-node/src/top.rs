//! `nauka top` — the live, full-screen view of a cluster: per-node fill
//! and migration rates while a rebalance runs, the registry one keypress
//! away. Read-only by design: this is a dashboard, not a control panel —
//! destructive acts stay explicit commands.
//!
//! Data comes from each member's plain HTTP API (`/api/status` carries
//! `self_used_bytes`): no cluster identity, works from any laptop that
//! can reach the nodes. A member whose API does not answer is shown
//! unreachable rather than dropped — absence of data is data.

use std::collections::{HashMap, VecDeque};
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Clear, Gauge, Paragraph, Row, Sparkline, Table};
use ratatui::Frame;

/// How much per-node history the sparklines and rates look at.
const HISTORY: usize = 60;
/// A node is "moving" when its store changed by more than this per tick.
const QUIET_BYTES: u64 = 1024 * 1024;

#[derive(serde::Deserialize)]
struct ApiNode {
    addr: String,
    #[serde(default)]
    id: Option<u64>,
    capacity_bytes: u64,
    is_leader: bool,
    #[serde(default)]
    is_alive: bool,
    #[serde(default)]
    disabled: bool,
}

#[derive(serde::Deserialize)]
struct ApiStatus {
    leader: Option<String>,
    nodes: Vec<ApiNode>,
    files: usize,
    total_bytes: u64,
    /// Absent on pre-0.5.26 nodes — shown as "n/a", never a crash.
    #[serde(default)]
    self_used_bytes: Option<u64>,
    #[serde(default)]
    self_shard_count: Option<u64>,
    #[serde(default)]
    self_net_rx_bytes: Option<u64>,
    #[serde(default)]
    self_net_tx_bytes: Option<u64>,
}

#[derive(serde::Deserialize, Clone)]
struct ApiFile {
    hash: String,
    size: u64,
    name: Option<String>,
}

#[derive(Clone, Default)]
struct NodeState {
    id: Option<u64>,
    capacity: u64,
    is_leader: bool,
    disabled: bool,
    alive_per_seed: bool,
    reachable: bool,
    used: Option<u64>,
    shards: Option<u64>,
    /// (instant, used bytes) samples, newest last.
    history: VecDeque<(Instant, u64)>,
    /// (instant, cumulative rx, cumulative tx) — short window: network is
    /// live traffic, a minute-long average would hide every burst.
    net: VecDeque<(Instant, u64, u64)>,
}

impl NodeState {
    /// Bytes/second over the sampled window; None below two samples.
    fn rate(&self) -> Option<f64> {
        let (first, last) = (self.history.front()?, self.history.back()?);
        let dt = last.0.duration_since(first.0).as_secs_f64();
        if dt < 1.0 {
            return None;
        }
        Some((last.1 as f64 - first.1 as f64) / dt)
    }

    /// (ingress, egress) in bytes/second over the short network window.
    fn net_rates(&self) -> Option<(f64, f64)> {
        let (first, last) = (self.net.front()?, self.net.back()?);
        let dt = last.0.duration_since(first.0).as_secs_f64();
        if dt < 0.5 {
            return None;
        }
        Some((
            last.1.saturating_sub(first.1) as f64 / dt,
            last.2.saturating_sub(first.2) as f64 / dt,
        ))
    }

    fn moving(&self) -> bool {
        let mut it = self.history.iter().rev();
        match (it.next(), it.next()) {
            (Some(a), Some(b)) => a.1.abs_diff(b.1) > QUIET_BYTES,
            _ => false,
        }
    }
}

struct App {
    seed_api: String,
    interval: Duration,
    paused: bool,
    tab: Tab,
    sort: Sort,
    nodes: HashMap<String, NodeState>,
    order: Vec<String>,
    leader: Option<String>,
    files_count: usize,
    logical_bytes: u64,
    files: Vec<ApiFile>,
    files_filter: String,
    files_scroll: usize,
    quiet_since: Option<Instant>,
    seed_error: Option<String>,
    started: Instant,
    // ── Interactive control ──
    /// Whether admin actions are possible: the cluster identity was
    /// installed at startup (a member machine, or --token). Read-only
    /// otherwise — the menu says why.
    can_admin: bool,
    /// Highlighted node, an index into `sorted_addrs()`.
    selected: usize,
    focus: Focus,
    /// Transient result of the last action, shown in the banner.
    action_msg: Option<(String, bool)>,
}

/// What has the keyboard: the table, the per-node action menu, or a
/// confirmation prompt for a chosen action.
enum Focus {
    Normal,
    Menu,
    Confirm(PendingAction),
}

/// A node action awaiting confirmation.
struct PendingAction {
    kind: Action,
    addr: String,
    node_id: Option<u64>,
}

#[derive(Clone, Copy, PartialEq)]
enum Action {
    Disable,
    Enable,
    Remove,
}

impl Action {
    fn label(self) -> &'static str {
        match self {
            Action::Disable => "Disable (drain)",
            Action::Enable => "Enable (rejoin placement)",
            Action::Remove => "Remove from cluster",
        }
    }
    fn key(self) -> char {
        match self {
            Action::Disable => 'd',
            Action::Enable => 'e',
            Action::Remove => 'r',
        }
    }
}

#[derive(PartialEq, Clone, Copy)]
enum Tab {
    Nodes,
    Files,
}

#[derive(Clone, Copy)]
enum Sort {
    Used,
    Rate,
    Addr,
    Capacity,
}

impl Sort {
    fn next(self) -> Self {
        match self {
            Sort::Used => Sort::Rate,
            Sort::Rate => Sort::Addr,
            Sort::Addr => Sort::Capacity,
            Sort::Capacity => Sort::Used,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Sort::Used => "used",
            Sort::Rate => "rate",
            Sort::Addr => "addr",
            Sort::Capacity => "capacity",
        }
    }
}

impl App {
    /// The node addresses in display order — the single source of truth
    /// shared by the renderer and the selection, so the highlighted row
    /// and the acted-on node are always the same one.
    fn sorted_addrs(&self) -> Vec<String> {
        let mut addrs: Vec<String> = self.order.clone();
        addrs.sort_by(|a, b| {
            let (na, nb) = (&self.nodes[a], &self.nodes[b]);
            match self.sort {
                Sort::Used => nb.used.unwrap_or(0).cmp(&na.used.unwrap_or(0)),
                Sort::Rate => nb
                    .rate()
                    .unwrap_or(0.0)
                    .abs()
                    .partial_cmp(&na.rate().unwrap_or(0.0).abs())
                    .unwrap_or(std::cmp::Ordering::Equal),
                Sort::Addr => a.cmp(b),
                Sort::Capacity => nb.capacity.cmp(&na.capacity),
            }
        });
        addrs
    }

    /// Consensus-plane addresses of the members, for admin RPCs.
    fn peers(&self) -> Vec<SocketAddr> {
        self.order.iter().filter_map(|a| a.parse().ok()).collect()
    }

    /// The actions offered for a node in its current state.
    fn actions_for(&self, addr: &str) -> Vec<Action> {
        let disabled = self.nodes.get(addr).map(|n| n.disabled).unwrap_or(false);
        if disabled {
            vec![Action::Enable, Action::Remove]
        } else {
            vec![Action::Disable, Action::Remove]
        }
    }
}

/// Flip a node's draining state through the leader — quiet, no terminal
/// output (we are inside the alternate screen).
async fn apply_disabled(peers: &[SocketAddr], addr: &str, disabled: bool) -> Result<(), String> {
    match nauka_raft::write_via_leader(
        peers,
        nauka_raft::types::AppCommand::SetNodeDisabled {
            addr: addr.to_string(),
            disabled,
        },
    )
    .await
    {
        Ok(r) if r.ok => Ok(()),
        Ok(_) => Err("the cluster refused the change".into()),
        Err(e) => Err(one_line(&e.to_string())),
    }
}

/// Drop a member from the voter set (same two steps as `nauka node
/// remove`), quietly.
async fn apply_remove(peers: &[SocketAddr], node_id: u64) -> Result<(), String> {
    use nauka_raft::types::{AdminRequest, AdminResponse};
    let members = match nauka_raft::admin_via_leader(peers, &AdminRequest::Metrics).await {
        Ok(AdminResponse::Metrics { members, .. }) => members,
        Ok(other) => return Err(format!("metrics: {other:?}")),
        Err(e) => return Err(one_line(&e.to_string())),
    };
    let ids: Vec<u64> = members.keys().copied().filter(|i| *i != node_id).collect();
    if ids.len() == members.len() {
        return Err("that node is not a member".into());
    }
    match nauka_raft::admin_via_leader(peers, &AdminRequest::ChangeMembership(ids)).await {
        Ok(AdminResponse::Ok(_)) => Ok(()),
        Ok(other) => Err(format!("{other:?}")),
        Err(e) => Err(one_line(&e.to_string())),
    }
}

fn one_line(s: &str) -> String {
    s.lines().next().unwrap_or(s).to_string()
}

fn human(b: u64) -> String {
    indicatif::HumanBytes(b).to_string()
}

fn human_rate(bps: f64) -> String {
    let sign = if bps < 0.0 { "−" } else { "+" };
    let per_min = bps.abs() * 60.0;
    if per_min < 512.0 * 1024.0 && bps.abs() < QUIET_BYTES as f64 / 5.0 {
        return "stable".into();
    }
    format!("{sign}{}/min", human(per_min as u64))
}

/// One polling pass: the seed's status names the members; every member's
/// own API is then asked for its disk usage, concurrently.
async fn poll(app: &mut App) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("client");
    let seed = format!("{}/api/status", app.seed_api.trim_end_matches('/'));
    let status: ApiStatus = match client.get(&seed).send().await {
        Ok(r) => match r.error_for_status() {
            Ok(r) => match r.json().await {
                Ok(s) => s,
                Err(e) => {
                    app.seed_error = Some(format!("bad status payload: {e}"));
                    return;
                }
            },
            Err(e) => {
                app.seed_error = Some(e.to_string());
                return;
            }
        },
        Err(e) => {
            app.seed_error = Some(format!("no node answering at {seed}: {e}"));
            return;
        }
    };
    app.seed_error = None;
    app.leader = status.leader.clone();
    app.files_count = status.files;
    app.logical_bytes = status.total_bytes;

    // Membership from the seed; usage from each member itself. The HTTP
    // port is assumed 8080 on every node — the same convention `node add`
    // provisions and relies on.
    let mut fetches = Vec::new();
    for n in &status.nodes {
        let ip = n.addr.split(':').next().unwrap_or(&n.addr).to_string();
        let url = format!("http://{ip}:8080/api/status");
        let client = client.clone();
        fetches.push(async move {
            let got: Option<ApiStatus> = match client.get(&url).send().await {
                Ok(r) => r.json().await.ok(),
                Err(_) => None,
            };
            (url, got)
        });
    }
    let results = futures::future::join_all(fetches).await;

    let now = Instant::now();
    let mut order = Vec::new();
    for (n, (_url, got)) in status.nodes.iter().zip(results) {
        order.push(n.addr.clone());
        let entry = app.nodes.entry(n.addr.clone()).or_default();
        entry.id = n.id;
        entry.capacity = n.capacity_bytes;
        entry.is_leader = n.is_leader;
        entry.disabled = n.disabled;
        entry.alive_per_seed = n.is_alive;
        match got {
            Some(s) => {
                entry.reachable = true;
                entry.used = s.self_used_bytes;
                entry.shards = s.self_shard_count;
                if let Some(u) = s.self_used_bytes {
                    entry.history.push_back((now, u));
                    while entry.history.len() > HISTORY {
                        entry.history.pop_front();
                    }
                }
                if let (Some(rx), Some(tx)) = (s.self_net_rx_bytes, s.self_net_tx_bytes) {
                    entry.net.push_back((now, rx, tx));
                    while entry.net.len() > 6 {
                        entry.net.pop_front();
                    }
                }
            }
            None => entry.reachable = false,
        }
    }
    app.nodes.retain(|k, _| order.contains(k));
    app.order = order;

    if app.nodes.values().any(NodeState::moving) {
        app.quiet_since = None;
    } else if app.quiet_since.is_none() {
        app.quiet_since = Some(now);
    }
}

async fn poll_files(app: &mut App) {
    let url = format!("{}/api/files", app.seed_api.trim_end_matches('/'));
    if let Ok(r) = reqwest::Client::new()
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        if let Ok(files) = r.json::<Vec<ApiFile>>().await {
            app.files = files;
        }
    }
}

fn draw(f: &mut Frame, app: &App) {
    let [header, body, banner, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(f.area());

    // ── Header ──────────────────────────────────────────────────────────
    let alive = app
        .nodes
        .values()
        .filter(|n| n.alive_per_seed || n.reachable)
        .count();
    let shard_total: u64 = app.nodes.values().filter_map(|n| n.used).sum();
    let mut spans = vec![
        Span::styled(" nauka top ", Style::new().bold().fg(Color::Cyan)),
        Span::raw(format!("— {} nodes · {} alive · ", app.nodes.len(), alive)),
        Span::raw(format!(
            "{} files · {} logical · {} shards on disk",
            app.files_count,
            human(app.logical_bytes),
            human(shard_total)
        )),
    ];
    if let Some(l) = &app.leader {
        spans.push(Span::raw(format!(" · leader {l}")));
    } else {
        spans.push(Span::styled(
            " · NO LEADER",
            Style::new().fg(Color::Red).bold(),
        ));
    }
    if let Some(e) = &app.seed_error {
        spans = vec![Span::styled(
            format!(" nauka top — {e}"),
            Style::new().fg(Color::Red),
        )];
    }
    f.render_widget(Paragraph::new(Line::from(spans)), header);

    match app.tab {
        Tab::Nodes => draw_nodes(f, app, body),
        Tab::Files => draw_files(f, app, body),
    }

    // ── Banner: last action result, else the rebalance state ────────────
    let line = if let Some((msg, is_err)) = &app.action_msg {
        let mark = if *is_err {
            Span::styled(" ✗ ", Style::new().fg(Color::Red).bold())
        } else {
            Span::styled(" ✓ ", Style::new().fg(Color::Green).bold())
        };
        Line::from(vec![mark, Span::raw(msg.clone())])
    } else {
        match app.quiet_since {
            Some(t) if t.duration_since(app.started) > Duration::ZERO || !app.nodes.is_empty() => {
                let secs = t.elapsed().as_secs();
                Line::from(vec![
                    Span::styled(" ● ", Style::new().fg(Color::Green)),
                    Span::raw(format!("quiet — no shard movement for {secs}s")),
                ])
            }
            _ => {
                let moving: f64 = app
                    .nodes
                    .values()
                    .filter_map(|n| n.rate())
                    .map(f64::abs)
                    .sum();
                Line::from(vec![
                    Span::styled(" ⇄ ", Style::new().fg(Color::Yellow)),
                    Span::raw(format!(
                        "rebalancing — {} moving across the cluster",
                        human_rate(moving / 2.0).trim_start_matches(['+', '−'])
                    )),
                ])
            }
        }
    };
    f.render_widget(Paragraph::new(line), banner);

    // ── Footer (contextual) ─────────────────────────────────────────────
    let footer_text = match (&app.focus, app.tab) {
        (Focus::Menu, _) => " pick an action  ·  [esc] cancel".to_string(),
        (Focus::Confirm(_), _) => " [y] confirm   [n] cancel".to_string(),
        (Focus::Normal, Tab::Nodes) => format!(
            " [↑↓] select  [enter] actions  [s]ort:{}  [p]{}  [+/-]{}s  [2] files  [q]uit",
            app.sort.label(),
            if app.paused { "aused" } else { "ause" },
            app.interval.as_secs()
        ),
        (Focus::Normal, Tab::Files) => format!(
            " [1] nodes  type to filter: “{}”  [↑↓] scroll  [q]uit",
            app.files_filter
        ),
    };
    f.render_widget(
        Paragraph::new(footer_text).style(Style::new().add_modifier(Modifier::DIM)),
        footer,
    );

    // ── Popups ──────────────────────────────────────────────────────────
    match &app.focus {
        Focus::Menu => draw_menu(f, app),
        Focus::Confirm(p) => draw_confirm(f, app, p),
        Focus::Normal => {}
    }
}

/// A centered popup rect of the given size, clamped to the frame.
fn centered(f: &Frame, w: u16, h: u16) -> Rect {
    let area = f.area();
    let [v] = Layout::vertical([Constraint::Length(h.min(area.height))])
        .flex(Flex::Center)
        .areas(area);
    let [r] = Layout::horizontal([Constraint::Length(w.min(area.width))])
        .flex(Flex::Center)
        .areas(v);
    r
}

fn draw_menu(f: &mut Frame, app: &App) {
    let addrs = app.sorted_addrs();
    let Some(addr) = addrs.get(app.selected) else {
        return;
    };
    let actions = app.actions_for(addr);
    let mut lines = vec![Line::from(Span::styled(
        addr.clone(),
        Style::new().bold().fg(Color::Cyan),
    ))];
    for a in &actions {
        let (style, suffix) = if !app.can_admin {
            (Style::new().add_modifier(Modifier::DIM), "")
        } else if *a == Action::Remove {
            (Style::new().fg(Color::Red), "")
        } else {
            (Style::new(), "")
        };
        lines.push(Line::from(vec![
            Span::styled(format!("  [{}] ", a.key()), Style::new().fg(Color::Yellow)),
            Span::styled(format!("{}{suffix}", a.label()), style),
        ]));
    }
    if !app.can_admin {
        lines.push(Line::from(Span::styled(
            "  read-only: run on a member, or set NAUKA_TOKEN",
            Style::new().fg(Color::Red).add_modifier(Modifier::DIM),
        )));
    }
    let h = lines.len() as u16 + 2;
    let area = centered(f, 46, h);
    f.render_widget(Clear, area);
    f.render_widget(
        Paragraph::new(lines).block(
            Block::bordered()
                .title(" actions ")
                .border_style(Style::new().fg(Color::Cyan)),
        ),
        area,
    );
}

fn draw_confirm(f: &mut Frame, app: &App, p: &PendingAction) {
    let held = app.nodes.get(&p.addr).and_then(|n| n.used).unwrap_or(0);
    let mut lines = vec![Line::from(vec![
        Span::raw(format!(
            "{} ",
            p.kind.label().split_whitespace().next().unwrap_or("")
        )),
        Span::styled(p.addr.clone(), Style::new().bold().fg(Color::Cyan)),
        Span::raw("?"),
    ])];
    match p.kind {
        Action::Disable => lines.push(Line::from(Span::styled(
            "It drains its shards to the others (reversible).",
            Style::new().add_modifier(Modifier::DIM),
        ))),
        Action::Enable => lines.push(Line::from(Span::styled(
            "Shards migrate back toward it over the next scrubs.",
            Style::new().add_modifier(Modifier::DIM),
        ))),
        Action::Remove => {
            lines.push(Line::from(Span::styled(
                "Irreversible. The others re-replicate its shards.",
                Style::new().fg(Color::Red),
            )));
            if held > QUIET_BYTES {
                lines.push(Line::from(Span::styled(
                    format!(
                        "It still holds {} — Disable to drain it first.",
                        human(held)
                    ),
                    Style::new().fg(Color::Yellow),
                )));
            }
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("  [y] ", Style::new().fg(Color::Green).bold()),
        Span::raw("confirm    "),
        Span::styled("[n] ", Style::new().fg(Color::Red).bold()),
        Span::raw("cancel"),
    ]));
    let h = lines.len() as u16 + 2;
    let area = centered(f, 54, h);
    f.render_widget(Clear, area);
    let border = if p.kind == Action::Remove {
        Color::Red
    } else {
        Color::Yellow
    };
    f.render_widget(
        Paragraph::new(lines).block(
            Block::bordered()
                .title(" confirm ")
                .border_style(Style::new().fg(border)),
        ),
        area,
    );
}

fn draw_nodes(f: &mut Frame, app: &App, area: Rect) {
    let addrs = app.sorted_addrs();

    let rows_area = area;
    // Each node gets 2 lines: the data row and its fill gauge.
    let constraints: Vec<Constraint> = addrs.iter().map(|_| Constraint::Length(2)).collect();
    let node_areas = Layout::vertical(constraints).split(rows_area);

    for (i, addr) in addrs.iter().enumerate() {
        if i >= node_areas.len() {
            break;
        }
        let n = &app.nodes[addr];
        let is_sel = i == app.selected;
        let [row, gauge_row] =
            Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).areas(node_areas[i]);
        let [dot_a, addr_a, role_a, used_a, spark_a, rate_a, net_a, shards_a] =
            Layout::horizontal([
                Constraint::Length(2),
                Constraint::Length(23),
                Constraint::Length(8),
                Constraint::Length(11),
                Constraint::Length(14),
                Constraint::Length(14),
                Constraint::Length(24),
                Constraint::Min(10),
            ])
            .areas(row);

        let dot = if is_sel {
            Span::styled("▸", Style::new().fg(Color::Cyan).bold())
        } else if n.reachable && n.alive_per_seed {
            Span::styled("●", Style::new().fg(Color::Green))
        } else if n.reachable || n.alive_per_seed {
            Span::styled("●", Style::new().fg(Color::Yellow))
        } else {
            Span::styled("●", Style::new().fg(Color::Red))
        };
        f.render_widget(Paragraph::new(Line::from(dot)), dot_a);
        let addr_style = if is_sel {
            Style::new().bold().fg(Color::Cyan)
        } else {
            Style::new().bold()
        };
        f.render_widget(Paragraph::new(addr.clone()).style(addr_style), addr_a);
        let (role_txt, role_style) = if n.disabled {
            ("drain", Style::new().fg(Color::Yellow))
        } else if n.is_leader {
            ("leader", Style::new().fg(Color::Cyan))
        } else {
            ("", Style::new())
        };
        f.render_widget(Paragraph::new(role_txt).style(role_style), role_a);
        f.render_widget(
            Paragraph::new(match n.used {
                Some(u) => human(u),
                None => "n/a".into(),
            }),
            used_a,
        );
        // The sparkline shows VARIATION, rebased on the window minimum:
        // raw values would render a flat history as full-height blocks
        // (ratatui scales to the max), which reads as a second gauge. A
        // flat window draws a quiet baseline instead, and the trend
        // borrows the rate's color so the two can never be confused
        // with the capacity gauge below.
        let raw: Vec<u64> = n.history.iter().map(|(_, u)| *u).collect();
        if raw.len() > 1 {
            let lo = *raw.iter().min().expect("non-empty");
            let hi = *raw.iter().max().expect("non-empty");
            if hi - lo <= QUIET_BYTES {
                f.render_widget(
                    Paragraph::new("▁".repeat(spark_a.width as usize))
                        .style(Style::new().fg(Color::DarkGray)),
                    spark_a,
                );
            } else {
                let data: Vec<u64> = raw.iter().map(|v| v - lo).collect();
                let trend = if raw.last() >= raw.first() {
                    Color::Yellow
                } else {
                    Color::Green
                };
                f.render_widget(
                    Sparkline::default()
                        .data(&data)
                        .style(Style::new().fg(trend)),
                    spark_a,
                );
            }
        }
        let (rate_txt, rate_style) = match n.rate() {
            Some(r) if r.abs() >= QUIET_BYTES as f64 / 5.0 => (
                human_rate(r),
                if r > 0.0 {
                    Style::new().fg(Color::Yellow)
                } else {
                    Style::new().fg(Color::Green)
                },
            ),
            Some(_) => ("stable".into(), Style::new().add_modifier(Modifier::DIM)),
            None => ("…".into(), Style::new().add_modifier(Modifier::DIM)),
        };
        f.render_widget(Paragraph::new(rate_txt).style(rate_style), rate_a);
        // Live network state, machine level: what the bandwidth bill sees.
        let net_line = match n.net_rates() {
            Some((rx, tx)) => Line::from(vec![
                Span::styled("▼", Style::new().fg(Color::Blue)),
                Span::raw(format!("{}/s ", human(rx as u64))),
                Span::styled("▲", Style::new().fg(Color::Magenta)),
                Span::raw(format!("{}/s", human(tx as u64))),
            ]),
            None => Line::from(Span::styled(
                "net …",
                Style::new().add_modifier(Modifier::DIM),
            )),
        };
        f.render_widget(Paragraph::new(net_line), net_a);
        f.render_widget(
            Paragraph::new(match n.shards {
                Some(s) => format!("{s} shards · cap {}", human(n.capacity)),
                None => format!("cap {}", human(n.capacity)),
            })
            .style(Style::new().add_modifier(Modifier::DIM)),
            shards_a,
        );

        let ratio = match n.used {
            Some(u) if n.capacity > 0 => (u as f64 / n.capacity as f64).min(1.0),
            _ => 0.0,
        };
        let color = if ratio > 0.85 {
            Color::Red
        } else if ratio > 0.6 {
            Color::Yellow
        } else {
            Color::Cyan
        };
        let [_, g] =
            Layout::horizontal([Constraint::Length(2), Constraint::Min(10)]).areas(gauge_row);
        f.render_widget(
            Gauge::default()
                .gauge_style(Style::new().fg(color).bg(Color::DarkGray))
                .ratio(ratio)
                .label(format!("{:.0}% of capacity", ratio * 100.0)),
            g,
        );
    }
}

fn draw_files(f: &mut Frame, app: &App, area: Rect) {
    let filter = app.files_filter.to_lowercase();
    let visible: Vec<&ApiFile> = app
        .files
        .iter()
        .filter(|x| {
            filter.is_empty()
                || x.name
                    .as_deref()
                    .unwrap_or("")
                    .to_lowercase()
                    .contains(&filter)
                || x.hash.starts_with(&filter)
        })
        .collect();
    let rows: Vec<Row> = visible
        .iter()
        .skip(app.files_scroll)
        .take(area.height.saturating_sub(2) as usize)
        .map(|x| {
            Row::new(vec![
                Cell::from(x.hash.chars().take(16).collect::<String>())
                    .style(Style::new().add_modifier(Modifier::DIM)),
                Cell::from(human(x.size)),
                Cell::from(x.name.clone().unwrap_or_else(|| "—".into())),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(17),
            Constraint::Length(11),
            Constraint::Min(20),
        ],
    )
    .header(Row::new(vec!["HASH", "SIZE", "NAME"]).style(Style::new().bold().fg(Color::Cyan)))
    .block(Block::default().title(format!(
        " registry — {} file(s){} ",
        visible.len(),
        if filter.is_empty() {
            String::new()
        } else {
            format!(" matching “{}”", app.files_filter)
        }
    )));
    f.render_widget(table, area);
}

pub async fn run(api: String, interval_secs: u64, can_admin: bool) -> Result<()> {
    if !std::io::stdout().is_terminal() {
        bail!("`nauka top` is a full-screen terminal view — run it in a tty (or use `nauka status --json` for scripts)");
    }
    let mut app = App {
        seed_api: api,
        interval: Duration::from_secs(interval_secs.max(1)),
        paused: false,
        tab: Tab::Nodes,
        sort: Sort::Used,
        nodes: HashMap::new(),
        order: Vec::new(),
        leader: None,
        files_count: 0,
        logical_bytes: 0,
        files: Vec::new(),
        files_filter: String::new(),
        files_scroll: 0,
        quiet_since: None,
        seed_error: None,
        started: Instant::now(),
        can_admin,
        selected: 0,
        focus: Focus::Normal,
        action_msg: None,
    };
    poll(&mut app).await;
    poll_files(&mut app).await;

    // The terminal must come back whole even if we panic mid-frame.
    let mut terminal = ratatui::init();
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        ratatui::restore();
        prev_hook(info);
    }));

    let mut last_poll = Instant::now();
    let mut ticks: u64 = 0;
    let res = loop {
        if let Err(e) = terminal.draw(|f| draw(f, &app)) {
            break Err(e.into());
        }
        // Key handling with a short poll so redraws stay snappy.
        if crossterm::event::poll(Duration::from_millis(200))? {
            if let Event::Key(k) = crossterm::event::read()? {
                if k.kind != KeyEventKind::Press {
                    continue;
                }
                // Ctrl-C always quits, whatever the focus.
                if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) {
                    break Ok(());
                }
                // Any keypress clears a stale action result.
                app.action_msg = None;

                match &app.focus {
                    // ── The per-node action menu ──
                    Focus::Menu => {
                        let addrs = app.sorted_addrs();
                        let addr = addrs.get(app.selected).cloned();
                        match k.code {
                            KeyCode::Esc | KeyCode::Char('q') => app.focus = Focus::Normal,
                            KeyCode::Char(c) => {
                                if let Some(addr) = addr {
                                    if let Some(kind) =
                                        app.actions_for(&addr).into_iter().find(|a| a.key() == c)
                                    {
                                        if !app.can_admin {
                                            app.focus = Focus::Normal;
                                            app.action_msg = Some((
                                                "read-only: admin needs the cluster identity \
                                                 (run on a member, or set NAUKA_TOKEN)"
                                                    .into(),
                                                true,
                                            ));
                                        } else {
                                            let node_id = app.nodes.get(&addr).and_then(|n| n.id);
                                            app.focus = Focus::Confirm(PendingAction {
                                                kind,
                                                addr,
                                                node_id,
                                            });
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }

                    // ── Confirmation of a chosen action ──
                    Focus::Confirm(p) => match k.code {
                        KeyCode::Char('y') => {
                            let peers = app.peers();
                            let (kind, addr, node_id) = (p.kind, p.addr.clone(), p.node_id);
                            app.focus = Focus::Normal;
                            let result = match kind {
                                Action::Disable => apply_disabled(&peers, &addr, true)
                                    .await
                                    .map(|_| format!("{addr} is draining — watch it empty here")),
                                Action::Enable => apply_disabled(&peers, &addr, false)
                                    .await
                                    .map(|_| format!("{addr} back in the placement view")),
                                Action::Remove => match node_id {
                                    Some(id) => apply_remove(&peers, id)
                                        .await
                                        .map(|_| format!("{addr} removed from the cluster")),
                                    None => Err("that node has no id yet".into()),
                                },
                            };
                            app.action_msg = Some(match result {
                                Ok(msg) => (msg, false),
                                Err(e) => (e, true),
                            });
                            poll(&mut app).await; // reflect the change at once
                            last_poll = Instant::now();
                        }
                        KeyCode::Char('n') | KeyCode::Esc | KeyCode::Char('q') => {
                            app.focus = Focus::Normal;
                        }
                        _ => {}
                    },

                    // ── Normal navigation ──
                    Focus::Normal => match (app.tab, k.code) {
                        (_, KeyCode::Char('q')) => break Ok(()),
                        (_, KeyCode::Char('1')) => app.tab = Tab::Nodes,
                        (_, KeyCode::Char('2')) => {
                            app.tab = Tab::Files;
                            poll_files(&mut app).await;
                        }
                        (Tab::Nodes, KeyCode::Up | KeyCode::Char('k')) => {
                            app.selected = app.selected.saturating_sub(1);
                        }
                        (Tab::Nodes, KeyCode::Down | KeyCode::Char('j')) => {
                            let n = app.order.len().saturating_sub(1);
                            app.selected = (app.selected + 1).min(n);
                        }
                        (Tab::Nodes, KeyCode::Enter | KeyCode::Char('a')) => {
                            if !app.order.is_empty() {
                                app.focus = Focus::Menu;
                            }
                        }
                        (Tab::Nodes, KeyCode::Char('s')) => app.sort = app.sort.next(),
                        (Tab::Nodes, KeyCode::Char('p')) => app.paused = !app.paused,
                        (Tab::Nodes, KeyCode::Char('+')) => {
                            app.interval = (app.interval + Duration::from_secs(1))
                                .min(Duration::from_secs(60));
                        }
                        (Tab::Nodes, KeyCode::Char('-')) => {
                            app.interval = app
                                .interval
                                .saturating_sub(Duration::from_secs(1))
                                .max(Duration::from_secs(1));
                        }
                        (Tab::Files, KeyCode::Esc) => app.tab = Tab::Nodes,
                        (Tab::Files, KeyCode::Backspace) => {
                            app.files_filter.pop();
                            app.files_scroll = 0;
                        }
                        (Tab::Files, KeyCode::Up) => {
                            app.files_scroll = app.files_scroll.saturating_sub(1);
                        }
                        (Tab::Files, KeyCode::Down) => {
                            app.files_scroll = (app.files_scroll + 1).min(app.files.len());
                        }
                        (Tab::Files, KeyCode::Char(c)) => {
                            app.files_filter.push(c);
                            app.files_scroll = 0;
                        }
                        _ => {}
                    },
                }
            }
        }
        // Keep the selection within the current node count.
        if !app.order.is_empty() {
            app.selected = app.selected.min(app.order.len() - 1);
        }
        if !app.paused && last_poll.elapsed() >= app.interval {
            poll(&mut app).await;
            last_poll = Instant::now();
            ticks += 1;
            // The registry moves slowly; refreshing it every 10 ticks is
            // plenty and keeps the hot loop to one request per member.
            if app.tab == Tab::Files || ticks.is_multiple_of(10) {
                poll_files(&mut app).await;
            }
        }
    };
    ratatui::restore();
    res
}
