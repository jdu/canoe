//! `ratatui` rendering — pure function of `App`'s state, no I/O here.

use crate::app::{App, Screen};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};
use ratatui::Frame;

const SELECTED_STYLE: Style = Style::new().add_modifier(Modifier::REVERSED);

pub fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let chunks = Layout::default().direction(Direction::Vertical).constraints([Constraint::Length(3), Constraint::Min(0)]).split(area);

    draw_banner(frame, chunks[0], app);
    match app.screen {
        Screen::List => draw_list(frame, chunks[1], app),
        Screen::PipelineDetail(idx) => draw_detail(frame, chunks[1], app, idx),
        Screen::Projects => draw_projects(frame, chunks[1], app),
    }
}

fn draw_banner(frame: &mut Frame, area: Rect, app: &App) {
    let poll_line = match (&app.last_poll_at, &app.last_poll_error) {
        (_, Some(err)) => Line::from(vec![
            Span::raw(format!("coordinator: {}  ", app.coordinator_url)),
            Span::styled(format!("last poll failed: {err}"), Style::default().fg(Color::Red)),
        ]),
        (Some(t), None) => Line::from(format!("coordinator: {}  last updated: {}", app.coordinator_url, t.format("%H:%M:%S"))),
        (None, None) => Line::from(format!("coordinator: {}  waiting for first poll...", app.coordinator_url)),
    };
    let help = match (&app.projects_input, app.screen) {
        (crate::app::ProjectsInput::None, Screen::List) => {
            "↑/↓ or j/k: select   enter: pipeline detail   s: start   c/r: cancel/re-run task   p: projects   q: quit"
        }
        (crate::app::ProjectsInput::None, Screen::PipelineDetail(_)) => {
            "↑/↓ or j/k: select   c/r: cancel/re-run step   esc/backspace: back   q: quit"
        }
        (crate::app::ProjectsInput::None, Screen::Projects) => {
            "↑/↓ or j/k: select   a: add project   u: push selected   t: trigger   esc: back   q: quit"
        }
        (_, _) => "type, enter: confirm   esc: cancel",
    };
    let block = Block::default().borders(Borders::ALL).title("canoe-tui");
    frame.render_widget(Paragraph::new(vec![poll_line, Line::from(help)]).block(block), area);
}

fn draw_list(frame: &mut Frame, area: Rect, app: &App) {
    let Some(status) = &app.status else {
        frame.render_widget(Paragraph::new("no data yet"), area);
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(30), Constraint::Percentage(25)])
        .split(area);

    let pipeline_rows: Vec<Row> = status
        .pipelines
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let step_count: usize = p.chains.iter().map(|c| c.steps.len()).sum();
            let circuits_open = p.chains.iter().flat_map(|c| &c.steps).filter(|s| s.circuit_open).count();
            let row = Row::new(vec![p.pipeline.clone(), p.chains.len().to_string(), step_count.to_string(), circuits_open.to_string()]);
            if i == app.list_selected {
                row.style(SELECTED_STYLE)
            } else {
                row
            }
        })
        .collect();
    let pipeline_table = Table::new(pipeline_rows, [Constraint::Length(20), Constraint::Length(8), Constraint::Length(8), Constraint::Length(14)])
        .header(Row::new(vec!["pipeline", "chains", "steps", "circuits open"]).style(Style::default().add_modifier(Modifier::BOLD)))
        .block(Block::default().borders(Borders::ALL).title("Pipelines"));
    frame.render_widget(pipeline_table, chunks[0]);

    let offset = status.pipelines.len();
    let adapter_rows: Vec<Row> = status
        .adapters
        .iter()
        .enumerate()
        .map(|(i, a)| {
            let last_run = a.last_run_at.map(|t| t.format("%H:%M:%S").to_string()).unwrap_or_else(|| "never".to_string());
            let last_result = match (a.last_success, &a.last_error) {
                (Some(true), _) => "ok".to_string(),
                (Some(false), Some(err)) => format!("failed: {err}"),
                (Some(false), None) => "failed".to_string(),
                (None, _) => "-".to_string(),
            };
            let next_run = a.next_run_at.map(|t| t.format("%H:%M:%S").to_string()).unwrap_or_else(|| "-".to_string());
            let row = Row::new(vec![a.name.clone(), last_run, last_result, next_run]);
            if offset + i == app.list_selected {
                row.style(SELECTED_STYLE)
            } else {
                row
            }
        })
        .collect();
    let adapter_table = Table::new(adapter_rows, [Constraint::Length(20), Constraint::Length(12), Constraint::Length(10), Constraint::Length(12)])
        .header(Row::new(vec!["adapter", "last run", "result", "next run"]).style(Style::default().add_modifier(Modifier::BOLD)))
        .block(Block::default().borders(Borders::ALL).title("Adapters"));
    frame.render_widget(adapter_table, chunks[1]);

    // Tasks: surfaced for the first time by lifecycle control — the
    // coordinator reported nothing about them before (see
    // `crates/canoe-coordinator/src/tasks.rs`'s module docs). Selected
    // via the same flat List-screen index space, past pipelines and
    // adapters — `App::list_selected_task` is what resolves a selection
    // in this range back to `(project, task)` for the `c`/`r` key
    // bindings.
    let task_offset = offset + status.adapters.len();
    let task_rows: Vec<Row> = status
        .tasks
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let state = format!("{:?}", t.state);
            let last_run = t.last_run_at.map(|ts| ts.format("%H:%M:%S").to_string()).unwrap_or_else(|| "never".to_string());
            let row = Row::new(vec![t.name.clone(), state, last_run, t.last_error.clone().unwrap_or_default()]);
            if task_offset + i == app.list_selected {
                row.style(SELECTED_STYLE)
            } else {
                row
            }
        })
        .collect();
    let task_table = Table::new(task_rows, [Constraint::Length(20), Constraint::Length(10), Constraint::Length(12), Constraint::Min(10)])
        .header(Row::new(vec!["task", "state", "last run", "last error"]).style(Style::default().add_modifier(Modifier::BOLD)))
        .block(Block::default().borders(Borders::ALL).title("Tasks"));
    frame.render_widget(task_table, chunks[2]);
}

fn draw_detail(frame: &mut Frame, area: Rect, app: &App, pipeline_idx: usize) {
    let Some(status) = &app.status else {
        frame.render_widget(Paragraph::new("no data yet"), area);
        return;
    };
    let Some(pipeline) = status.pipelines.get(pipeline_idx) else {
        frame.render_widget(Paragraph::new("pipeline no longer present"), area);
        return;
    };

    let mut idx = 0usize;
    let mut rows = Vec::new();
    for chain in &pipeline.chains {
        for step in &chain.steps {
            let replicas = match (step.alive_replicas, step.desired_replicas) {
                (Some(a), Some(d)) => format!("{a}/{d}"),
                _ => "-".to_string(),
            };
            let backlog_lag = step.backlog.or(step.lag).map(|v| v.to_string()).unwrap_or_else(|| "-".to_string());
            let circuit = if step.circuit_open { "OPEN" } else { "-" };
            let paused = if step.paused { "PAUSED" } else { "-" };
            let row = Row::new(vec![
                chain.chain.clone(),
                step.name.clone(),
                step.kind.clone(),
                replicas,
                backlog_lag,
                circuit.to_string(),
                paused.to_string(),
                step.last_error.clone().unwrap_or_default(),
            ]);
            rows.push(if idx == app.detail_selected { row.style(SELECTED_STYLE) } else { row });
            idx += 1;
        }
    }

    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(16),
            Constraint::Length(16),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Min(10),
        ],
    )
    .header(
        Row::new(vec!["chain", "step", "kind", "replicas", "backlog/lag", "circuit", "paused", "last error"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::default().borders(Borders::ALL).title(format!("Pipeline: {}", pipeline.pipeline)));
    frame.render_widget(table, area);
}

fn draw_projects(frame: &mut Frame, area: Rect, app: &App) {
    use crate::app::ProjectsInput;

    let chunks = Layout::default().direction(Direction::Vertical).constraints([Constraint::Min(0), Constraint::Length(3)]).split(area);

    let rows: Vec<Row> = app
        .local_projects
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let dev = if p.is_dev { "dev" } else { "" };
            let feedback = app.project_feedback.get(&p.name).cloned().unwrap_or_default();
            let row = Row::new(vec![p.name.clone(), p.hcl_path.display().to_string(), dev.to_string(), feedback]);
            if i == app.projects_selected {
                row.style(SELECTED_STYLE)
            } else {
                row
            }
        })
        .collect();
    let table = Table::new(
        rows,
        [Constraint::Length(20), Constraint::Length(30), Constraint::Length(6), Constraint::Min(20)],
    )
    .header(Row::new(vec!["project", "hcl path", "dev", "last result"]).style(Style::default().add_modifier(Modifier::BOLD)))
    .block(Block::default().borders(Borders::ALL).title("Projects (local registry)"));
    frame.render_widget(table, chunks[0]);

    let input_line = match &app.projects_input {
        ProjectsInput::None => Line::from("press 'a' to register a new project"),
        ProjectsInput::AddName(buf) => Line::from(format!("project name: {buf}_")),
        ProjectsInput::AddPath { name, buf } => Line::from(format!("[{name}] .hcl path: {buf}_")),
        ProjectsInput::Trigger { project, buf } => {
            Line::from(format!("[{project}] trigger (adapter:<name> or pipeline:<name>): {buf}_"))
        }
    };
    frame.render_widget(Paragraph::new(input_line).block(Block::default().borders(Borders::ALL).title("Input")), chunks[1]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use crate::client::{AdapterStatusDto, ChainStatus, PipelineStatus, StatusResponse, StepStatusDto};
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn fixture() -> StatusResponse {
        StatusResponse {
            pipelines: vec![PipelineStatus {
                pipeline: "orders".to_string(),
                chains: vec![ChainStatus {
                    chain: "orders-0".to_string(),
                    source: "orders-adapter".to_string(),
                    steps: vec![StepStatusDto {
                        pipeline: "orders".to_string(),
                        chain: "orders-0".to_string(),
                        name: "enrich".to_string(),
                        kind: "python".to_string(),
                        desired_replicas: Some(2),
                        alive_replicas: Some(2),
                        circuit_open: false,
                        backlog: Some(3),
                        lag: None,
                        last_reconciled: chrono::Utc::now(),
                        last_error: None,
                        paused: false,
                    }],
                }],
            }],
            adapters: vec![AdapterStatusDto {
                name: "orders-adapter".to_string(),
                last_run_at: None,
                last_success: None,
                last_error: None,
                next_run_at: None,
            }],
            tasks: vec![],
        }
    }

    #[test]
    fn list_screen_renders_pipeline_and_adapter_names() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));

        terminal.draw(|f| draw(f, &app)).unwrap();
        let content = buffer_to_string(terminal.backend().buffer());
        assert!(content.contains("orders"), "pipeline name should be rendered");
        assert!(content.contains("orders-adapter"), "adapter name should be rendered");
    }

    #[test]
    fn detail_screen_renders_step_row() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.enter();

        terminal.draw(|f| draw(f, &app)).unwrap();
        let content = buffer_to_string(terminal.backend().buffer());
        assert!(content.contains("enrich"), "step name should be rendered");
        assert!(content.contains("2/2"), "alive/desired replica count should be rendered");
    }

    fn buffer_to_string(buffer: &ratatui::buffer::Buffer) -> String {
        let area = buffer.area;
        let mut out = String::new();
        for y in 0..area.height {
            for x in 0..area.width {
                out.push_str(buffer[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }
}
