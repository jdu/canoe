//! canoe-tui: a read-only terminal client for a running `canoe-coordinator`.
//! Polls `GET /status` (see `crates/canoe-coordinator/src/status.rs`) on a
//! timer and renders a pipeline/adapter list, with a per-pipeline detail
//! drill-down. No write actions — this never scales a step or triggers an
//! adapter, it only displays what the coordinator already reports.

mod app;
mod client;
mod projects;
mod ui;

use app::App;
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io::stdout;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "canoe-tui", about = "Terminal status viewer for a canoe-coordinator")]
struct Cli {
    /// Base URL of the coordinator's metrics/status server, e.g.
    /// "http://localhost:9095" (see `canoe-coordinator`'s `--metrics-addr`).
    #[arg(long, env = "CANOE_COORDINATOR_URL", default_value = "http://localhost:9095")]
    coordinator_url: String,

    /// How often to poll `GET /status`.
    #[arg(long, default_value_t = 2)]
    refresh_interval_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let poll_url = cli.coordinator_url.clone();
    let poll_interval = Duration::from_secs(cli.refresh_interval_secs);
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut ticker = tokio::time::interval(poll_interval);
        loop {
            ticker.tick().await;
            let result = client::fetch_status(&client, &poll_url).await;
            if tx.send(result).is_err() {
                return; // render loop exited
            }
        }
    });

    let projects_path = projects::default_path()?;
    let local_projects = projects::load(&projects_path).unwrap_or_default();

    let mut terminal = init_terminal()?;
    let mut app = App::new(cli.coordinator_url).with_local_projects(local_projects);
    let result = run(&mut terminal, &mut app, &mut rx, &projects_path).await;
    restore_terminal()?;
    result
}

fn init_terminal() -> anyhow::Result<Terminal<CrosstermBackend<std::io::Stdout>>> {
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    Ok(Terminal::new(CrosstermBackend::new(stdout()))?)
}

fn restore_terminal() -> anyhow::Result<()> {
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}

async fn run(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<anyhow::Result<client::StatusResponse>>,
    projects_path: &std::path::Path,
) -> anyhow::Result<()> {
    let http = reqwest::Client::new();
    loop {
        while let Ok(result) = rx.try_recv() {
            app.apply_poll(result);
        }

        terminal.draw(|f| ui::draw(f, app))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    handle_key(app, key.code, &http, projects_path).await;
                }
            }
        }

        if app.should_quit {
            return Ok(());
        }
    }
}

/// Text-capture mode (`app.projects_input`) intercepts printable
/// characters/Backspace/Enter/Esc first when active — everything else
/// (quit, navigation, screen switches) only applies once no input mode
/// is active, so typing a project name doesn't also move the selection
/// or quit the app on a stray `j`/`q`.
async fn handle_key(app: &mut App, code: KeyCode, http: &reqwest::Client, projects_path: &std::path::Path) {
    if app.projects_input != app::ProjectsInput::None {
        match code {
            KeyCode::Esc => app.back(),
            KeyCode::Backspace => app.input_backspace(),
            KeyCode::Char(c) => app.input_char(c),
            KeyCode::Enter => {
                if let app::ProjectsInput::Trigger { .. } = &app.projects_input {
                    if let Some(req) = app.confirm_trigger() {
                        fire_trigger(app, http, req).await;
                    }
                } else if let Some(new_project) = app.confirm_add_input() {
                    if let Err(e) = projects::save(projects_path, &app.local_projects) {
                        app.apply_project_feedback(&new_project.name, format!("saved locally but failed to persist: {e}"));
                    }
                }
            }
            _ => {}
        }
        return;
    }

    match code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Esc | KeyCode::Backspace => match app.screen {
            app::Screen::List => app.should_quit = true,
            app::Screen::PipelineDetail(_) | app::Screen::Projects => app.back(),
        },
        KeyCode::Down | KeyCode::Char('j') => app.select_next(),
        KeyCode::Up | KeyCode::Char('k') => app.select_prev(),
        KeyCode::Enter => app.enter(),
        KeyCode::Char('p') if app.screen != app::Screen::Projects => app.open_projects(),
        KeyCode::Char('a') if app.screen == app::Screen::Projects => app.start_add_project(),
        KeyCode::Char('t') if app.screen == app::Screen::Projects => app.start_trigger(),
        KeyCode::Char('u') if app.screen == app::Screen::Projects => {
            if let Some(project) = app.selected_project().cloned() {
                match client::push_project(http, &app.coordinator_url, &project.name, &project.hcl_path, project.is_dev).await {
                    Ok(resp) => app.apply_project_feedback(&project.name, format!("pushed: {resp}")),
                    Err(e) => app.apply_project_feedback(&project.name, format!("push failed: {e}")),
                }
            }
        }
        // "Start" prominence: fire the existing trigger action directly
        // from the selected List row (an adapter or a triggered
        // pipeline) — same `trigger_adapter`/`trigger_pipeline` calls
        // `Screen::Projects`' own trigger flow uses, just reached without
        // going through that screen first.
        KeyCode::Char('s') if app.screen == app::Screen::List => {
            if let Some(req) = app.list_selected_trigger() {
                fire_trigger(app, http, req).await;
            }
        }
        // Cancel/re-run a step (PipelineDetail's selected row) or a task
        // (List's selected row, once selection is past the
        // pipelines/adapters rows) — same inline-action pattern as the
        // Projects screen's own key bindings, no new interaction
        // machinery.
        KeyCode::Char('c') if matches!(app.screen, app::Screen::PipelineDetail(_)) => {
            if let Some((project, step)) = app.selected_detail_step() {
                match client::cancel_step(http, &app.coordinator_url, &project, &step).await {
                    Ok(()) => app.apply_project_feedback(&project, format!("step \"{step}\" cancelled")),
                    Err(e) => app.apply_project_feedback(&project, format!("cancel step \"{step}\" failed: {e}")),
                }
            }
        }
        KeyCode::Char('r') if matches!(app.screen, app::Screen::PipelineDetail(_)) => {
            if let Some((project, step)) = app.selected_detail_step() {
                match client::rerun_step(http, &app.coordinator_url, &project, &step).await {
                    Ok(()) => app.apply_project_feedback(&project, format!("step \"{step}\" re-run")),
                    Err(e) => app.apply_project_feedback(&project, format!("re-run step \"{step}\" failed: {e}")),
                }
            }
        }
        KeyCode::Char('c') if app.screen == app::Screen::List => {
            if let Some((project, task)) = app.list_selected_task() {
                match client::cancel_task(http, &app.coordinator_url, &project, &task).await {
                    Ok(()) => app.apply_project_feedback(&project, format!("task \"{task}\" cancelled")),
                    Err(e) => app.apply_project_feedback(&project, format!("cancel task \"{task}\" failed: {e}")),
                }
            }
        }
        KeyCode::Char('r') if app.screen == app::Screen::List => {
            if let Some((project, task)) = app.list_selected_task() {
                match client::rerun_task(http, &app.coordinator_url, &project, &task).await {
                    Ok(()) => app.apply_project_feedback(&project, format!("task \"{task}\" re-run")),
                    Err(e) => app.apply_project_feedback(&project, format!("re-run task \"{task}\" failed: {e}")),
                }
            }
        }
        _ => {}
    }
}

async fn fire_trigger(app: &mut App, http: &reqwest::Client, req: app::TriggerRequest) {
    let (project, result) = match &req {
        app::TriggerRequest::Adapter { project, name } => {
            (project.clone(), client::trigger_adapter(http, &app.coordinator_url, project, name).await)
        }
        app::TriggerRequest::Pipeline { project, name } => {
            (project.clone(), client::trigger_pipeline(http, &app.coordinator_url, project, name).await)
        }
    };
    match result {
        Ok(()) => app.apply_project_feedback(&project, "triggered".to_string()),
        Err(e) => app.apply_project_feedback(&project, format!("trigger failed: {e}")),
    }
}
