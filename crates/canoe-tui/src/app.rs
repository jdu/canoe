//! Pure application state and transitions — no terminal/rendering here,
//! so this is unit-testable without a `ratatui` backend. `ui.rs` reads
//! this to draw; `main.rs`'s event loop mutates it (and performs the
//! actual I/O — HTTP pushes/triggers, local project-file persistence —
//! that a state transition here only *requests*, never performs itself).

use crate::client::StatusResponse;
use crate::projects::LocalProject;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    List,
    /// Index into `App::status.pipelines` of the pipeline being drilled into.
    PipelineDetail(usize),
    /// Locally-registered projects: add/push/trigger — see
    /// `ProjectsInput` for the inline text-capture sub-states.
    Projects,
}

/// `ratatui` has no built-in text-input widget — this is the small
/// inline text-capture mode `Screen::Projects` needs for "add a project"
/// (name, then path) and "trigger" (kind:name), one character at a time
/// via plain `KeyCode::Char`/`Backspace`/`Enter` handling in `main.rs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectsInput {
    None,
    AddName(String),
    AddPath { name: String, buf: String },
    /// Typed as `adapter:<name>` or `pipeline:<name>` — see
    /// `confirm_trigger`'s docs for why one buffer covers both instead of
    /// a separate kind-selection step.
    Trigger { project: String, buf: String },
}

/// What actually firing a trigger from `confirm_trigger` means — parsed
/// out of the typed buffer, for `main.rs` to dispatch to
/// `client::trigger_adapter`/`client::trigger_pipeline`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerRequest {
    Adapter { project: String, name: String },
    Pipeline { project: String, name: String },
}

pub struct App {
    pub coordinator_url: String,
    pub status: Option<StatusResponse>,
    pub last_poll_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_poll_error: Option<String>,
    pub screen: Screen,
    /// Selected row on the list screen — pipelines table while `< pipeline
    /// count`, otherwise the adapters table at `selected - pipeline count`.
    pub list_selected: usize,
    pub detail_selected: usize,
    pub should_quit: bool,

    /// Locally-registered projects (persisted to disk by `main.rs` via
    /// `crate::projects::save` after every mutation — this struct itself
    /// does no file I/O).
    pub local_projects: Vec<LocalProject>,
    pub projects_selected: usize,
    pub projects_input: ProjectsInput,
    /// Last push/trigger result message per project name — session-only
    /// (not persisted), the last two pieces of info `App::new`'s docs
    /// promise are "only meaningful after at least one successful push
    /// this session."
    pub project_feedback: HashMap<String, String>,
}

impl App {
    pub fn new(coordinator_url: String) -> Self {
        Self {
            coordinator_url,
            status: None,
            last_poll_at: None,
            last_poll_error: None,
            screen: Screen::List,
            list_selected: 0,
            detail_selected: 0,
            should_quit: false,
            local_projects: Vec::new(),
            projects_selected: 0,
            projects_input: ProjectsInput::None,
            project_feedback: HashMap::new(),
        }
    }

    pub fn with_local_projects(mut self, projects: Vec<LocalProject>) -> Self {
        self.local_projects = projects;
        self
    }

    pub fn apply_poll(&mut self, result: anyhow::Result<StatusResponse>) {
        match result {
            Ok(status) => {
                self.status = Some(status);
                self.last_poll_at = Some(chrono::Utc::now());
                self.last_poll_error = None;
            }
            Err(e) => self.last_poll_error = Some(e.to_string()),
        }
    }

    fn list_row_count(&self) -> usize {
        self.status.as_ref().map(|s| s.pipelines.len() + s.adapters.len() + s.tasks.len()).unwrap_or(0)
    }

    /// "Start" prominence: the trigger action for whichever pipeline/
    /// adapter row is currently selected on the List screen — `None` for
    /// a task row (tasks aren't triggered this way, only cancelled/
    /// re-run) or when nothing's selected yet. Reuses `TriggerRequest`
    /// verbatim (same type `Screen::Projects`' trigger input already
    /// produces) rather than a separate type, since it's exactly the
    /// same action either way — just a different way of picking the
    /// target.
    pub fn list_selected_trigger(&self) -> Option<TriggerRequest> {
        if self.screen != Screen::List {
            return None;
        }
        let status = self.status.as_ref()?;
        if self.list_selected < status.pipelines.len() {
            let full = &status.pipelines[self.list_selected].pipeline;
            let (project, name) = crate::client::split_project_prefix(full)?;
            return Some(TriggerRequest::Pipeline { project: project.to_string(), name: name.to_string() });
        }
        let adapter_idx = self.list_selected - status.pipelines.len();
        if adapter_idx < status.adapters.len() {
            let full = &status.adapters[adapter_idx].name;
            let (project, name) = crate::client::split_project_prefix(full)?;
            return Some(TriggerRequest::Adapter { project: project.to_string(), name: name.to_string() });
        }
        None
    }

    /// The `(project, bare_task_name)` for whichever task row is
    /// currently selected on the List screen — `None` if the selection
    /// isn't on a task row (or nothing's selected).
    pub fn list_selected_task(&self) -> Option<(String, String)> {
        if self.screen != Screen::List {
            return None;
        }
        let status = self.status.as_ref()?;
        let offset = status.pipelines.len() + status.adapters.len();
        let task = status.tasks.get(self.list_selected.checked_sub(offset)?)?;
        let (project, name) = crate::client::split_project_prefix(&task.name)?;
        Some((project.to_string(), name.to_string()))
    }

    /// The `(project, bare_step_name)` for whichever step row is
    /// currently selected on the `PipelineDetail` screen — for cancel/
    /// re-run key bindings there.
    pub fn selected_detail_step(&self) -> Option<(String, String)> {
        let Screen::PipelineDetail(idx) = self.screen else { return None };
        let status = self.status.as_ref()?;
        let pipeline = status.pipelines.get(idx)?;
        let mut i = 0usize;
        for chain in &pipeline.chains {
            for step in &chain.steps {
                if i == self.detail_selected {
                    let (project, name) = crate::client::split_project_prefix(&step.name)?;
                    return Some((project.to_string(), name.to_string()));
                }
                i += 1;
            }
        }
        None
    }

    pub fn select_next(&mut self) {
        // Text-capture mode owns Up/Down-adjacent input while active in
        // the real key handler (`main.rs`) — this method is only ever
        // called for row navigation, so it's safe to just check the
        // screen here without also checking `projects_input`.
        match self.screen {
            Screen::List => {
                let n = self.list_row_count();
                if n > 0 {
                    self.list_selected = (self.list_selected + 1) % n;
                }
            }
            Screen::PipelineDetail(idx) => {
                if let Some(n) = self.detail_row_count(idx) {
                    if n > 0 {
                        self.detail_selected = (self.detail_selected + 1) % n;
                    }
                }
            }
            Screen::Projects => {
                let n = self.local_projects.len();
                if n > 0 {
                    self.projects_selected = (self.projects_selected + 1) % n;
                }
            }
        }
    }

    pub fn select_prev(&mut self) {
        match self.screen {
            Screen::List => {
                let n = self.list_row_count();
                if n > 0 {
                    self.list_selected = (self.list_selected + n - 1) % n;
                }
            }
            Screen::PipelineDetail(idx) => {
                if let Some(n) = self.detail_row_count(idx) {
                    if n > 0 {
                        self.detail_selected = (self.detail_selected + n - 1) % n;
                    }
                }
            }
            Screen::Projects => {
                let n = self.local_projects.len();
                if n > 0 {
                    self.projects_selected = (self.projects_selected + n - 1) % n;
                }
            }
        }
    }

    fn detail_row_count(&self, pipeline_idx: usize) -> Option<usize> {
        self.status.as_ref()?.pipelines.get(pipeline_idx).map(|p| p.chains.iter().map(|c| c.steps.len()).sum())
    }

    /// Enter the detail screen for the currently selected pipeline row —
    /// a no-op if the current selection is on an adapter row (adapters
    /// have no detail screen) or nothing is selected yet.
    pub fn enter(&mut self) {
        if self.screen != Screen::List {
            return;
        }
        let Some(status) = &self.status else { return };
        if self.list_selected < status.pipelines.len() {
            self.screen = Screen::PipelineDetail(self.list_selected);
            self.detail_selected = 0;
        }
    }

    pub fn back(&mut self) {
        if self.projects_input != ProjectsInput::None {
            self.projects_input = ProjectsInput::None;
            return;
        }
        if self.screen != Screen::List {
            self.screen = Screen::List;
        }
    }

    /// Switches to the Projects screen — a no-op on selection/input state
    /// beyond that, same as `enter`'s screen transitions elsewhere.
    pub fn open_projects(&mut self) {
        self.screen = Screen::Projects;
    }

    /// Starts the "add a project" text-capture flow — name first, then
    /// (via `advance_add_input`) the `.hcl` path.
    pub fn start_add_project(&mut self) {
        if self.screen == Screen::Projects {
            self.projects_input = ProjectsInput::AddName(String::new());
        }
    }

    /// Starts the trigger text-capture flow for the currently selected
    /// local project.
    pub fn start_trigger(&mut self) {
        if self.screen != Screen::Projects {
            return;
        }
        if let Some(p) = self.local_projects.get(self.projects_selected) {
            self.projects_input = ProjectsInput::Trigger { project: p.name.clone(), buf: String::new() };
        }
    }

    /// Appends one character to whichever buffer the current
    /// `projects_input` state owns — a no-op if no input mode is active.
    pub fn input_char(&mut self, c: char) {
        match &mut self.projects_input {
            ProjectsInput::None => {}
            ProjectsInput::AddName(buf) => buf.push(c),
            ProjectsInput::AddPath { buf, .. } => buf.push(c),
            ProjectsInput::Trigger { buf, .. } => buf.push(c),
        }
    }

    pub fn input_backspace(&mut self) {
        match &mut self.projects_input {
            ProjectsInput::None => {}
            ProjectsInput::AddName(buf) => {
                buf.pop();
            }
            ProjectsInput::AddPath { buf, .. } => {
                buf.pop();
            }
            ProjectsInput::Trigger { buf, .. } => {
                buf.pop();
            }
        }
    }

    /// Advances (or, for the final step, commits) the current text-input
    /// flow. `AddName` -> `AddPath` (name captured, now typing the
    /// `.hcl` path). `AddPath` -> commits a new `LocalProject` (pushed
    /// onto `local_projects`, `projects_input` reset to `None`) and
    /// returns it so `main.rs` can persist the updated list to disk
    /// (`crate::projects::save`) — this method itself does no file I/O.
    /// `Trigger`/`None` are handled by `confirm_trigger` instead; calling
    /// this while either is active is a no-op.
    pub fn confirm_add_input(&mut self) -> Option<LocalProject> {
        match std::mem::replace(&mut self.projects_input, ProjectsInput::None) {
            ProjectsInput::AddName(name) if !name.trim().is_empty() => {
                self.projects_input = ProjectsInput::AddPath { name: name.trim().to_string(), buf: String::new() };
                None
            }
            ProjectsInput::AddPath { name, buf } if !buf.trim().is_empty() => {
                let project = LocalProject { name, hcl_path: PathBuf::from(buf.trim()), is_dev: false };
                self.local_projects.push(project.clone());
                self.projects_selected = self.local_projects.len() - 1;
                Some(project)
            }
            other => {
                // Empty name/path typed then confirmed — put the state
                // back rather than silently discarding what was typed.
                self.projects_input = other;
                None
            }
        }
    }

    /// Parses the current `Trigger` buffer (`"adapter:<name>"` or
    /// `"pipeline:<name>"` — one buffer covers both kinds rather than a
    /// separate kind-selection step first, since typing the four-or-eight
    /// character prefix is no heavier than a second keypress-driven menu
    /// would be for this TUI's existing input style) and, if well-formed,
    /// resets `projects_input` to `None` and returns the parsed request
    /// for `main.rs` to actually fire. Malformed input (no recognized
    /// prefix, or empty name) leaves the buffer as-is so the user can
    /// correct it rather than silently discarding what they typed.
    pub fn confirm_trigger(&mut self) -> Option<TriggerRequest> {
        let ProjectsInput::Trigger { project, buf } = &self.projects_input else { return None };
        let (project, buf) = (project.clone(), buf.clone());
        let parsed = if let Some(name) = buf.strip_prefix("adapter:") {
            (!name.is_empty()).then(|| TriggerRequest::Adapter { project: project.clone(), name: name.to_string() })
        } else if let Some(name) = buf.strip_prefix("pipeline:") {
            (!name.is_empty()).then(|| TriggerRequest::Pipeline { project: project.clone(), name: name.to_string() })
        } else {
            None
        };
        if parsed.is_some() {
            self.projects_input = ProjectsInput::None;
        }
        parsed
    }

    /// The currently-selected local project, for `main.rs` to kick off an
    /// async `client::push_project` call against.
    pub fn selected_project(&self) -> Option<&LocalProject> {
        self.local_projects.get(self.projects_selected)
    }

    /// Records a push/trigger result message for display — called by
    /// `main.rs` once the corresponding async HTTP call resolves.
    pub fn apply_project_feedback(&mut self, project: &str, message: String) {
        self.project_feedback.insert(project.to_string(), message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{AdapterStatusDto, ChainStatus, PipelineStatus, StepStatusDto, TaskState, TaskStatusDto};

    fn step(name: &str) -> StepStatusDto {
        StepStatusDto {
            pipeline: "orders".to_string(),
            chain: "orders-0".to_string(),
            name: name.to_string(),
            kind: "python".to_string(),
            desired_replicas: Some(1),
            alive_replicas: Some(1),
            circuit_open: false,
            backlog: Some(0),
            lag: None,
            last_reconciled: chrono::Utc::now(),
            last_error: None,
            paused: false,
        }
    }

    fn fixture() -> StatusResponse {
        StatusResponse {
            pipelines: vec![
                PipelineStatus {
                    pipeline: "default--orders".to_string(),
                    chains: vec![ChainStatus {
                        chain: "default--orders-0".to_string(),
                        source: "orders-adapter".to_string(),
                        steps: vec![step("default--enrich"), step("default--dedupe")],
                    }],
                },
                PipelineStatus { pipeline: "default--billing".to_string(), chains: vec![] },
            ],
            adapters: vec![AdapterStatusDto {
                name: "default--orders-adapter".to_string(),
                last_run_at: None,
                last_success: None,
                last_error: None,
                next_run_at: None,
            }],
            tasks: vec![TaskStatusDto {
                name: "default--notify".to_string(),
                state: TaskState::Pending,
                last_run_at: None,
                last_error: None,
            }],
        }
    }

    #[test]
    fn list_selection_wraps_around_over_pipelines_and_adapters() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        // 2 pipelines + 1 adapter + 1 task = 4 rows.
        assert_eq!(app.list_selected, 0);
        app.select_prev();
        assert_eq!(app.list_selected, 3, "wraps backward from 0 to the last row");
        app.select_next();
        assert_eq!(app.list_selected, 0, "wraps forward from the last row back to 0");
    }

    #[test]
    fn enter_on_a_pipeline_row_opens_its_detail_screen() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.list_selected = 1; // "billing"
        app.enter();
        assert_eq!(app.screen, Screen::PipelineDetail(1));
    }

    #[test]
    fn enter_on_an_adapter_row_is_a_no_op() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.list_selected = 2; // the one adapter row
        app.enter();
        assert_eq!(app.screen, Screen::List, "adapters have no detail screen to drill into");
    }

    #[test]
    fn back_returns_to_the_list_screen() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.screen = Screen::PipelineDetail(0);
        app.back();
        assert_eq!(app.screen, Screen::List);
    }

    #[test]
    fn detail_selection_wraps_over_that_pipelines_steps() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.list_selected = 0;
        app.enter();
        // "orders" has 2 steps across its one chain.
        assert_eq!(app.detail_selected, 0);
        app.select_prev();
        assert_eq!(app.detail_selected, 1);
    }

    #[test]
    fn a_failed_poll_keeps_the_last_known_good_snapshot() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        assert!(app.status.is_some());
        app.apply_poll(Err(anyhow::anyhow!("connection refused")));
        assert!(app.status.is_some(), "a failed poll must not blank the last-known-good snapshot");
        assert_eq!(app.last_poll_error.as_deref(), Some("connection refused"));
    }

    #[test]
    fn open_projects_switches_screen() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.open_projects();
        assert_eq!(app.screen, Screen::Projects);
    }

    #[test]
    fn adding_a_project_captures_name_then_path_then_commits() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.open_projects();
        app.start_add_project();
        assert_eq!(app.projects_input, ProjectsInput::AddName(String::new()));

        for c in "my-pipeline".chars() {
            app.input_char(c);
        }
        assert!(app.confirm_add_input().is_none(), "first confirm only advances name -> path, doesn't commit yet");
        assert_eq!(app.projects_input, ProjectsInput::AddPath { name: "my-pipeline".to_string(), buf: String::new() });

        for c in "/tmp/my-pipeline.canoe.hcl".chars() {
            app.input_char(c);
        }
        let committed = app.confirm_add_input().expect("second confirm should commit the new project");
        assert_eq!(committed.name, "my-pipeline");
        assert_eq!(committed.hcl_path, PathBuf::from("/tmp/my-pipeline.canoe.hcl"));
        assert!(!committed.is_dev);
        assert_eq!(app.local_projects.len(), 1);
        assert_eq!(app.projects_input, ProjectsInput::None, "input mode resets after a successful commit");
    }

    #[test]
    fn backspace_edits_the_current_input_buffer() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.open_projects();
        app.start_add_project();
        app.input_char('a');
        app.input_char('b');
        app.input_backspace();
        assert_eq!(app.projects_input, ProjectsInput::AddName("a".to_string()));
    }

    #[test]
    fn back_cancels_input_mode_before_leaving_the_screen() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.open_projects();
        app.start_add_project();
        app.input_char('x');
        app.back();
        assert_eq!(app.projects_input, ProjectsInput::None, "back() cancels the in-progress input first");
        assert_eq!(app.screen, Screen::Projects, "not yet back to List — that's a second back()");
        app.back();
        assert_eq!(app.screen, Screen::List);
    }

    fn project(name: &str) -> LocalProject {
        LocalProject { name: name.to_string(), hcl_path: PathBuf::from(format!("/tmp/{name}.hcl")), is_dev: false }
    }

    #[test]
    fn projects_screen_selection_wraps() {
        let mut app = App::new("http://localhost:9095".to_string()).with_local_projects(vec![project("a"), project("b"), project("c")]);
        app.open_projects();
        assert_eq!(app.projects_selected, 0);
        app.select_prev();
        assert_eq!(app.projects_selected, 2, "wraps backward from 0 to the last row");
        app.select_next();
        assert_eq!(app.projects_selected, 0, "wraps forward from the last row back to 0");
    }

    #[test]
    fn selected_project_tracks_the_current_selection() {
        let mut app = App::new("http://localhost:9095".to_string()).with_local_projects(vec![project("a"), project("b")]);
        app.open_projects();
        app.select_next();
        assert_eq!(app.selected_project().map(|p| p.name.as_str()), Some("b"));
    }

    #[test]
    fn trigger_input_parses_adapter_and_pipeline_prefixes() {
        let mut app = App::new("http://localhost:9095".to_string()).with_local_projects(vec![project("my-pipeline")]);
        app.open_projects();
        app.start_trigger();
        assert_eq!(app.projects_input, ProjectsInput::Trigger { project: "my-pipeline".to_string(), buf: String::new() });

        for c in "adapter:orders_adapter".chars() {
            app.input_char(c);
        }
        let req = app.confirm_trigger().expect("well-formed adapter: input should parse");
        assert_eq!(req, TriggerRequest::Adapter { project: "my-pipeline".to_string(), name: "orders_adapter".to_string() });
        assert_eq!(app.projects_input, ProjectsInput::None);
    }

    #[test]
    fn trigger_input_rejects_an_unrecognized_prefix_without_clearing_the_buffer() {
        let mut app = App::new("http://localhost:9095".to_string()).with_local_projects(vec![project("my-pipeline")]);
        app.open_projects();
        app.start_trigger();
        for c in "nonsense".chars() {
            app.input_char(c);
        }
        assert!(app.confirm_trigger().is_none());
        assert_eq!(
            app.projects_input,
            ProjectsInput::Trigger { project: "my-pipeline".to_string(), buf: "nonsense".to_string() },
            "malformed input stays in the buffer so the user can correct it, rather than being silently discarded"
        );
    }

    #[test]
    fn apply_project_feedback_records_a_message_per_project() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_project_feedback("my-pipeline", "pushed".to_string());
        assert_eq!(app.project_feedback.get("my-pipeline").map(String::as_str), Some("pushed"));
    }

    #[test]
    fn list_selected_trigger_resolves_a_pipeline_row() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.list_selected = 0; // "default--orders"
        assert_eq!(
            app.list_selected_trigger(),
            Some(TriggerRequest::Pipeline { project: "default".to_string(), name: "orders".to_string() })
        );
    }

    #[test]
    fn list_selected_trigger_resolves_an_adapter_row() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.list_selected = 2; // the adapter row
        assert_eq!(
            app.list_selected_trigger(),
            Some(TriggerRequest::Adapter { project: "default".to_string(), name: "orders-adapter".to_string() })
        );
    }

    #[test]
    fn list_selected_trigger_is_none_for_a_task_row() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.list_selected = 3; // the task row
        assert!(app.list_selected_trigger().is_none());
    }

    #[test]
    fn list_selected_task_resolves_the_task_row() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.list_selected = 3; // the task row
        assert_eq!(app.list_selected_task(), Some(("default".to_string(), "notify".to_string())));
        app.list_selected = 0; // a pipeline row
        assert!(app.list_selected_task().is_none());
    }

    #[test]
    fn selected_detail_step_resolves_the_step_under_the_cursor() {
        let mut app = App::new("http://localhost:9095".to_string());
        app.apply_poll(Ok(fixture()));
        app.list_selected = 0; // "default--orders"
        app.enter();
        assert_eq!(app.detail_selected, 0);
        assert_eq!(app.selected_detail_step(), Some(("default".to_string(), "enrich".to_string())));
        app.select_next();
        assert_eq!(app.selected_detail_step(), Some(("default".to_string(), "dedupe".to_string())));
    }
}
