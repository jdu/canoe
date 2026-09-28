//! Fires every `task` block's one-shot Job once its `depends_on` names
//! are all satisfied — see `canoe_config::TaskConfig`'s docs for what a
//! name may refer to and what "satisfied" means for each kind:
//!
//! - a **step**, **dynamic_route**, or **pipeline** name is satisfied as
//!   soon as the coordinator has *applied* it (`activation::mark_activated`,
//!   `dynamic_router::run`) — never once it's "done," since those run
//!   forever. This is an apply-order gate, not a readiness one: it
//!   doesn't wait for the Deployment's pods to actually come up, just
//!   that the apply call itself succeeded.
//! - a **task** name is satisfied only once that task's own one-shot Job
//!   actually completes (`AdapterJobRunner::job_status`) — a real
//!   completion dependency, so a chain of `task`s runs in the order
//!   `depends_on` describes.
//!
//! `depends_on_complete` (separate from `depends_on` above) is the same
//! real-completion gate generalized to *any* name with a "done" —
//! another task (as above) or a batch-mode step (`activation::run_batch_reconcile_loop`,
//! which calls `activation::mark_completed` on Job success) — checked
//! against the shared `Ctx.completed` registry rather than this module's
//! own task-only `completed: HashMap<String, Outcome>`. A streaming-mode
//! step or `dynamic_route` never has a "done" to report; `canoe-dsl`
//! rejects a `depends_on_complete` entry naming one at compile time.
//!
//! Polling-based throughout (mirrors `scheduler.rs`'s adapter loop and
//! `autoscaler.rs`'s reconcile loop), not event-driven — see README.md's
//! "Status / what's stubbed" for the latency and coordinator-restart
//! implications.
//!
//! **Status reporting and lifecycle control** (cancel/re-run): unlike
//! every other node kind, this loop's shape is "wait once, fire once,
//! poll once to completion" rather than a continuously-ticking reconcile
//! loop — so, to support re-run *after* a task has already
//! succeeded/failed/been cancelled, the whole wait-fire-poll sequence
//! below runs inside an outer `loop` that goes back to waiting once a run
//! ends, instead of the function returning. `ctx.status.update_task` is
//! called at every state transition (`Pending` -> `Running` ->
//! `Succeeded`/`Failed`) — this is surfacing state `tasks.rs` already
//! tracked internally via the local `completed` map, not computing
//! anything new.

use crate::activation::Ctx;
use crate::status::{TaskState, TaskStatus};
use canoe_config::TaskConfig;
use canoe_k8s::{AdapterJobRunner, JobStatus};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const DEPENDENCY_POLL_INTERVAL: Duration = Duration::from_secs(2);
const JOB_STATUS_POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Whether a task's own Job has finished, and how — shared across every
/// task-runner loop so a task depending on another task can see it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Succeeded,
    Failed,
}

/// Spawns one detached loop per `task`. Fire-and-forget, same convention
/// as `scheduler::start`/`archiver::start`/`dynamic_router::start`.
/// Registers one force-fire channel per (already project-prefixed) task
/// name into `ctx.force_fire` before spawning — same registration
/// pattern `scheduler::start` uses for `ctx.adapter_triggers`.
pub fn start(ctx: Ctx, runner_image: String, tasks: Vec<TaskConfig>) {
    let task_names: HashSet<String> = tasks.iter().map(|t| t.name.clone()).collect();
    let completed: Arc<Mutex<HashMap<String, Outcome>>> = Arc::new(Mutex::new(HashMap::new()));

    for task in tasks {
        let ctx_task = ctx.clone();
        let runner_image = runner_image.clone();
        let task_names = task_names.clone();
        let completed = completed.clone();

        let (force_tx, force_rx) = mpsc::channel::<()>(1);
        ctx.force_fire.lock().unwrap().insert(task.name.clone(), force_tx);

        ctx.status.update_task(TaskStatus { name: task.name.clone(), state: TaskState::Pending, last_run_at: None, last_error: None });

        tokio::spawn(async move {
            run(ctx_task, runner_image, task, task_names, completed, force_rx).await;
        });
    }
}

/// Waits for `depends_on`/`depends_on_complete` to be satisfied (or a
/// force-fire, which bypasses that gate entirely — "re-run implies the
/// caller already knows it should run now"), then fires the task's Job
/// and polls it to completion, reporting status throughout. Loops back
/// to waiting afterward (rather than returning) so a later re-run can
/// fire this task again even after it already finished once.
async fn run(
    ctx: Ctx,
    runner_image: String,
    task: TaskConfig,
    task_names: HashSet<String>,
    completed: Arc<Mutex<HashMap<String, Outcome>>>,
    mut force_rx: mpsc::Receiver<()>,
) {
    let (task_deps, other_deps): (Vec<String>, Vec<String>) =
        task.depends_on.iter().cloned().partition(|d| task_names.contains(d));
    let runner = AdapterJobRunner::new(ctx.k8s_client.clone(), ctx.namespace.clone(), runner_image);

    info!(task = %task.name, depends_on = ?task.depends_on, depends_on_complete = ?task.depends_on_complete, "task waiting on dependencies");

    loop {
        // --- wait for dependencies (or a force-fire) ---
        let mut forced = false;
        loop {
            if let Some(failed) = task_deps.iter().find(|d| completed.lock().unwrap().get(*d) == Some(&Outcome::Failed)) {
                error!(task = %task.name, dependency = %failed, "dependency task failed, this task will never run");
                ctx.status.update_task(TaskStatus {
                    name: task.name.clone(),
                    state: TaskState::Failed,
                    last_run_at: None,
                    last_error: Some(format!("dependency task \"{failed}\" failed")),
                });
                return;
            }

            tokio::select! {
                _ = tokio::time::sleep(DEPENDENCY_POLL_INTERVAL) => {}
                _ = force_rx.recv() => {
                    forced = true;
                    info!(task = %task.name, "force-fire trigger received");
                }
            }
            if forced {
                break;
            }

            // Pause check: a paused task never breaks out of the wait
            // loop on its own, regardless of whether its dependencies are
            // otherwise satisfied — same "check before any other gating
            // logic" placement as `run_batch_reconcile_loop`'s.
            if crate::activation::is_paused(&ctx.control, &task.name) {
                continue;
            }

            let steps_ready = other_deps.iter().all(|d| ctx.activated.lock().unwrap().contains(d));
            let tasks_ready = task_deps.iter().all(|d| completed.lock().unwrap().get(d) == Some(&Outcome::Succeeded));
            // `depends_on_complete` reuses `ctx.completed` (populated by
            // batch-mode steps' `run_batch_reconcile_loop` and by this
            // same function's own success arm below for a task-on-task
            // completion dependency) — a real completion gate, same as
            // `task_deps` above, just against a different registry so a
            // step's completion is visible too, not only another task's.
            let complete_deps_ready = task.depends_on_complete.iter().all(|d| ctx.completed.lock().unwrap().contains(d));
            if steps_ready && tasks_ready && complete_deps_ready {
                break;
            }
        }

        info!(task = %task.name, forced, "firing task job");
        ctx.status.update_task(TaskStatus { name: task.name.clone(), state: TaskState::Running, last_run_at: Some(Utc::now()), last_error: None });

        let job_name = match runner
            .run_task_once(&task, &ctx.pod_kafka_bootstrap, &ctx.metrics_url, ctx.log_base_path.as_deref(), ctx.pod_s3_endpoint.as_deref())
            .await
        {
            Ok(name) => name,
            Err(e) => {
                error!(task = %task.name, error = %e, "failed to submit task job");
                ctx.status.update_task(TaskStatus {
                    name: task.name.clone(),
                    state: TaskState::Failed,
                    last_run_at: Some(Utc::now()),
                    last_error: Some(e.to_string()),
                });
                continue;
            }
        };

        // Polls until the Job finishes either way (not forever — a Job is
        // a one-shot run, so this loop has a natural end) so any other
        // task that names this one in `depends_on` can see the outcome,
        // and so `GET /status` reflects the real state. A cancel deletes
        // the Job directly (`crate::projects`'s cancel handler) — the
        // next poll here then sees `NotFound` (propagated as an `Err` by
        // `job_status`), logged and treated the same as any other poll
        // failure: back to the outer wait loop, ready for a later re-run.
        loop {
            tokio::time::sleep(JOB_STATUS_POLL_INTERVAL).await;
            match runner.job_status(&job_name).await {
                Ok(JobStatus::Succeeded) => {
                    info!(task = %task.name, job = %job_name, "task job completed successfully");
                    completed.lock().unwrap().insert(task.name.clone(), Outcome::Succeeded);
                    // Also visible via `ctx.completed` — the shared
                    // registry a step's `depends_on_complete` polls (see
                    // `activation::dependencies_satisfied`) — so a step
                    // can depend on a task's completion too, not just
                    // another task on this one via the local map above.
                    crate::activation::mark_completed(&ctx, &task.name);
                    ctx.status.update_task(TaskStatus {
                        name: task.name.clone(),
                        state: TaskState::Succeeded,
                        last_run_at: Some(Utc::now()),
                        last_error: None,
                    });
                    break;
                }
                Ok(JobStatus::Failed) => {
                    warn!(task = %task.name, job = %job_name, "task job failed");
                    completed.lock().unwrap().insert(task.name.clone(), Outcome::Failed);
                    ctx.status.update_task(TaskStatus {
                        name: task.name.clone(),
                        state: TaskState::Failed,
                        last_run_at: Some(Utc::now()),
                        last_error: Some(format!("job {job_name} failed")),
                    });
                    break;
                }
                Ok(JobStatus::Running) => continue,
                Err(e) => {
                    warn!(task = %task.name, job = %job_name, error = %e, "failed to poll task job status (job may have been cancelled)");
                    ctx.status.update_task(TaskStatus {
                        name: task.name.clone(),
                        state: TaskState::Failed,
                        last_run_at: Some(Utc::now()),
                        last_error: Some(e.to_string()),
                    });
                    break;
                }
            }
        }
    }
}
