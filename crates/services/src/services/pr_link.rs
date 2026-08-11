//! Detects `pr-link` records emitted by a Claude Code agent that opened a pull
//! request itself (via `gh`/`git`) rather than through vibe-kanban's own PR
//! flow, and records that PR against the owning workspace.
//!
//! Without this the PR is invisible to the board: `pull_requests` rows are
//! otherwise only written by `POST /api/workspaces/{id}/git/pr`, so an
//! agent-opened PR never surfaces on its card and `pr_monitor` — which polls
//! `PullRequest::get_open` — never syncs its merge state either.
//!
//! Like the pipeline-stage tracker this scans the shared per-execution
//! `MsgStore` stdout stream, so headless (child stdout) and headed (transcript
//! tail) executions are handled identically with no per-executor knowledge.

use std::{
    collections::HashSet,
    sync::{Arc, LazyLock, Mutex},
};

use db::{
    DBService,
    models::{pull_request::PullRequest, workspace_repo::WorkspaceRepo},
};
use futures::StreamExt;
use serde::Deserialize;
use utils::msg_store::MsgStore;
use uuid::Uuid;

/// Idempotency guard keyed by execution_process_id, mirroring the
/// pipeline-stage tracker: normal execution start and headed re-adoption are
/// separate call sites that both spawn a tracker on their (fresh) `MsgStore`.
static TRACKED_EXECUTIONS: LazyLock<Mutex<HashSet<Uuid>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PrLink {
    #[serde(rename = "prNumber")]
    pub pr_number: i64,
    #[serde(rename = "prUrl")]
    pub pr_url: String,
    /// `owner/repo` as reported by the agent. Absent on older clients, in
    /// which case a single-repo workspace is unambiguous anyway.
    #[serde(rename = "prRepository", default)]
    pub pr_repository: Option<String>,
}

/// Parse a `pr-link` record out of one raw stdout line.
///
/// The line is the agent's own JSON record, e.g.
/// `{"type":"pr-link","prNumber":61,"prUrl":"https://…/pull/61", …}`.
/// Anything that is not a well-formed `pr-link` object, or that carries a
/// non-positive number or an empty URL, is ignored.
pub fn parse_pr_link(line: &str) -> Option<PrLink> {
    let trimmed = line.trim();
    // Cheap reject before paying for a full parse: every candidate is a JSON
    // object mentioning the discriminator.
    if !trimmed.starts_with('{') || !trimmed.contains("pr-link") {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(trimmed).ok()?;
    if value.get("type")?.as_str()? != "pr-link" {
        return None;
    }
    let link: PrLink = serde_json::from_value(value).ok()?;
    if link.pr_number <= 0 || link.pr_url.trim().is_empty() {
        return None;
    }
    Some(link)
}

/// Resolve which of the workspace's repos a `pr-link` belongs to and persist
/// it. Ambiguity is left unrecorded rather than guessed: attributing a PR to
/// the wrong repo would show a bogus PR on the card and start syncing it.
async fn record_pr_link(db: &DBService, workspace_id: Uuid, link: &PrLink) {
    let repos = match WorkspaceRepo::find_repos_for_workspace(&db.pool, workspace_id).await {
        Ok(repos) => repos,
        Err(e) => {
            tracing::warn!("pr-link: failed to load repos for workspace {workspace_id}: {e}");
            return;
        }
    };

    // Prefer an explicit `owner/repo` match; fall back to the sole repo of a
    // single-repo workspace.
    let repo = link
        .pr_repository
        .as_deref()
        .and_then(|full| {
            let name = full.rsplit('/').next().unwrap_or(full);
            repos.iter().find(|r| r.name.eq_ignore_ascii_case(name))
        })
        .or(if repos.len() == 1 {
            repos.first()
        } else {
            None
        });

    let Some(repo) = repo else {
        tracing::warn!(
            "pr-link: could not attribute {} to a repo of workspace {workspace_id} ({} repos, reported {:?})",
            link.pr_url,
            repos.len(),
            link.pr_repository
        );
        return;
    };

    let target_branch = WorkspaceRepo::find_by_workspace_id(&db.pool, workspace_id)
        .await
        .ok()
        .and_then(|wrs| {
            wrs.into_iter()
                .find(|wr| wr.repo_id == repo.id)
                .map(|wr| wr.target_branch)
        })
        .or_else(|| repo.default_target_branch.clone())
        .unwrap_or_else(|| "main".to_string());

    // `create` upserts on `pr_url`, so a record replayed from persisted logs
    // (or reported twice by the agent) is harmless.
    match PullRequest::create_for_workspace(
        &db.pool,
        workspace_id,
        repo.id,
        &target_branch,
        link.pr_number,
        &link.pr_url,
    )
    .await
    {
        Ok(_) => tracing::info!(
            "pr-link: recorded {} for workspace {workspace_id} (repo {})",
            link.pr_url,
            repo.name
        ),
        Err(e) => tracing::warn!("pr-link: failed to record {}: {e}", link.pr_url),
    }
}

pub fn spawn_pr_link_tracker(
    store: Arc<MsgStore>,
    workspace_id: Uuid,
    execution_process_id: Uuid,
    db: DBService,
) {
    {
        let mut tracked = TRACKED_EXECUTIONS.lock().unwrap();
        if !tracked.insert(execution_process_id) {
            return;
        }
    }

    tokio::spawn(async move {
        let mut lines = store.stdout_lines_stream();
        let mut seen: HashSet<String> = HashSet::new();

        while let Some(line) = lines.next().await {
            let Ok(line) = line else { continue };
            let Some(link) = parse_pr_link(&line) else {
                continue;
            };
            // Within one execution the agent re-announces the same PR on
            // resume; only the first sighting needs a write.
            if !seen.insert(link.pr_url.clone()) {
                continue;
            }
            record_pr_link(&db, workspace_id, &link).await;
        }

        TRACKED_EXECUTIONS
            .lock()
            .unwrap()
            .remove(&execution_process_id);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const REAL: &str = r#"{"type":"pr-link","sessionId":"68bbfcea-8c88-4293-8d91-1d11a1de3601","prNumber":61,"prUrl":"https://github.com/dromos-inc/kidbounds/pull/61","prRepository":"dromos-inc/kidbounds","timestamp":"2026-08-10T20:50:00Z"}"#;

    #[test]
    fn parses_a_real_record() {
        let link = parse_pr_link(REAL).expect("should parse");
        assert_eq!(link.pr_number, 61);
        assert_eq!(link.pr_url, "https://github.com/dromos-inc/kidbounds/pull/61");
        assert_eq!(link.pr_repository.as_deref(), Some("dromos-inc/kidbounds"));
    }

    #[test]
    fn tolerates_surrounding_whitespace() {
        assert!(parse_pr_link(&format!("  {REAL}  ")).is_some());
    }

    #[test]
    fn ignores_other_record_types() {
        assert!(
            parse_pr_link(
                r#"{"type":"bridge-session","sessionId":"x","bridgeSessionId":"y","lastSequenceNum":0}"#
            )
            .is_none()
        );
        assert!(parse_pr_link(r#"{"type":"assistant","message":{}}"#).is_none());
    }

    #[test]
    fn ignores_prose_mentioning_the_marker() {
        assert!(parse_pr_link("the agent emits a pr-link record when it opens a PR").is_none());
    }

    #[test]
    fn ignores_malformed_or_empty_values() {
        assert!(parse_pr_link(r#"{"type":"pr-link","prNumber":0,"prUrl":"u"}"#).is_none());
        assert!(parse_pr_link(r#"{"type":"pr-link","prNumber":5,"prUrl":""}"#).is_none());
        assert!(parse_pr_link(r#"{"type":"pr-link","prUrl":"u"}"#).is_none());
        assert!(parse_pr_link(r#"{"type":"pr-link","#).is_none());
    }

    #[test]
    fn repository_field_is_optional() {
        let link = parse_pr_link(r#"{"type":"pr-link","prNumber":7,"prUrl":"https://x/pull/7"}"#)
            .expect("should parse");
        assert_eq!(link.pr_repository, None);
    }
}
