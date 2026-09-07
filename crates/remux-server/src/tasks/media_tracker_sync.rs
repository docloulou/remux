use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{info, warn};

use super::{ProgressReporter, Task, TaskCategory, TaskService};
use crate::{AppContext, db, services::media_tracker::sync_tracker};

/// Pulls each user's remote watch history and ratings into their library.
/// The push direction needs no task: it rides on the playback events.
pub struct MediaTrackerSyncTask;

#[async_trait]
impl Task for MediaTrackerSyncTask {
    fn key(&self) -> &str {
        "MediaTrackerSync"
    }

    fn name(&self) -> &str {
        "Sync Media Trackers"
    }

    fn description(&self) -> &str {
        "Pulls watched state and ratings from every user's connected media tracker (such as Simkl) into their remux library. Only changes since the previous pull are fetched. Runs hourly by default."
    }

    fn short_description(&self) -> &str {
        "Pulls remote watch history and ratings"
    }

    fn category(&self) -> TaskCategory {
        TaskCategory::Users
    }

    async fn run(
        &self,
        ctx: AppContext,
        _tasks: Arc<TaskService>,
        progress: ProgressReporter,
    ) -> Result<()> {
        // Copy the ids out so the runtime list is not held across awaits.
        let addon_ids: Vec<uuid::Uuid> = ctx
            .addons
            .list()
            .iter()
            .filter(|r| {
                r.row
                    .enabled
                    && r.caps
                        .media_tracker
                        .is_some()
            })
            .map(|r| {
                r.row
                    .id
            })
            .collect();

        let mut trackers = Vec::new();
        for addon_id in addon_ids {
            trackers.extend(
                db::UserMediaTracker::list_for_addon(&ctx.db, addon_id)
                    .await?
                    .into_iter()
                    .filter(|t| t.status == db::MediaTrackerStatus::Connected),
            );
        }

        let total = trackers.len();
        let mut failures = 0usize;
        for (i, tracker) in trackers
            .iter()
            .enumerate()
        {
            progress.report(i, total);
            match sync_tracker(&ctx, tracker, false).await {
                Ok(report) => info!(
                    tracker = %tracker.id,
                    user = %tracker.user_id,
                    received = report.received,
                    matched = report.matched,
                    marked_played = report.marked_played,
                    rated = report.rated,
                    "media tracker pull finished"
                ),
                Err(e) => {
                    failures += 1;
                    warn!(
                        tracker = %tracker.id,
                        user = %tracker.user_id,
                        error = %e,
                        "media tracker pull failed"
                    );
                }
            }
        }
        progress.set(100.0);
        if failures > 0 && failures == total {
            anyhow::bail!("every media tracker pull failed ({failures} of {total})");
        }
        Ok(())
    }
}
