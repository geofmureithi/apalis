use std::{convert::Infallible, sync::Arc};

use apalis_core::{task::Task, task_fn::FromRequest};

/// The context provided to each cron job
#[derive(Debug)]
pub struct CronContext<Schedule> {
    /// The schedule for the cron job
    schedule: Option<Arc<Schedule>>,
}

impl<Schedule> Default for CronContext<Schedule> {
    fn default() -> Self {
        Self { schedule: None }
    }
}

impl<Schedule> Clone for CronContext<Schedule> {
    fn clone(&self) -> Self {
        Self {
            schedule: self.schedule.clone(),
        }
    }
}

impl<Schedule> CronContext<Schedule> {
    /// Creates a new `CronContext` with the given schedule.
    pub fn new(schedule: Arc<Schedule>) -> Self {
        Self {
            schedule: Some(schedule),
        }
    }

    /// Returns a reference to the schedule, if it exists.
    pub fn schedule(&self) -> Option<&Schedule> {
        self.schedule.as_deref()
    }
}

impl<Args: Sync, IdType: Sync, Schedule: Sync + Send>
    FromRequest<Task<Args, CronContext<Schedule>, IdType>> for CronContext<Schedule>
{
    type Error = Infallible;
    async fn from_request(
        req: &Task<Args, CronContext<Schedule>, IdType>,
    ) -> Result<Self, Self::Error> {
        Ok(req.parts.ctx.clone())
    }
}
