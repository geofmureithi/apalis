use crate::service::JobService;
use futures::future::BoxFuture;

use crate::{context::JobContext, response::JobResponse};

/// Represents a result for a [Job] executed via [JobService].
pub type JobFuture<I> = BoxFuture<'static, I>;

/// Trait representing a job.
///
///
/// # Example
/// ```rust
/// impl Job for Email {
///     const NAME: &'static str = "apalis::Email";
/// }
/// ```
pub trait Job: Sized {
    /// Represents the name for job.
    const NAME: &'static str;
}

/// Trait representing a handler for Trait implementations
/// to be run via the [JobService] service
/// # Example
/// ```rust
/// impl JobHandler for Email {
///     type Result = JobFuture<Result<(), JobError>>;
///     fn handle(self, ctx: JobContext) -> Self::Result {     
///         let fut = async move {
///             let pool = ctx.data_opt::<PgPool>()?;
///             let sendgrid = ctx.data_opt::<SendgridClient>()?;
///             let to = self.get_email();
///             let user = pool.execute("Select * from users where email = ?1 LIMIT 1")
///                             .bind(to)
///                             .execute(pool).await?;
///             ctx.update_progress(50);
///             sendgrid.send_email(user, to).await?
///         };
///         Box::pin(fut)
///     }
/// }
pub trait JobHandler<J>
where
    J: Job,
{
    type Result: JobResponse;
    /// Consumes the job returning a result that implements [JobResponse]
    fn handle(self, ctx: JobContext) -> Self::Result;
}
