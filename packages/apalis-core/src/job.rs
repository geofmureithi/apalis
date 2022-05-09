use crate::service::JobService;
use futures::future::BoxFuture;

use crate::{context::JobContext, response::JobResponse};

/// Represents a result for a [Job] executed via [JobService].
pub type JobFuture<I> = BoxFuture<'static, I>;

/// Trait representing a job to be run via the [JobService] service.
///
///
/// # Example
/// ```rust
/// impl Job for Email {
///     type Result = JobFuture<Result<(), JobError>>;
///     fn handle(&self, ctx: &JobContext) -> Self::Result {
///         let pool = ctx.data_opt::<PgPool>()?;
///         let sendgrid = ctx.data_opt::<SendgridClient>()?;
///         let to = self.get_email();
///         let fut = async move {
///             let user = pool.execute("Select * from users where email = ?1 LIMIT 1")
///                             .bind(to)
///                             .execute(pool).await?;
///             sendgrid.send_email(user, to).await?
///         };
///         Box::pin(fut)
///     }
/// }
/// ```
pub trait Job: Sized {
    /// The result of a job
    type Result: JobResponse + 'static;

    /// Represents the name for job.
    ///
    /// It defaults to [type_name]
    /// Note: Its advisable to set a name in production to avoid issues with refactoring.
    ///
    /// [type_name]: https://doc.rust-lang.org/std/any/fn.type_name.html
    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Consumes the job returning a result that implements [IntoResponse]
    fn handle(self, ctx: &JobContext) -> Self::Result
    where
        Self: Sized;
}
