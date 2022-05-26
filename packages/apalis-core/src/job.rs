use crate::{error::StorageError, service::JobService};
use futures::future::BoxFuture;
use serde::{de::DeserializeOwned, Serialize};

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
pub trait Job: Sized + Send + Unpin {
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

/// Job objects that can be reconstructed from the data stored in Storage.
///
/// Implemented for all `Deserialize` objects by default by relying on Msgpack
/// decoding.
trait JobDecodable
where
    Self: Sized,
{
    /// Decode the given Redis value into a message
    ///
    /// In the default implementation, the string value is decoded by assuming
    /// it was encoded through the Msgpack encoding.
    fn decode_job(value: &Vec<u8>) -> Result<Self, StorageError>;
}

/// Job objects that can be encoded to a string to be stored in Storage.
///
/// Implemented for all `Serialize` objects by default by encoding with Serde.
trait JobEncodable
where
    Self: Sized,
{
    /// Encode the value into a bytes array to be inserted into Storage.
    ///
    /// In the default implementation, the object is encoded with Serde.
    fn encode_job(&self) -> Result<Vec<u8>, StorageError>;
}

impl<T> JobDecodable for T
where
    T: DeserializeOwned,
{
    fn decode_job(value: &Vec<u8>) -> Result<T, StorageError> {
        Ok(serde_json::from_slice(value)?)
    }
}

impl<T: Serialize> JobEncodable for T {
    fn encode_job(&self) -> Result<Vec<u8>, StorageError> {
        Ok(serde_json::to_vec(self)?)
    }
}
