use std::convert::From;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::request::Request;
use crate::service_fn::FromRequest;

/// A wrapper type that defines a task's namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace(pub String);

impl Deref for Namespace {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for Namespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Namespace {
    fn from(s: String) -> Self {
        Namespace(s)
    }
}

impl From<Namespace> for String {
    fn from(value: Namespace) -> String {
        value.0
    }
}

impl AsRef<str> for Namespace {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<Req, Ctx> FromRequest<Request<Req, Ctx>> for Namespace {
    fn from_request(req: &Request<Req, Ctx>) -> Result<Self, Error> {
        let msg = "Missing `Namespace`. This is a bug, please file a report with the backend you are using".to_owned();
        req.parts.namespace.clone().ok_or(Error::MissingData(msg))
    }
}
