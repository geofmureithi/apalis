use futures::StreamExt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::notify::Notify;
use crate::request::RequestStream;
use crate::worker::{Worker, WorkerId};

use self::controller::Control;
use self::stream::BackendStream;

/// Util for controlling pollers
pub mod controller;
/// Util for controlled stream
pub mod stream;

const STOPPED: usize = 2;
const PLUGGED: usize = 1;
const UNPLUGGED: usize = 0;

/// Tells the poller that the worker is ready for a new request
#[derive(Debug)]
pub struct Ready<T> {
    instance: usize,
    sender: async_oneshot::Sender<T>,
}

impl<T> Ready<T> {
    /// Get the specific worker instance
    pub fn instance(&self) -> usize {
        self.instance
    }
}

impl<T> Deref for Ready<T> {
    type Target = async_oneshot::Sender<T>;
    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<T> DerefMut for Ready<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}
impl<T> Ready<T> {
    /// Generate a new instance of ready
    pub fn new(sender: async_oneshot::Sender<T>, instance: usize) -> Self {
        Self { instance, sender }
    }
}
