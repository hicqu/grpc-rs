// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use futures::executor::{self, Notify, Spawn};
use futures::{Async, Future};

use super::lock::SpinLock;
use cq::CompletionQueue;

type BoxFuture<T, E> = Box<Future<Item = T, Error = E> + Send>;

/// A handle to a `Spawn`.
/// Inner future is expected to be polled in the same thread as cq.
type SpawnHandle = Option<Spawn<BoxFuture<(), ()>>>;

struct NotifyContext {
    kicked: bool,
}

/// A custom notify.
///
/// It will poll the inner future directly if it's notified on the
/// same thread as inner cq.
#[derive(Clone)]
pub struct SpawnNotify {
    ctx: Arc<SpinLock<NotifyContext>>,
    handle: Arc<SpinLock<SpawnHandle>>,
}

impl SpawnNotify {
    fn new(s: Spawn<BoxFuture<(), ()>>) -> SpawnNotify {
        SpawnNotify {
            handle: Arc::new(SpinLock::new(Some(s))),
            ctx: Arc::new(SpinLock::new(NotifyContext {
                kicked: false,
            })),
        }
    }
}

impl Notify for SpawnNotify {
    fn notify(&self, _: usize) {
        poll(&Arc::new(self.clone()), false)
    }
}

/// Poll the future.
///
/// `woken` indicates that if the cq is kicked by itself.
fn poll(notify: &Arc<SpawnNotify>, woken: bool) {
    let mut handle = notify.handle.lock();
    if woken {
        notify.ctx.lock().kicked = false;
    }
    if handle.is_none() {
        // it's resolved, no need to poll again.
        return;
    }
    match handle.as_mut().unwrap().poll_future_notify(notify, 0) {
        Err(_) | Ok(Async::Ready(_)) => {
            // Future stores notify, and notify contains future,
            // hence circular reference. Take the future to break it.
            handle.take();
            return;
        }
        _ => {}
    }
}

/// An executor that drives a future in the gRPC poll thread, which
/// can reduce thread context switching.
pub(crate) struct Executor<'a> {
    cq: &'a CompletionQueue,
}

impl<'a> Executor<'a> {
    pub fn new(cq: &CompletionQueue) -> Executor {
        Executor { cq }
    }

    pub fn cq(&self) -> &CompletionQueue {
        self.cq
    }

    /// Spawn the future into inner poll loop.
    ///
    /// If you want to trace the future, you may need to create a sender/receiver
    /// pair by yourself.
    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        let s = executor::spawn(Box::new(f) as BoxFuture<_, _>);
        let notify = Arc::new(SpawnNotify::new(s));
        poll(&notify, false)
    }
}
