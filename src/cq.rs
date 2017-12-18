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

use std::mem;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::thread::{self, ThreadId};

use grpc_sys::{self, GprClockType, GrpcCompletionQueue};
use futures::Async;
use futures::future::BoxFuture;
use futures::executor::{Notify, Spawn};

use async::{Alarm, CallTag, SpinLock};

use error::{Error, Result};

pub use grpc_sys::GrpcCompletionType as EventType;
pub use grpc_sys::GrpcEvent as Event;

/// `CompletionQueueHandle` enable notification of the completion of asynchronous actions.
pub struct CompletionQueueHandle {
    cq: *mut GrpcCompletionQueue,
    // When `ref_cnt` < 0, a shutdown is pending, completion queue should not
    // accept requests anymore; when `ref_cnt` == 0, completion queue should
    // be shutdown; When `ref_cnt` > 0, completion queue can accept requests
    // and should not be shutdown.
    ref_cnt: AtomicIsize,
}

unsafe impl Sync for CompletionQueueHandle {}
unsafe impl Send for CompletionQueueHandle {}

impl CompletionQueueHandle {
    pub fn new() -> CompletionQueueHandle {
        CompletionQueueHandle {
            cq: unsafe { grpc_sys::grpc_completion_queue_create_for_next(ptr::null_mut()) },
            ref_cnt: AtomicIsize::new(1),
        }
    }

    fn add_ref(&self) -> Result<()> {
        loop {
            let cnt = self.ref_cnt.load(Ordering::SeqCst);
            if cnt <= 0 {
                // `shutdown` has been called, reject any requests.
                return Err(Error::QueueShutdown);
            }
            let new_cnt = cnt + 1;
            if cnt ==
                self.ref_cnt
                    .compare_and_swap(cnt, new_cnt, Ordering::SeqCst)
            {
                return Ok(());
            }
        }
    }

    fn unref(&self) {
        let shutdown = loop {
            let cnt = self.ref_cnt.load(Ordering::SeqCst);
            // If `shutdown` is not called, `cnt` > 0, so minus 1 to unref.
            // If `shutdown` is called, `cnt` < 0, so plus 1 to unref.
            let new_cnt = cnt - cnt.signum();
            if cnt ==
                self.ref_cnt
                    .compare_and_swap(cnt, new_cnt, Ordering::SeqCst)
            {
                break new_cnt == 0;
            }
        };
        if shutdown {
            unsafe {
                grpc_sys::grpc_completion_queue_shutdown(self.cq);
            }
        }
    }

    fn shutdown(&self) {
        let shutdown = loop {
            let cnt = self.ref_cnt.load(Ordering::SeqCst);
            if cnt <= 0 {
                // `shutdown` is called, skipped.
                return;
            }
            // Make cnt negative to indicate that `shutdown` has been called.
            // Because `cnt` is initialised to 1, so minus 1 to make it reach
            // toward 0. That is `new_cnt = -(cnt - 1) = -cnt + 1`.
            let new_cnt = -cnt + 1;
            if cnt ==
                self.ref_cnt
                    .compare_and_swap(cnt, new_cnt, Ordering::SeqCst)
            {
                break new_cnt == 0;
            }
        };
        if shutdown {
            unsafe {
                grpc_sys::grpc_completion_queue_shutdown(self.cq);
            }
        }
    }
}

impl Drop for CompletionQueueHandle {
    fn drop(&mut self) {
        unsafe { grpc_sys::grpc_completion_queue_destroy(self.cq) }
    }
}

pub struct CompletionQueueRef<'a> {
    queue: &'a CompletionQueue,
}

impl<'a> CompletionQueueRef<'a> {
    pub fn as_ptr(&self) -> *mut GrpcCompletionQueue {
        self.queue.handle.cq
    }
}

impl<'a> Drop for CompletionQueueRef<'a> {
    fn drop(&mut self) {
        self.queue.handle.unref();
    }
}

#[derive(Clone)]
pub struct CompletionQueue {
    handle: Arc<CompletionQueueHandle>,
    id: ThreadId,
    fq: Arc<ReadyQueue>,
}

impl CompletionQueue {
    pub fn new(handle: Arc<CompletionQueueHandle>, id: ThreadId) -> CompletionQueue {
        let fq = ReadyQueue {
            items: SpinLock::new(Vec::with_capacity(BATCH_SIZE)),
            worker_id: id,
        };
        CompletionQueue {
            handle: handle,
            id: id,
            fq: Arc::new(fq),
        }
    }

    /// Blocks until an event is available, the completion queue is being shut down.
    pub fn next(&self) -> Event {
        unsafe {
            let inf = grpc_sys::gpr_inf_future(GprClockType::Realtime);
            grpc_sys::grpc_completion_queue_next(self.handle.cq, inf, ptr::null_mut())
        }
    }

    pub fn borrow(&self) -> Result<CompletionQueueRef> {
        self.handle.add_ref()?;
        Ok(CompletionQueueRef { queue: self })
    }

    /// Begin destruction of a completion queue.
    ///
    /// Once all possible events are drained then `next()` will start to produce
    /// `Event::QueueShutdown` events only.
    pub fn shutdown(&self) {
        self.handle.shutdown()
    }

    pub fn worker_id(&self) -> ThreadId {
        self.id
    }

    fn push_and_notify(&self, f: Item) {
        self.fq.push_and_notify(f, self.clone())
    }

    fn pop_and_poll(&self, notify: QueueNotify) {
        self.fq.pop_and_poll(notify, self.clone());
    }
}

const BATCH_SIZE: usize = 1024;
type Item = Spawn<BoxFuture<(), ()>>;

struct ReadyQueue {
    items: SpinLock<Vec<Item>>,
    worker_id: ThreadId,
}

unsafe impl Send for ReadyQueue {}
unsafe impl Sync for ReadyQueue {}

impl ReadyQueue {
    fn push_and_notify(&self, f: Item, cq: CompletionQueue) {
        let notify = QueueNotify::new(cq.clone());

        if thread::current().id() == self.worker_id {
            let notify = Arc::new(notify);
            poll(f, &notify);
        } else {
            let count = {
                let mut items = self.items.lock();
                items.push(f);
                items.len()
            };

            if 1 == count {
                let alarm = notify.alarm.clone();
                let tag = Box::new(CallTag::Queue(notify));
                let mut al = alarm.lock();
                // We need to keep the alarm until queue is empty.
                *al = Some(Alarm::new(&cq, tag).unwrap());
                al.as_mut().unwrap().alarm();
            }
        }
    }

    fn pop_and_poll(&self, mut notify: QueueNotify, cq: CompletionQueue) {
        // Drop alarm without locking.
        notify.alarm = Arc::new(SpinLock::new(None));
        let mut notify = Arc::new(notify);

        let mut batch: Vec<Item> = Vec::with_capacity(64); 
	{
            let mut items = self.items.lock();
	    mem::swap(&mut *items, &mut batch);
            //batch = items.drain(..).collect();
        };

        let mut done = true;
        for f in batch {
            notify = if done {
                // Future has resloved, and the notify is empty, reuse it.
                notify
            } else {
                // Future is not complete yet. Other thread holds the notify,
                // create a new one for the next ready Future.
                Arc::new(QueueNotify::new(cq.clone()))
            };

            done = poll(f, &notify);
        }

        if done {
            notify.alarm.lock().take();
        }
    }
}

fn poll(f: Item, notify: &Arc<QueueNotify>) -> bool {
    let mut option = notify.f.lock();
    *option = Some(f);
    match option.as_mut().unwrap().poll_future_notify(notify, 0) {
        Err(_) | Ok(Async::Ready(_)) => {
            // Future has resloved, empty the future so that we can
            // reuse the notify.
            option.take();
            true
        }
        Ok(Async::NotReady) => {
            // Future is not complete yet.
            false
        }
    }
}

#[derive(Clone)]
pub struct QueueNotify {
    cq: CompletionQueue,
    f: Arc<SpinLock<Option<Item>>>,
    alarm: Arc<SpinLock<Option<Alarm>>>,
}

unsafe impl Send for QueueNotify {}
unsafe impl Sync for QueueNotify {}

impl QueueNotify {
    pub fn new(cq: CompletionQueue) -> QueueNotify {
        QueueNotify {
            cq: cq,
            f: Arc::new(SpinLock::new(None)),
            alarm: Arc::new(SpinLock::new(None)),
        }
    }

    pub fn resolve(self, success: bool) {
        // it should always be canceled for now.
        assert!(!success);
        self.cq.clone().pop_and_poll(self);
    }

    pub fn push_and_notify(&self, f: Item) {
        self.cq.push_and_notify(f);
    }
}

impl Notify for QueueNotify {
    fn notify(&self, _: usize) {
        if let Some(f) = self.f.lock().take() {
            self.cq.push_and_notify(f);
        }
    }
}

