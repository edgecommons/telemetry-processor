//! # Test-only fakes shared across unit tests (compiled only under `#[cfg(test)]`)
//!
//! [`FakeMessaging`] implements [`MessagingService`] entirely in-process: it records every
//! `publish`/`publish_northbound` call and lets a test invoke a subscribed handler directly
//! ([`FakeMessaging::deliver`]) to simulate an inbound message — no broker required. This is the
//! downstream analog of the (crate-private) `edgecommons::testutil::RecordingMessaging` the library
//! uses for its own tests; component crates cannot reach that type (it is `pub(crate)`), so this
//! crate carries its own, exactly as `file-replicator`'s `src/events.rs` carries its own recording
//! substitute for the (also unconstructable outside the library) `EventsFacade` (see
//! [`crate::observe::EvtEmitter::recording`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edgecommons::messaging::message::Message;
use edgecommons::prelude::{MessageHandler, MessagingService, Qos, ReplyFuture};
use edgecommons::{EdgeCommonsError, Result};

/// A [`MessagingService`] fake: records local/northbound publishes, and lets a test simulate an
/// inbound message by invoking the handler stored for a subscribed filter.
#[derive(Default)]
pub(crate) struct FakeMessaging {
    pub published: Mutex<Vec<(String, Message)>>,
    pub northbound: Mutex<Vec<(String, Message, Qos)>>,
    handlers: Mutex<HashMap<String, Arc<dyn MessageHandler>>>,
    pub unsubscribed: Mutex<Vec<String>>,
    /// When set, `publish`/`publish_northbound` return this error instead of recording — drives the
    /// "a publish failure is tallied + surfaced as `evt`" tests.
    fail_publish: Mutex<Option<String>>,
}

impl FakeMessaging {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Simulate an inbound message on `filter`'s handler (a no-op if nothing subscribed it).
    pub async fn deliver(&self, filter: &str, topic: &str, msg: Message) {
        let handler = self.handlers.lock().unwrap().get(filter).cloned();
        if let Some(h) = handler {
            h.handle(topic.to_string(), msg).await;
        }
    }

    /// Whether `filter` currently has a live subscription (inserted by `subscribe`, removed by
    /// `unsubscribe`).
    pub fn is_subscribed(&self, filter: &str) -> bool {
        self.handlers.lock().unwrap().contains_key(filter)
    }

    pub fn set_fail_publish(&self, reason: impl Into<String>) {
        *self.fail_publish.lock().unwrap() = Some(reason.into());
    }
}

#[async_trait::async_trait]
impl MessagingService for FakeMessaging {
    async fn publish(&self, topic: &str, msg: &Message) -> Result<()> {
        if let Some(reason) = self.fail_publish.lock().unwrap().clone() {
            return Err(EdgeCommonsError::Messaging(reason));
        }
        self.published.lock().unwrap().push((topic.to_string(), msg.clone()));
        Ok(())
    }

    async fn publish_northbound(&self, topic: &str, msg: &Message, qos: Qos) -> Result<()> {
        if let Some(reason) = self.fail_publish.lock().unwrap().clone() {
            return Err(EdgeCommonsError::Messaging(reason));
        }
        self.northbound.lock().unwrap().push((topic.to_string(), msg.clone(), qos));
        Ok(())
    }

    async fn publish_raw(&self, _topic: &str, _payload: &serde_json::Value) -> Result<()> {
        Err(EdgeCommonsError::Messaging("publish_raw not supported by FakeMessaging".into()))
    }

    async fn publish_northbound_raw(
        &self,
        _topic: &str,
        _payload: &serde_json::Value,
        _qos: Qos,
    ) -> Result<()> {
        Err(EdgeCommonsError::Messaging(
            "publish_northbound_raw not supported by FakeMessaging".into(),
        ))
    }

    async fn subscribe(
        &self,
        filter: &str,
        handler: Arc<dyn MessageHandler>,
        _max_messages: usize,
        _max_concurrency: usize,
    ) -> Result<()> {
        self.handlers.lock().unwrap().insert(filter.to_string(), handler);
        Ok(())
    }

    async fn subscribe_northbound(
        &self,
        _filter: &str,
        _handler: Arc<dyn MessageHandler>,
        _qos: Qos,
        _max_messages: usize,
        _max_concurrency: usize,
    ) -> Result<()> {
        Err(EdgeCommonsError::Messaging("subscribe_northbound not supported by FakeMessaging".into()))
    }

    async fn unsubscribe(&self, filter: &str) -> Result<()> {
        self.handlers.lock().unwrap().remove(filter);
        self.unsubscribed.lock().unwrap().push(filter.to_string());
        Ok(())
    }

    async fn unsubscribe_northbound(&self, _filter: &str) -> Result<()> {
        Ok(())
    }

    async fn request(&self, _topic: &str, _msg: Message) -> Result<ReplyFuture> {
        Err(EdgeCommonsError::Messaging("request not supported by FakeMessaging".into()))
    }

    async fn request_northbound(&self, _topic: &str, _msg: Message) -> Result<ReplyFuture> {
        Err(EdgeCommonsError::Messaging("request_northbound not supported by FakeMessaging".into()))
    }

    async fn request_with_timeout(
        &self,
        _topic: &str,
        _msg: Message,
        _timeout: Option<Duration>,
    ) -> Result<ReplyFuture> {
        Err(EdgeCommonsError::Messaging(
            "request_with_timeout not supported by FakeMessaging".into(),
        ))
    }

    async fn request_northbound_with_timeout(
        &self,
        _topic: &str,
        _msg: Message,
        _timeout: Option<Duration>,
    ) -> Result<ReplyFuture> {
        Err(EdgeCommonsError::Messaging(
            "request_northbound_with_timeout not supported by FakeMessaging".into(),
        ))
    }

    async fn reply(&self, _request: &Message, _reply: Message) -> Result<()> {
        Err(EdgeCommonsError::Messaging("reply not supported by FakeMessaging".into()))
    }

    async fn reply_northbound(&self, _request: &Message, _reply: Message) -> Result<()> {
        Err(EdgeCommonsError::Messaging("reply_northbound not supported by FakeMessaging".into()))
    }

    fn cancel_request(&self, _reply_future: ReplyFuture) {}
    fn cancel_request_northbound(&self, _reply_future: ReplyFuture) {}

    fn connected(&self) -> bool {
        true
    }
}
