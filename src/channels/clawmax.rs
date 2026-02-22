//! ClawMax channel - WebSocket JSON transport.
//!
//! This channel connects to a user-provided WebSocket endpoint and exchanges
//! JSON messages that mirror ZeroClaw's internal channel message structure.

use crate::config::traits::ChannelConfig;

use super::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use std::time::{SystemTime, UNIX_EPOCH};

/// ClawMax channel configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClawMaxConfig {
    /// WebSocket server URL (ws:// or wss://).
    pub ws_url: String,
    /// Allowed sender IDs. Empty = deny all. "*" = allow all.
    #[serde(default)]
    pub allowed_senders: Vec<String>,
}

impl ChannelConfig for ClawMaxConfig {
    fn name() -> &'static str {
        "ClawMax"
    }
    fn desc() -> &'static str {
        "WebSocket JSON channel"
    }
}

pub struct ClawMaxChannel {
    ws_url: String,
    allowed_senders: Vec<String>,
    allow_all: bool,
    outbound_tx: Mutex<Option<mpsc::Sender<OutboundFrame>>>,
}

enum OutboundFrame {
    Text(String),
}

#[derive(Serialize)]
struct OutboundEnvelope<'a> {
    #[serde(rename = "type")]
    frame_type: &'static str,
    direction: &'static str,
    message: OutboundMessage<'a>,
}

#[derive(Serialize)]
struct OutboundMessage<'a> {
    id: String,
    channel: &'static str,
    timestamp: u64,
    content: &'a str,
    recipient: &'a str,
    subject: Option<&'a str>,
    thread_ts: Option<&'a str>,
}

impl ClawMaxChannel {
    pub fn new(config: ClawMaxConfig) -> Self {
        let allow_all = config.allowed_senders.iter().any(|s| s == "*");
        Self {
            ws_url: config.ws_url.trim().to_string(),
            allowed_senders: config.allowed_senders,
            allow_all,
            outbound_tx: Mutex::new(None),
        }
    }

    fn validate_ws_url(&self) -> anyhow::Result<()> {
        if self.ws_url.starts_with("ws://") || self.ws_url.starts_with("wss://") {
            return Ok(());
        }
        anyhow::bail!("ClawMax ws_url must start with ws:// or wss://")
    }

    fn is_sender_allowed(&self, sender: &str) -> bool {
        if self.allow_all {
            return true;
        }
        if self.allowed_senders.is_empty() {
            return false;
        }
        self.allowed_senders.iter().any(|allowed| allowed == sender)
    }

    fn now_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn parse_timestamp(value: Option<&serde_json::Value>) -> Option<u64> {
        match value {
            Some(v) => {
                if let Some(n) = v.as_u64() {
                    return Some(n);
                }
                if let Some(n) = v.as_i64() {
                    if n >= 0 {
                        return Some(n as u64);
                    }
                }
                if let Some(s) = v.as_str() {
                    return s.parse::<u64>().ok();
                }
                None
            }
            None => None,
        }
    }

    fn parse_inbound_value(&self, value: &serde_json::Value) -> Option<ChannelMessage> {
        let frame_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if frame_type != "message" {
            return None;
        }

        if let Some(direction) = value.get("direction").and_then(|v| v.as_str()) {
            if direction != "in" && direction != "inbound" {
                return None;
            }
        }

        let payload = value.get("message").unwrap_or(value);

        let sender = payload
            .get("sender")
            .and_then(|v| v.as_str())
            .or_else(|| payload.get("from").and_then(|v| v.as_str()))?;

        if !self.is_sender_allowed(sender) {
            tracing::warn!("ClawMax: ignoring message from unauthorized sender: {sender}");
            return None;
        }

        let content = payload
            .get("content")
            .and_then(|v| v.as_str())
            .map(str::trim)?;

        if content.is_empty() {
            return None;
        }

        let reply_target = payload
            .get("reply_target")
            .and_then(|v| v.as_str())
            .or_else(|| payload.get("recipient").and_then(|v| v.as_str()))
            .unwrap_or(sender);

        let id = payload
            .get("id")
            .and_then(|v| v.as_str())
            .or_else(|| payload.get("message_id").and_then(|v| v.as_str()))
            .map(|v| v.to_string())
            .unwrap_or_else(|| format!("clawmax_{}", Uuid::new_v4()));

        let thread_ts = payload
            .get("thread_ts")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string());

        let raw_channel = payload
            .get("channel")
            .and_then(|v| v.as_str())
            .unwrap_or("clawmax");
        let channel = if raw_channel == "clawmax" {
            "clawmax"
        } else {
            tracing::warn!(
                "ClawMax: inbound channel '{raw_channel}' does not match; defaulting to clawmax"
            );
            "clawmax"
        };

        let timestamp = Self::parse_timestamp(payload.get("timestamp"))
            .unwrap_or_else(Self::now_timestamp);

        Some(ChannelMessage {
            id,
            sender: sender.to_string(),
            reply_target: reply_target.to_string(),
            content: content.to_string(),
            channel: channel.to_string(),
            timestamp,
            thread_ts,
        })
    }

    fn build_outbound_frame(message: &SendMessage) -> anyhow::Result<OutboundFrame> {
        let envelope = OutboundEnvelope {
            frame_type: "message",
            direction: "out",
            message: OutboundMessage {
                id: format!("clawmax_out_{}", Uuid::new_v4()),
                channel: "clawmax",
                timestamp: Self::now_timestamp(),
                content: message.content.as_str(),
                recipient: message.recipient.as_str(),
                subject: message.subject.as_deref(),
                thread_ts: message.thread_ts.as_deref(),
            },
        };

        Ok(OutboundFrame::Text(serde_json::to_string(&envelope)?))
    }

    async fn set_outbound_sender(&self, sender: Option<mpsc::Sender<OutboundFrame>>) {
        let mut guard = self.outbound_tx.lock().await;
        *guard = sender;
    }

    async fn outbound_sender(&self) -> Option<mpsc::Sender<OutboundFrame>> {
        self.outbound_tx.lock().await.clone()
    }
}

#[async_trait]
impl Channel for ClawMaxChannel {
    fn name(&self) -> &str {
        "clawmax"
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        let sender = self
            .outbound_sender()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClawMax channel is not connected"))?;

        let frame = Self::build_outbound_frame(message)?;
        sender
            .send(frame)
            .await
            .map_err(|e| anyhow::anyhow!("ClawMax outbound queue closed: {e}"))
    }

    async fn listen(&self, tx: mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        self.validate_ws_url()?;

        tracing::info!("ClawMax: connecting to {}", self.ws_url);
        let (ws_stream, _) = tokio_tungstenite::connect_async(&self.ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        let (out_tx, mut out_rx) = mpsc::channel::<OutboundFrame>(64);
        self.set_outbound_sender(Some(out_tx)).await;

        tracing::info!("ClawMax: connected and listening for messages...");

        loop {
            tokio::select! {
                inbound = read.next() => {
                    match inbound {
                        Some(Ok(Message::Text(text))) => {
                            let value: serde_json::Value = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(err) => {
                                    tracing::debug!("ClawMax: invalid JSON payload: {err}");
                                    continue;
                                }
                            };

                            let frame_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            if frame_type == "ping" {
                                let pong = serde_json::json!({
                                    "type": "pong",
                                    "ts": Self::now_timestamp()
                                });
                                if let Err(err) = write.send(Message::Text(pong.to_string().into())).await {
                                    tracing::warn!("ClawMax: failed to send pong: {err}");
                                    break;
                                }
                                continue;
                            }

                            if let Some(msg) = self.parse_inbound_value(&value) {
                                if tx.send(msg).await.is_err() {
                                    tracing::warn!("ClawMax: message channel closed");
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            if let Err(err) = write.send(Message::Pong(payload)).await {
                                tracing::warn!("ClawMax: failed to reply to ping: {err}");
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) => break,
                        Some(Ok(_)) => {}
                        Some(Err(err)) => {
                            tracing::warn!("ClawMax WebSocket error: {err}");
                            break;
                        }
                        None => break,
                    }
                }
                outbound = out_rx.recv() => {
                    match outbound {
                        Some(frame) => {
                            let OutboundFrame::Text(text) = frame;
                            if let Err(err) = write.send(Message::Text(text.into())).await {
                                tracing::warn!("ClawMax: failed to send outbound frame: {err}");
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        self.set_outbound_sender(None).await;
        anyhow::bail!("ClawMax WebSocket stream ended")
    }

    async fn health_check(&self) -> bool {
        if self.validate_ws_url().is_err() {
            return false;
        }
        tokio_tungstenite::connect_async(&self.ws_url).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let toml_str = r#"ws_url = \"ws://127.0.0.1:9000/ws\""#;
        let config: ClawMaxConfig = toml::from_str(toml_str).unwrap();
        assert!(config.allowed_senders.is_empty());
    }

    #[test]
    fn test_parse_inbound_defaults() {
        let config = ClawMaxConfig {
            ws_url: "ws://localhost:9000/ws".into(),
            allowed_senders: vec!["*".into()],
        };
        let channel = ClawMaxChannel::new(config);
        let raw = serde_json::json!({
            "type": "message",
            "direction": "in",
            "message": {
                "sender": "user_a",
                "content": "hello"
            }
        });
        let msg = channel.parse_inbound_value(&raw).expect("message parsed");
        assert_eq!(msg.sender, "user_a");
        assert_eq!(msg.reply_target, "user_a");
        assert_eq!(msg.channel, "clawmax");
        assert_eq!(msg.content, "hello");
        assert!(!msg.id.is_empty());
        assert!(msg.timestamp > 0);
    }

    #[test]
    fn test_parse_inbound_respects_allowlist() {
        let config = ClawMaxConfig {
            ws_url: "ws://localhost:9000/ws".into(),
            allowed_senders: vec!["user_a".into()],
        };
        let channel = ClawMaxChannel::new(config);
        let raw = serde_json::json!({
            "type": "message",
            "direction": "in",
            "message": {
                "sender": "user_b",
                "content": "hello"
            }
        });
        assert!(channel.parse_inbound_value(&raw).is_none());
    }
}
