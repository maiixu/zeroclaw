use crate::channels::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::mpsc;

/// Call `gws` CLI via `zsh -lc` and return parsed JSON output.
/// `params` = URL query params (--params), `body` = request body (--json).
fn gws_call(
    subcmd: &str,
    params: Option<serde_json::Value>,
    body: Option<serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let params_json = params
        .map(|p| serde_json::to_string(&p))
        .transpose()?;
    let body_json = body
        .map(|b| serde_json::to_string(&b))
        .transpose()?;

    let mut cmd = format!("gws {subcmd}");
    if params_json.is_some() {
        cmd.push_str(" --params \"$GWS_PARAMS\"");
    }
    if body_json.is_some() {
        cmd.push_str(" --json \"$GWS_JSON\"");
    }

    let mut command = std::process::Command::new("zsh");
    command.arg("-lc").arg(&cmd);
    if let Some(ref json) = params_json {
        command.env("GWS_PARAMS", json);
    }
    if let Some(ref json) = body_json {
        command.env("GWS_JSON", json);
    }

    let output = command.output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("gws error: {stderr}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        return Ok(serde_json::Value::Object(Default::default()));
    }
    Ok(serde_json::from_str(&stdout)?)
}

#[derive(Debug, Deserialize)]
struct GChatMessage {
    name: String,
    #[serde(rename = "createTime")]
    create_time: String,
    text: Option<String>,
    sender: Option<GChatSender>,
}

#[derive(Debug, Deserialize)]
struct GChatSender {
    name: Option<String>,
    #[serde(rename = "displayName")]
    display_name: Option<String>,
    #[serde(rename = "type")]
    sender_type: Option<String>,
}

/// Google Chat channel — polls a space via `gws` CLI.
#[derive(Clone)]
pub struct GChatChannel {
    space_id: String,
    allowed_senders: Vec<String>,
    bot_sender_id: Option<String>,
    poll_interval_secs: u64,
}

impl GChatChannel {
    pub fn new(
        space_id: String,
        allowed_senders: Vec<String>,
        bot_sender_id: Option<String>,
        poll_interval_secs: Option<u64>,
    ) -> Self {
        Self {
            space_id,
            allowed_senders,
            bot_sender_id,
            poll_interval_secs: poll_interval_secs.unwrap_or(8),
        }
    }

    fn is_sender_allowed(&self, sender_id: &str) -> bool {
        self.allowed_senders.iter().any(|s| s == sender_id)
    }
}

#[async_trait]
impl Channel for GChatChannel {
    fn name(&self) -> &str {
        "gchat"
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        let space_id = if message.recipient.starts_with("spaces/") {
            message.recipient.clone()
        } else {
            self.space_id.clone()
        };

        gws_call(
            "chat spaces messages create",
            Some(serde_json::json!({ "parent": space_id })),
            Some(serde_json::json!({ "text": message.content })),
        )?;

        Ok(())
    }

    async fn listen(&self, tx: mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        tracing::info!(space_id = %self.space_id, "Google Chat channel listening...");

        // Start 60s in the past on first poll
        let mut last_timestamp = chrono::Utc::now() - chrono::Duration::seconds(60);
        // Track processed message names to prevent reprocessing within the same second
        let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();

        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(self.poll_interval_secs)).await;

            // GChat API filter doesn't support sub-seconds; use last_timestamp truncated to
            // whole seconds. Dedup via seen_names handles messages within the same second.
            let since = last_timestamp.format("%Y-%m-%dT%H:%M:%SZ").to_string();

            let space_id = self.space_id.clone();
            let result = tokio::task::spawn_blocking(move || {
                gws_call(
                    "chat spaces messages list",
                    Some(serde_json::json!({
                        "parent": space_id,
                        "filter": format!("createTime > \"{since}\""),
                    })),
                    None,
                )
            })
            .await
            .map_err(|e| anyhow::anyhow!("gchat poll join error: {e}"))??;

            let messages: Vec<GChatMessage> = result
                .get("messages")
                .and_then(|m| serde_json::from_value(m.clone()).ok())
                .unwrap_or_default();

            // Process chronologically
            let mut sorted = messages;
            sorted.sort_by(|a, b| a.create_time.cmp(&b.create_time));

            for msg in sorted {
                // Skip already-processed messages (dedup within the same second)
                if seen_names.contains(&msg.name) {
                    continue;
                }

                // Skip bots
                if msg
                    .sender
                    .as_ref()
                    .and_then(|s| s.sender_type.as_deref())
                    == Some("BOT")
                {
                    continue;
                }

                let sender_id = msg
                    .sender
                    .as_ref()
                    .and_then(|s| s.name.as_deref())
                    .unwrap_or("")
                    .to_string();

                // Skip own bot messages
                if let Some(ref bot_id) = self.bot_sender_id {
                    if &sender_id == bot_id {
                        continue;
                    }
                }

                if !self.is_sender_allowed(&sender_id) {
                    continue;
                }

                let text = msg.text.unwrap_or_default();
                if text.trim().is_empty() {
                    continue;
                }

                // Track name and advance timestamp
                seen_names.insert(msg.name.clone());
                // Bound set size to prevent unbounded growth
                if seen_names.len() > 500 {
                    seen_names.clear();
                }
                if let Ok(t) = chrono::DateTime::parse_from_rfc3339(&msg.create_time) {
                    let t_utc = t.with_timezone(&chrono::Utc);
                    if t_utc > last_timestamp {
                        last_timestamp = t_utc;
                    }
                }

                let display_name = msg
                    .sender
                    .as_ref()
                    .and_then(|s| s.display_name.as_deref())
                    .unwrap_or(&sender_id)
                    .to_string();

                let channel_msg = ChannelMessage {
                    id: msg.name.clone(),
                    sender: display_name,
                    reply_target: self.space_id.clone(),
                    content: text,
                    channel: "gchat".to_string(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    thread_ts: None,
                    interruption_scope_id: None,
                    attachments: vec![],
                };

                if tx.send(channel_msg).await.is_err() {
                    return Ok(());
                }
            }
        }
    }

    async fn health_check(&self) -> bool {
        tokio::task::spawn_blocking(|| {
            std::process::Command::new("zsh")
                .arg("-lc")
                .arg("gws --version")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        })
        .await
        .unwrap_or(false)
    }
}
