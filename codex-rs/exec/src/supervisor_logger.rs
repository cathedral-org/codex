use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use codex_app_server_protocol::CommandExecutionStatus;
use codex_app_server_protocol::McpToolCallStatus;
use codex_app_server_protocol::PatchApplyStatus;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_core::config::Config;
use codex_protocol::protocol::SessionConfiguredEvent;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Instant;

#[derive(Debug, Clone, Serialize)]
struct ConversationEntry {
    role: String,
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_type: Option<String>,
    timestamp: String,
}

#[derive(Debug, Serialize)]
struct WorkerStatus {
    status: String,
    instance_id: String,
    last_message_index: usize,
    timestamp: String,
}

#[derive(Debug, Serialize)]
struct FinalResult {
    instance_id: String,
    status: String,
    started_at: String,
    completed_at: String,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
    conversation: Vec<ConversationEntry>,
}

#[derive(Debug, Deserialize)]
struct FollowupInput {
    message: Option<String>,
    terminate: Option<bool>,
}

#[derive(Debug)]
pub(crate) enum FollowupDecision {
    Continue(String),
    Terminate,
    Timeout,
}

pub(crate) struct SupervisorLogger {
    instance_id: String,
    mode: Option<String>,
    model: String,
    started_at: String,
    realtime_path: PathBuf,
    status_path: PathBuf,
    final_result_path: PathBuf,
    followup_path: PathBuf,
    conversation: Vec<ConversationEntry>,
    completed_item_ids: HashSet<String>,
    message_fingerprints: HashSet<(String, String)>,
}

impl SupervisorLogger {
    pub(crate) fn new(
        dir: PathBuf,
        instance_id: String,
        mode: Option<String>,
        model: String,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let realtime_path = dir.join("realtime_context.txt");
        let status_path = dir.join("status.json");
        let final_result_path = dir.join("final_result.json");
        let followup_path = dir.join("followup_input.json");

        File::create(&realtime_path)?;
        if final_result_path.exists() {
            std::fs::remove_file(&final_result_path)?;
        }

        let logger = Self {
            instance_id,
            mode,
            model,
            started_at: now_timestamp(),
            realtime_path,
            status_path,
            final_result_path,
            followup_path,
            conversation: Vec::new(),
            completed_item_ids: HashSet::new(),
            message_fingerprints: HashSet::new(),
        };
        logger.append_realtime("worker log initialized")?;
        logger.write_status("running")?;
        Ok(logger)
    }

    pub(crate) fn log_initial_prompt(
        &mut self,
        config: &Config,
        prompt: &str,
        session_configured: &SessionConfiguredEvent,
    ) -> anyhow::Result<()> {
        self.model = session_configured.model.clone();
        self.record_system_event(
            "session_configured",
            format!(
                "workdir: {}\nmodel: {}\nprovider: {}\napproval: {}\nmode: {}",
                config.cwd.display(),
                session_configured.model,
                session_configured.model_provider_id,
                config.permissions.approval_policy.value(),
                self.mode.as_deref().unwrap_or("default")
            ),
        )?;
        self.record_user_message(prompt.to_string())?;
        Ok(())
    }

    pub(crate) fn record_user_message(&mut self, message: String) -> anyhow::Result<()> {
        self.push_entry("user", None, message)
    }

    pub(crate) fn record_system_event(
        &mut self,
        event_type: &str,
        content: String,
    ) -> anyhow::Result<()> {
        self.push_entry("system", Some(event_type.to_string()), content)
    }

    pub(crate) fn mark_running(&mut self) -> anyhow::Result<()> {
        self.write_status("running")
    }

    pub(crate) fn mark_waiting_for_followup(&mut self) -> anyhow::Result<()> {
        self.record_system_event(
            "waiting_for_followup",
            format!(
                "waiting for followup input at {}",
                self.followup_path.display()
            ),
        )?;
        self.write_status("waiting_for_followup")
    }

    pub(crate) fn mark_completed(&mut self) -> anyhow::Result<()> {
        self.write_status("completed")?;
        self.write_final_result("completed")
    }

    pub(crate) fn mark_failed(&mut self) -> anyhow::Result<()> {
        self.write_status("failed")?;
        self.write_final_result("failed")
    }

    pub(crate) fn followup_path(&self) -> &Path {
        &self.followup_path
    }

    pub(crate) fn process_notification(
        &mut self,
        notification: &ServerNotification,
    ) -> anyhow::Result<()> {
        match notification {
            ServerNotification::ConfigWarning(notification) => {
                self.record_system_event(
                    "config_warning",
                    notification
                        .details
                        .as_ref()
                        .map(|details| format!("{} ({details})", notification.summary))
                        .unwrap_or_else(|| notification.summary.clone()),
                )?;
            }
            ServerNotification::DeprecationNotice(notification) => {
                self.record_system_event("deprecation_notice", notification.summary.clone())?;
            }
            ServerNotification::Error(notification) => {
                self.record_system_event(
                    "error",
                    format!(
                        "{}\nwill_retry: {}",
                        notification.error, notification.will_retry
                    ),
                )?;
            }
            ServerNotification::TurnStarted(notification) => {
                self.write_status("running")?;
                self.record_system_event(
                    "turn_started",
                    format!("turn_id: {}", notification.turn.id),
                )?;
            }
            ServerNotification::ItemStarted(notification) => {
                if let Some((event_type, content)) = describe_item("begin", &notification.item) {
                    self.record_system_event(&event_type, content)?;
                }
            }
            ServerNotification::ItemCompleted(notification) => {
                self.record_completed_item(&notification.item)?;
            }
            ServerNotification::HookStarted(notification) => {
                self.record_system_event(
                    "hook_started",
                    format!("{:?}", notification.run.event_name),
                )?;
            }
            ServerNotification::HookCompleted(notification) => {
                self.record_system_event(
                    "hook_completed",
                    format!(
                        "{:?}: {:?}",
                        notification.run.event_name, notification.run.status
                    ),
                )?;
            }
            ServerNotification::TurnDiffUpdated(notification) => {
                if !notification.diff.trim().is_empty() {
                    self.record_system_event("diff_updated", notification.diff.clone())?;
                }
            }
            ServerNotification::TurnPlanUpdated(notification) => {
                let mut content = String::new();
                if let Some(explanation) = &notification.explanation {
                    content.push_str(explanation);
                    content.push('\n');
                }
                for step in &notification.plan {
                    content.push_str(&format!("{:?}: {}\n", step.status, step.step));
                }
                if !content.trim().is_empty() {
                    self.record_system_event("plan_updated", content)?;
                }
            }
            ServerNotification::TurnCompleted(notification) => {
                for item in &notification.turn.items {
                    self.record_completed_item(item)?;
                }
                let status = match notification.turn.status {
                    TurnStatus::Completed => "completed",
                    TurnStatus::Failed => "failed",
                    TurnStatus::Interrupted => "interrupted",
                    TurnStatus::InProgress => "in_progress",
                };
                let mut content = format!("turn_id: {}\nstatus: {status}", notification.turn.id);
                if let Some(error) = &notification.turn.error {
                    content.push_str(&format!("\nerror: {error}"));
                }
                self.record_system_event("turn_completed", content)?;
                match notification.turn.status {
                    TurnStatus::Failed | TurnStatus::Interrupted => self.write_status("failed")?,
                    TurnStatus::Completed | TurnStatus::InProgress => {}
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn record_completed_item(&mut self, item: &ThreadItem) -> anyhow::Result<()> {
        let item_id = item.id().to_string();
        if !self.completed_item_ids.insert(item_id) {
            return Ok(());
        }

        match item {
            ThreadItem::UserMessage { content, .. } => {
                self.record_user_message(render_user_inputs(content))?;
            }
            ThreadItem::AgentMessage { text, .. } => {
                self.push_entry("assistant", None, text.clone())?;
            }
            ThreadItem::Reasoning {
                summary, content, ..
            } => {
                let text = if !summary.is_empty() {
                    summary.join("\n")
                } else {
                    content.join("\n")
                };
                if !text.trim().is_empty() {
                    self.record_system_event("reasoning", text)?;
                }
            }
            _ => {
                if let Some((event_type, content)) = describe_item("end", item) {
                    self.record_system_event(&event_type, content)?;
                }
            }
        }
        Ok(())
    }

    fn push_entry(
        &mut self,
        role: &str,
        event_type: Option<String>,
        content: String,
    ) -> anyhow::Result<()> {
        if event_type.is_none()
            && matches!(role, "user" | "assistant")
            && !self
                .message_fingerprints
                .insert((role.to_string(), content.clone()))
        {
            return Ok(());
        }

        let entry = ConversationEntry {
            role: role.to_string(),
            content,
            event_type,
            timestamp: now_timestamp(),
        };
        self.append_realtime_entry(&entry)?;
        self.conversation.push(entry);
        self.write_status_snapshot_if_exists()?;
        Ok(())
    }

    fn append_realtime_entry(&self, entry: &ConversationEntry) -> anyhow::Result<()> {
        let label = entry
            .event_type
            .as_ref()
            .map(|event_type| format!("{}:{event_type}", entry.role))
            .unwrap_or_else(|| entry.role.clone());
        self.append_realtime(&format!("[{}] {label}\n{}", entry.timestamp, entry.content))
    }

    fn append_realtime(&self, text: &str) -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.realtime_path)?;
        writeln!(file, "{text}\n")?;
        Ok(())
    }

    fn write_status(&self, status: &str) -> anyhow::Result<()> {
        let payload = WorkerStatus {
            status: status.to_string(),
            instance_id: self.instance_id.clone(),
            last_message_index: self.conversation.len(),
            timestamp: now_timestamp(),
        };
        write_json_atomic(&self.status_path, &payload)
    }

    fn write_status_snapshot_if_exists(&self) -> anyhow::Result<()> {
        if self.status_path.exists() {
            let raw = std::fs::read_to_string(&self.status_path)?;
            let status = serde_json::from_str::<serde_json::Value>(&raw)
                .ok()
                .and_then(|value| {
                    value
                        .get("status")
                        .and_then(|status| status.as_str())
                        .map(str::to_string)
                })
                .unwrap_or_else(|| "running".to_string());
            self.write_status(&status)?;
        }
        Ok(())
    }

    fn write_final_result(&self, status: &str) -> anyhow::Result<()> {
        let payload = FinalResult {
            instance_id: self.instance_id.clone(),
            status: status.to_string(),
            started_at: self.started_at.clone(),
            completed_at: now_timestamp(),
            model: self.model.clone(),
            mode: self.mode.clone(),
            conversation: self.conversation.clone(),
        };
        write_json_atomic(&self.final_result_path, &payload)
    }
}

pub(crate) async fn wait_for_followup(
    logger: &mut SupervisorLogger,
    timeout: Duration,
) -> anyhow::Result<FollowupDecision> {
    logger.mark_waiting_for_followup()?;
    let deadline = Instant::now() + timeout;
    loop {
        if logger.followup_path().exists() {
            let raw = tokio::fs::read_to_string(logger.followup_path()).await?;
            let _ = tokio::fs::remove_file(logger.followup_path()).await;
            if raw.trim().is_empty() {
                logger.record_system_event("followup_terminate", "empty followup input".into())?;
                return Ok(FollowupDecision::Terminate);
            }
            let input: FollowupInput = serde_json::from_str(&raw)?;
            if input.terminate.unwrap_or(false) {
                logger.record_system_event(
                    "followup_terminate",
                    "terminate requested by supervisor".into(),
                )?;
                return Ok(FollowupDecision::Terminate);
            }
            if let Some(message) = input.message
                && !message.trim().is_empty()
            {
                logger.record_system_event("followup_received", message.clone())?;
                return Ok(FollowupDecision::Continue(message));
            }
            logger.record_system_event("followup_terminate", "blank followup message".into())?;
            return Ok(FollowupDecision::Terminate);
        }

        if Instant::now() >= deadline {
            logger.record_system_event("followup_timeout", "followup wait timed out".into())?;
            return Ok(FollowupDecision::Timeout);
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn describe_item(prefix: &str, item: &ThreadItem) -> Option<(String, String)> {
    match item {
        ThreadItem::CommandExecution {
            command,
            cwd,
            status,
            aggregated_output,
            exit_code,
            duration_ms,
            ..
        } => {
            let event_type = match prefix {
                "begin" => "exec_command_begin",
                _ => "exec_command_end",
            };
            let mut content = format!(
                "command: {command}\ncwd: {}\nstatus: {}",
                cwd.display(),
                command_status_label(status)
            );
            if let Some(exit_code) = exit_code {
                content.push_str(&format!("\nexit_code: {exit_code}"));
            }
            if let Some(duration_ms) = duration_ms {
                content.push_str(&format!("\nduration_ms: {duration_ms}"));
            }
            if let Some(output) = aggregated_output
                && !output.trim().is_empty()
            {
                content.push_str("\noutput:\n");
                content.push_str(output);
            }
            Some((event_type.to_string(), content))
        }
        ThreadItem::FileChange {
            changes, status, ..
        } => {
            let mut content = format!("status: {}", patch_status_label(status));
            for change in changes {
                content.push_str(&format!("\n{} {:?}", change.path, change.kind));
            }
            Some(("file_change".to_string(), content))
        }
        ThreadItem::McpToolCall {
            server,
            tool,
            status,
            error,
            result,
            ..
        } => {
            let mut content = format!("{server}/{tool}\nstatus: {}", mcp_status_label(status));
            if let Some(error) = error {
                content.push_str(&format!("\nerror: {}", error.message));
            }
            if let Some(result) = result {
                content.push_str(&format!("\nresult: {:?}", result.content));
            }
            Some(("mcp_tool_call".to_string(), content))
        }
        ThreadItem::WebSearch { query, .. } => Some(("web_search".to_string(), query.clone())),
        ThreadItem::Plan { text, .. } => Some(("plan".to_string(), text.clone())),
        ThreadItem::ContextCompaction { .. } => Some((
            "context_compaction".to_string(),
            "context compacted".to_string(),
        )),
        _ => None,
    }
}

fn render_user_inputs(inputs: &[UserInput]) -> String {
    inputs
        .iter()
        .filter_map(|input| match input {
            UserInput::Text { text, .. } => Some(text.clone()),
            UserInput::LocalImage { path } => Some(format!("[local image: {}]", path.display())),
            UserInput::Image { url } => Some(format!("[image: {url}]")),
            UserInput::Skill { name, path } => {
                Some(format!("[skill: {name} at {}]", path.display()))
            }
            UserInput::Mention { name, path } => Some(format!("[mention: {name} at {path}]")),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn command_status_label(status: &CommandExecutionStatus) -> &'static str {
    match status {
        CommandExecutionStatus::InProgress => "in_progress",
        CommandExecutionStatus::Completed => "completed",
        CommandExecutionStatus::Failed => "failed",
        CommandExecutionStatus::Declined => "declined",
    }
}

fn patch_status_label(status: &PatchApplyStatus) -> &'static str {
    match status {
        PatchApplyStatus::InProgress => "in_progress",
        PatchApplyStatus::Completed => "completed",
        PatchApplyStatus::Failed => "failed",
        PatchApplyStatus::Declined => "declined",
    }
}

fn mcp_status_label(status: &McpToolCallStatus) -> &'static str {
    match status {
        McpToolCallStatus::InProgress => "in_progress",
        McpToolCallStatus::Completed => "completed",
        McpToolCallStatus::Failed => "failed",
    }
}

fn write_json_atomic<T: Serialize>(path: &Path, payload: &T) -> anyhow::Result<()> {
    let tmp_path = path.with_extension("tmp");
    let json = serde_json::to_string_pretty(payload)?;
    std::fs::write(&tmp_path, json)?;
    std::fs::rename(tmp_path, path)?;
    Ok(())
}

fn now_timestamp() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("unix:{}", duration.as_secs())
}
