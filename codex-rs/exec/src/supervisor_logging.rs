use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use codex_app_server_protocol::CommandExecutionStatus;
use codex_app_server_protocol::McpToolCallStatus;
use codex_app_server_protocol::PatchApplyStatus;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnStatus;
use codex_core::config::Config;
use codex_protocol::protocol::SessionConfiguredEvent;
use serde::Deserialize;
use serde::Serialize;

use crate::FollowupAction;

#[derive(Debug, Serialize, Clone)]
struct ConversationEntry {
    role: String,
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_type: Option<String>,
    timestamp: String,
}

#[derive(Debug, Serialize)]
struct StatusFile {
    status: String,
    instance_id: String,
    last_message_index: usize,
    timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct FinalResultFile {
    instance_id: String,
    status: String,
    started_at: String,
    completed_at: String,
    model: String,
    conversation: Vec<ConversationEntry>,
}

#[derive(Debug, Deserialize)]
struct FollowupInput {
    message: Option<String>,
    terminate: Option<bool>,
}

pub(crate) struct FileSessionLogger {
    realtime_path: PathBuf,
    final_result_path: PathBuf,
    status_path: PathBuf,
    followup_path: PathBuf,
    instance_id: String,
    started_at: String,
    model: String,
    mode: Option<String>,
    conversation: Vec<ConversationEntry>,
    completed_items: HashSet<String>,
}

impl FileSessionLogger {
    pub(crate) fn maybe_new(
        dir: Option<PathBuf>,
        instance_id: Option<String>,
        model: Option<String>,
        mode: Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        let Some(dir) = dir else {
            return Ok(None);
        };
        let dir = std::path::absolute(dir)?;
        std::fs::create_dir_all(&dir)?;
        let instance_id = instance_id.unwrap_or_else(|| "codex-worker".to_string());
        let realtime_path = dir.join("realtime_context.txt");
        let final_result_path = dir.join("final_result.json");
        let status_path = dir.join("status.json");
        let followup_path = dir.join("followup_input.json");
        let logger = Self {
            realtime_path,
            final_result_path,
            status_path,
            followup_path,
            instance_id,
            started_at: now_rfc3339(),
            model: model.unwrap_or_else(|| "unknown".to_string()),
            mode,
            conversation: Vec::new(),
            completed_items: HashSet::new(),
        };
        logger.append_realtime("worker logger initialized")?;
        logger.write_status("running")?;
        Ok(Some(logger))
    }

    pub(crate) fn log_initial(
        &mut self,
        config: &Config,
        prompt: &str,
        session_configured: &SessionConfiguredEvent,
    ) -> anyhow::Result<()> {
        self.model = session_configured.model.clone();
        self.append_realtime(&format!("instance_id: {}", self.instance_id))?;
        self.append_realtime(&format!("workdir: {}", config.cwd.display()))?;
        self.append_realtime(&format!("model: {}", session_configured.model))?;
        self.append_realtime(&format!(
            "provider: {}",
            session_configured.model_provider_id
        ))?;
        if let Some(mode) = self.mode.clone() {
            self.record("system", "mode", format!("Supervisor mode: {mode}"))?;
        }
        self.record("user", "initial_prompt", prompt.to_string())?;
        Ok(())
    }

    pub(crate) fn process_server_notification(
        &mut self,
        notification: &ServerNotification,
    ) -> anyhow::Result<()> {
        match notification {
            ServerNotification::ConfigWarning(notification) => {
                self.record("system", "config_warning", notification.summary.clone())?;
            }
            ServerNotification::DeprecationNotice(notification) => {
                self.record("system", "deprecation_notice", notification.summary.clone())?;
            }
            ServerNotification::Error(notification) => {
                self.record("system", "error", notification.error.to_string())?;
            }
            ServerNotification::HookStarted(notification) => {
                self.record(
                    "system",
                    "hook_started",
                    format!("{:?}", notification.run.event_name),
                )?;
            }
            ServerNotification::HookCompleted(notification) => {
                self.record(
                    "system",
                    "hook_completed",
                    format!(
                        "{:?} {:?}",
                        notification.run.event_name, notification.run.status
                    ),
                )?;
            }
            ServerNotification::ItemStarted(notification) => {
                self.log_item_started(&notification.item)?;
            }
            ServerNotification::ItemCompleted(notification) => {
                self.log_item_completed(&notification.item)?;
            }
            ServerNotification::TurnPlanUpdated(notification) => {
                let mut lines = Vec::new();
                if let Some(explanation) = &notification.explanation {
                    lines.push(explanation.clone());
                }
                lines.extend(
                    notification
                        .plan
                        .iter()
                        .map(|step| format!("{:?}: {}", step.status, step.step)),
                );
                if !lines.is_empty() {
                    self.record("system", "plan_update", lines.join("\n"))?;
                }
            }
            ServerNotification::TurnDiffUpdated(notification) => {
                if !notification.diff.trim().is_empty() {
                    self.record("system", "turn_diff", notification.diff.clone())?;
                }
            }
            ServerNotification::TurnStarted(notification) => {
                self.write_status("running")?;
                self.record(
                    "system",
                    "turn_started",
                    format!("turn {}", notification.turn.id),
                )?;
            }
            ServerNotification::TurnCompleted(notification) => {
                let status = match notification.turn.status {
                    TurnStatus::Completed => "completed",
                    TurnStatus::Failed => "failed",
                    TurnStatus::Interrupted => "interrupted",
                    TurnStatus::InProgress => "in_progress",
                };
                self.record(
                    "system",
                    "turn_completed",
                    format!("turn {} {status}", notification.turn.id),
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn log_warning(&mut self, event_type: &str, message: &str) -> anyhow::Result<()> {
        self.record("system", event_type, message.to_string())
    }

    pub(crate) fn log_followup(&mut self, message: &str) -> anyhow::Result<()> {
        self.record("user", "followup", message.to_string())
    }

    pub(crate) fn write_status(&self, status: &str) -> anyhow::Result<()> {
        let pid = std::fs::read_to_string(&self.status_path)
            .ok()
            .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
            .and_then(|value| value.get("pid").cloned());
        let file = StatusFile {
            status: status.to_string(),
            instance_id: self.instance_id.clone(),
            last_message_index: self.conversation.len(),
            timestamp: now_rfc3339(),
            pid,
        };
        write_json_atomic(&self.status_path, &file)
    }

    pub(crate) fn write_final_result(&self, status: &str) -> anyhow::Result<()> {
        let file = FinalResultFile {
            instance_id: self.instance_id.clone(),
            status: status.to_string(),
            started_at: self.started_at.clone(),
            completed_at: now_rfc3339(),
            model: self.model.clone(),
            conversation: self.conversation.clone(),
        };
        write_json_atomic(&self.final_result_path, &file)?;
        self.write_status(status)
    }

    pub(crate) fn take_followup_input(&self) -> anyhow::Result<Option<FollowupAction>> {
        if !self.followup_path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&self.followup_path).unwrap_or_default();
        let _ = std::fs::remove_file(&self.followup_path);
        if text.trim().is_empty() {
            return Ok(Some(FollowupAction::Terminate));
        }
        let input: FollowupInput = serde_json::from_str(&text)?;
        if input.terminate.unwrap_or(false) {
            return Ok(Some(FollowupAction::Terminate));
        }
        match input.message.map(|message| message.trim().to_string()) {
            Some(message) if !message.is_empty() => Ok(Some(FollowupAction::Message(message))),
            _ => Ok(Some(FollowupAction::Terminate)),
        }
    }

    fn log_item_started(&mut self, item: &ThreadItem) -> anyhow::Result<()> {
        match item {
            ThreadItem::CommandExecution { command, cwd, .. } => self.record(
                "system",
                "exec_command_begin",
                format!("command: {command}\ncwd: {}", cwd.display()),
            ),
            ThreadItem::McpToolCall {
                server,
                tool,
                arguments,
                ..
            } => self.record(
                "system",
                "mcp_tool_begin",
                format!("{server}/{tool}\narguments: {arguments}"),
            ),
            ThreadItem::FileChange { changes, .. } => self.record(
                "system",
                "file_change_begin",
                changes
                    .iter()
                    .map(|change| change.path.clone())
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            ThreadItem::WebSearch { query, .. } => {
                self.record("system", "web_search_begin", query.clone())
            }
            ThreadItem::DynamicToolCall {
                namespace,
                tool,
                arguments,
                ..
            } => self.record(
                "system",
                "dynamic_tool_begin",
                format!(
                    "{}/{}\narguments: {arguments}",
                    namespace.as_deref().unwrap_or(""),
                    tool
                ),
            ),
            _ => Ok(()),
        }
    }

    fn log_item_completed(&mut self, item: &ThreadItem) -> anyhow::Result<()> {
        if !self.completed_items.insert(item.id().to_string()) {
            return Ok(());
        }
        match item {
            ThreadItem::UserMessage { content, .. } => self.record(
                "user",
                "user_message",
                serde_json::to_string(content).unwrap_or_else(|_| "<unserializable>".to_string()),
            ),
            ThreadItem::AgentMessage { text, .. } => {
                self.record("assistant", "assistant_message", text.clone())
            }
            ThreadItem::Plan { text, .. } => self.record("assistant", "plan", text.clone()),
            ThreadItem::Reasoning {
                summary, content, ..
            } => {
                let text = if summary.is_empty() {
                    content.join("\n")
                } else {
                    summary.join("\n")
                };
                if text.trim().is_empty() {
                    Ok(())
                } else {
                    self.record("system", "reasoning", text)
                }
            }
            ThreadItem::CommandExecution {
                command,
                cwd,
                status,
                aggregated_output,
                exit_code,
                duration_ms,
                ..
            } => {
                let status = match status {
                    CommandExecutionStatus::Completed => "completed",
                    CommandExecutionStatus::Failed => "failed",
                    CommandExecutionStatus::Declined => "declined",
                    CommandExecutionStatus::InProgress => "in_progress",
                };
                self.record(
                    "system",
                    "exec_command_end",
                    format!(
                        "command: {command}\ncwd: {}\nstatus: {status}\nexit_code: {}\nduration_ms: {}\noutput:\n{}",
                        cwd.display(),
                        exit_code
                            .map(|code| code.to_string())
                            .unwrap_or_else(|| "null".to_string()),
                        duration_ms
                            .map(|duration| duration.to_string())
                            .unwrap_or_else(|| "null".to_string()),
                        aggregated_output.as_deref().unwrap_or("")
                    ),
                )
            }
            ThreadItem::FileChange {
                changes, status, ..
            } => {
                let status = match status {
                    PatchApplyStatus::Completed => "completed",
                    PatchApplyStatus::Failed => "failed",
                    PatchApplyStatus::Declined => "declined",
                    PatchApplyStatus::InProgress => "in_progress",
                };
                self.record(
                    "system",
                    "file_change_end",
                    format!(
                        "status: {status}\n{}",
                        changes
                            .iter()
                            .map(|change| change.path.clone())
                            .collect::<Vec<_>>()
                            .join("\n")
                    ),
                )
            }
            ThreadItem::McpToolCall {
                server,
                tool,
                status,
                result,
                error,
                duration_ms,
                ..
            } => {
                let status = match status {
                    McpToolCallStatus::Completed => "completed",
                    McpToolCallStatus::Failed => "failed",
                    McpToolCallStatus::InProgress => "in_progress",
                };
                self.record(
                    "system",
                    "mcp_tool_end",
                    format!(
                        "{server}/{tool}\nstatus: {status}\nduration_ms: {}\nresult: {}\nerror: {}",
                        duration_ms
                            .map(|duration| duration.to_string())
                            .unwrap_or_else(|| "null".to_string()),
                        result
                            .as_ref()
                            .and_then(|result| serde_json::to_string(result).ok())
                            .unwrap_or_else(|| "null".to_string()),
                        error
                            .as_ref()
                            .map(|error| error.message.clone())
                            .unwrap_or_else(|| "null".to_string())
                    ),
                )
            }
            ThreadItem::WebSearch { query, .. } => {
                self.record("system", "web_search_end", query.clone())
            }
            ThreadItem::ContextCompaction { .. } => self.record(
                "system",
                "context_compaction",
                "context compacted".to_string(),
            ),
            _ => Ok(()),
        }
    }

    fn record(&mut self, role: &str, event_type: &str, content: String) -> anyhow::Result<()> {
        let timestamp = now_rfc3339();
        self.append_realtime(&format!("[{role}] {event_type}\n{content}"))?;
        self.conversation.push(ConversationEntry {
            role: role.to_string(),
            content,
            event_type: Some(event_type.to_string()),
            timestamp,
        });
        self.write_status("running")
    }

    fn append_realtime(&self, text: &str) -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.realtime_path)?;
        writeln!(file, "\n--- {} ---", now_rfc3339())?;
        writeln!(file, "{text}")?;
        Ok(())
    }
}

fn now_rfc3339() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "unix_ms:{}.{:03}",
        duration.as_secs(),
        duration.subsec_millis()
    )
}

fn write_json_atomic<T: Serialize>(path: &PathBuf, value: &T) -> anyhow::Result<()> {
    let tmp_path = path.with_extension("json.tmp");
    {
        let mut file = File::create(&tmp_path)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        writeln!(file)?;
    }
    std::fs::rename(tmp_path, path)?;
    Ok(())
}
