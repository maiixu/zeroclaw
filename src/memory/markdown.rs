use super::traits::{Memory, MemoryCategory, MemoryEntry};
use async_trait::async_trait;
use chrono::Local;
use std::path::{Path, PathBuf};
use tokio::fs;

/// Markdown-based memory — plain files as source of truth
///
/// Layout:
///   workspace/MEMORY.md          — curated long-term memory (core)
///   workspace/memory/YYYY-MM-DD.md — daily logs (append-only)
///   workspace/memory/archive/    — archived daily logs (cleanup mode: archive)
pub struct MarkdownMemory {
    workspace_dir: PathBuf,
    cleanup_config: Option<crate::config::schema::MarkdownCleanupConfig>,
}

impl MarkdownMemory {
    pub fn new(workspace_dir: &Path) -> Self {
        Self {
            workspace_dir: workspace_dir.to_path_buf(),
            cleanup_config: None,
        }
    }

    pub fn with_cleanup_config(
        workspace_dir: &Path,
        cleanup_config: crate::config::schema::MarkdownCleanupConfig,
    ) -> Self {
        Self {
            workspace_dir: workspace_dir.to_path_buf(),
            cleanup_config: Some(cleanup_config),
        }
    }

    fn memory_dir(&self) -> PathBuf {
        self.workspace_dir.join("memory")
    }

    fn archive_dir(&self) -> PathBuf {
        self.memory_dir().join("archive")
    }

    fn core_path(&self) -> PathBuf {
        self.workspace_dir.join("MEMORY.md")
    }

    fn daily_path(&self) -> PathBuf {
        let date = Local::now().format("%Y-%m-%d").to_string();
        self.memory_dir().join(format!("{date}.md"))
    }

    async fn ensure_dirs(&self) -> anyhow::Result<()> {
        fs::create_dir_all(self.memory_dir()).await?;
        Ok(())
    }

    async fn append_to_file(&self, path: &Path, content: &str) -> anyhow::Result<()> {
        self.ensure_dirs().await?;

        let existing = if path.exists() {
            fs::read_to_string(path).await.unwrap_or_default()
        } else {
            String::new()
        };

        let updated = if existing.is_empty() {
            let header = if path == self.core_path() {
                "# Long-Term Memory\n\n"
            } else {
                let date = Local::now().format("%Y-%m-%d").to_string();
                &format!("# Daily Log — {date}\n\n")
            };
            format!("{header}{content}\n")
        } else {
            format!("{existing}\n{content}\n")
        };

        fs::write(path, updated).await?;
        Ok(())
    }

    fn parse_entries_from_file(
        path: &Path,
        content: &str,
        category: &MemoryCategory,
    ) -> Vec<MemoryEntry> {
        let filename = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        content
            .lines()
            .filter(|line| {
                let trimmed = line.trim();
                !trimmed.is_empty() && !trimmed.starts_with('#')
            })
            .enumerate()
            .map(|(i, line)| {
                let trimmed = line.trim();
                let clean = trimmed.strip_prefix("- ").unwrap_or(trimmed);
                MemoryEntry {
                    id: format!("{filename}:{i}"),
                    key: format!("{filename}:{i}"),
                    content: clean.to_string(),
                    category: category.clone(),
                    timestamp: filename.to_string(),
                    session_id: None,
                    score: None,
                    namespace: "default".into(),
                    importance: None,
                    superseded_by: None,
                }
            })
            .collect()
    }

    async fn read_all_entries(&self) -> anyhow::Result<Vec<MemoryEntry>> {
        let mut entries = Vec::new();

        // Read MEMORY.md (core)
        let core_path = self.core_path();
        if core_path.exists() {
            let content = fs::read_to_string(&core_path).await?;
            entries.extend(Self::parse_entries_from_file(
                &core_path,
                &content,
                &MemoryCategory::Core,
            ));
        }

        // Read daily logs
        let mem_dir = self.memory_dir();
        if mem_dir.exists() {
            let mut dir = fs::read_dir(&mem_dir).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("md") {
                    let content = fs::read_to_string(&path).await?;
                    entries.extend(Self::parse_entries_from_file(
                        &path,
                        &content,
                        &MemoryCategory::Daily,
                    ));
                }
            }
        }

        entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(entries)
    }

    /// Perform automatic cleanup of Markdown memory files to prevent unbounded growth.
    ///
    /// Triggered on store() when files exceed configured size limits.
    /// Supports two modes:
    /// - "archive": Moves old entries to archive/, preserving data for recovery
    /// - "prune": Deletes oldest entries (aggressive, frees space immediately)
    async fn perform_cleanup(&self) -> anyhow::Result<()> {
        let config = match &self.cleanup_config {
            Some(cfg) => cfg,
            None => return Ok(()),
        };

        if !config.enabled {
            return Ok(());
        }

        tracing::debug!(
            "markdown_cleanup: starting cleanup (mode={}, core_max_mb={}, daily_max_mb={})",
            config.cleanup_mode,
            config.core_max_size_mb,
            config.daily_max_size_mb
        );

        // Check and cleanup core memory (MEMORY.md)
        let core_path = self.core_path();
        if core_path.exists() {
            let core_size_mb = Self::get_file_size_mb(&core_path).await?;
            if core_size_mb > config.core_max_size_mb {
                tracing::warn!(
                    "markdown_cleanup: core memory ({:.2} MB) exceeds limit ({} MB), cleaning up",
                    core_size_mb,
                    config.core_max_size_mb
                );
                self.cleanup_file(&core_path, config, "core").await?;
            }
        }

        // Check and cleanup daily logs
        let mem_dir = self.memory_dir();
        if mem_dir.exists() {
            let mut dir = fs::read_dir(&mem_dir).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                // Skip archive directory
                if path.file_name().and_then(|n| n.to_str()) == Some("archive") {
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) == Some("md") {
                    let daily_size_mb = Self::get_file_size_mb(&path).await?;
                    if daily_size_mb > config.daily_max_size_mb {
                        let filename = path
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or("unknown");
                        tracing::warn!(
                            "markdown_cleanup: daily log {} ({:.2} MB) exceeds limit ({} MB), cleaning up",
                            filename,
                            daily_size_mb,
                            config.daily_max_size_mb
                        );
                        self.cleanup_file(&path, config, filename).await?;
                    }
                }
            }
        }

        tracing::debug!("markdown_cleanup: cleanup completed successfully");
        Ok(())
    }

    /// Get file size in MB.
    async fn get_file_size_mb(path: &Path) -> anyhow::Result<u32> {
        let metadata = fs::metadata(path).await?;
        let size_bytes = metadata.len();
        #[allow(clippy::cast_possible_truncation)]
        let size_mb = (size_bytes / (1024 * 1024)) as u32;
        Ok(size_mb)
    }

    /// Cleanup a single file based on configured mode.
    async fn cleanup_file(
        &self,
        path: &Path,
        config: &crate::config::schema::MarkdownCleanupConfig,
        file_label: &str,
    ) -> anyhow::Result<()> {
        let content = fs::read_to_string(path).await?;
        let lines: Vec<&str> = content.lines().collect();

        // Separate header lines from entry lines
        let (header_lines, entry_lines): (Vec<_>, Vec<_>) = lines
            .iter()
            .partition(|line| line.starts_with('#') || line.trim().is_empty());

        if entry_lines.is_empty() {
            tracing::debug!(
                "markdown_cleanup: {} has no entries to clean, skipping",
                file_label
            );
            return Ok(());
        }

        let retention_cutoff =
            chrono::Local::now() - chrono::Duration::days(config.cleanup_retention_days as i64);

        match config.cleanup_mode.as_str() {
            "archive" => {
                self.cleanup_archive_mode(
                    path,
                    file_label,
                    &header_lines,
                    &entry_lines,
                    &retention_cutoff,
                )
                .await?;
            }
            "prune" => {
                self.cleanup_prune_mode(
                    path,
                    file_label,
                    &header_lines,
                    &entry_lines,
                    &retention_cutoff,
                )
                .await?;
            }
            mode => {
                tracing::warn!(
                    "markdown_cleanup: unknown cleanup mode '{}', defaulting to archive",
                    mode
                );
                self.cleanup_archive_mode(
                    path,
                    file_label,
                    &header_lines,
                    &entry_lines,
                    &retention_cutoff,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Archive mode: move old entries to archive/, preserve all data.
    async fn cleanup_archive_mode(
        &self,
        original_path: &Path,
        file_label: &str,
        header_lines: &[&str],
        entry_lines: &[&str],
        retention_cutoff: &chrono::DateTime<chrono::Local>,
    ) -> anyhow::Result<()> {
        self.ensure_dirs().await?;
        fs::create_dir_all(self.archive_dir()).await?;

        let mut recent_entries = Vec::new();
        let mut archived_entries = Vec::new();

        // Check if file has a date-like name (YYYY-MM-DD.md)
        let file_date_str = original_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");

        for entry_line in entry_lines {
            let should_archive = if let Ok(file_date) =
                chrono::NaiveDate::parse_from_str(file_date_str, "%Y-%m-%d")
            {
                let file_datetime = file_date.and_hms_opt(0, 0, 0).unwrap();
                let file_chrono = retention_cutoff.with_timezone(&chrono::Local).date_naive();
                file_datetime < file_chrono.and_hms_opt(0, 0, 0).unwrap()
            } else {
                false
            };

            if should_archive {
                archived_entries.push(*entry_line);
            } else {
                recent_entries.push(*entry_line);
            }
        }

        // Write archived entries to archive file
        let archived_count = archived_entries.len();
        if !archived_entries.is_empty() {
            let archive_filename = format!(
                "{}_archived_{}.md",
                file_label,
                chrono::Local::now().format("%s")
            );
            let archive_path = self.archive_dir().join(archive_filename);

            let mut archive_content = String::new();
            for header in header_lines {
                if !header.trim().is_empty() {
                    archive_content.push_str(header);
                    archive_content.push('\n');
                }
            }
            archive_content.push_str("# Archived Entries\n\n");
            for entry in &archived_entries {
                archive_content.push_str(entry);
                archive_content.push('\n');
            }

            fs::write(&archive_path, archive_content).await?;
            tracing::info!(
                "markdown_cleanup: archived {} old entries from {} to {}",
                archived_count,
                file_label,
                archive_path.display()
            );
        }

        // Rewrite original file with only recent entries
        let recent_count = recent_entries.len();
        if !recent_entries.is_empty() {
            let mut new_content = String::new();
            for header in header_lines {
                if !header.trim().is_empty() {
                    new_content.push_str(header);
                    new_content.push('\n');
                }
            }
            for entry in &recent_entries {
                new_content.push_str(entry);
                new_content.push('\n');
            }
            fs::write(original_path, new_content).await?;
            tracing::info!(
                "markdown_cleanup: retained {} recent entries in {}",
                recent_count,
                file_label
            );
        } else {
            // All entries were archived, clear the file
            let mut header_content = String::new();
            for header in header_lines {
                if !header.trim().is_empty() {
                    header_content.push_str(header);
                    header_content.push('\n');
                }
            }
            fs::write(original_path, header_content).await?;
            tracing::info!("markdown_cleanup: archived all entries from {}", file_label);
        }

        Ok(())
    }

    /// Prune mode: aggressively delete oldest entries, keep only recent data.
    async fn cleanup_prune_mode(
        &self,
        original_path: &Path,
        file_label: &str,
        header_lines: &[&str],
        entry_lines: &[&str],
        retention_cutoff: &chrono::DateTime<chrono::Local>,
    ) -> anyhow::Result<()> {
        let mut retained_entries = Vec::new();
        let mut pruned_count = 0;

        // Check if file has a date-like name (YYYY-MM-DD.md)
        let file_date_str = original_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");

        for entry_line in entry_lines {
            let should_prune = if let Ok(file_date) =
                chrono::NaiveDate::parse_from_str(file_date_str, "%Y-%m-%d")
            {
                let file_datetime = file_date.and_hms_opt(0, 0, 0).unwrap();
                let file_chrono = retention_cutoff.with_timezone(&chrono::Local).date_naive();
                file_datetime < file_chrono.and_hms_opt(0, 0, 0).unwrap()
            } else {
                false
            };

            if should_prune {
                pruned_count += 1;
            } else {
                retained_entries.push(*entry_line);
            }
        }

        // Rewrite file with retained entries only
        if !retained_entries.is_empty() {
            let mut new_content = String::new();
            for header in header_lines {
                if !header.trim().is_empty() {
                    new_content.push_str(header);
                    new_content.push('\n');
                }
            }
            for entry in retained_entries {
                new_content.push_str(entry);
                new_content.push('\n');
            }
            fs::write(original_path, new_content).await?;
        } else {
            // All entries pruned, keep only headers
            let mut header_content = String::new();
            for header in header_lines {
                if !header.trim().is_empty() {
                    header_content.push_str(header);
                    header_content.push('\n');
                }
            }
            fs::write(original_path, header_content).await?;
        }

        tracing::warn!(
            "markdown_cleanup: pruned {} old entries from {} (prune mode)",
            pruned_count,
            file_label
        );

        Ok(())
    }
}

#[async_trait]
impl Memory for MarkdownMemory {
    fn name(&self) -> &str {
        "markdown"
    }

    async fn store(
        &self,
        key: &str,
        content: &str,
        category: MemoryCategory,
        _session_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let entry = format!("- **{key}**: {content}");
        let path = match category {
            MemoryCategory::Core => self.core_path(),
            _ => self.daily_path(),
        };
        self.append_to_file(&path, &entry).await?;

        // Trigger automatic cleanup if configured
        if let Err(e) = self.perform_cleanup().await {
            tracing::error!("markdown_cleanup: failed to perform cleanup: {}", e);
            // Don't propagate cleanup errors; store succeeds even if cleanup fails
        }

        Ok(())
    }

    async fn recall(
        &self,
        query: &str,
        limit: usize,
        _session_id: Option<&str>,
        since: Option<&str>,
        until: Option<&str>,
    ) -> anyhow::Result<Vec<MemoryEntry>> {
        let since_dt = since
            .map(chrono::DateTime::parse_from_rfc3339)
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid 'since' date (expected RFC 3339): {e}"))?;
        let until_dt = until
            .map(chrono::DateTime::parse_from_rfc3339)
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid 'until' date (expected RFC 3339): {e}"))?;
        if let (Some(s), Some(u)) = (&since_dt, &until_dt) {
            if s >= u {
                anyhow::bail!("'since' must be before 'until'");
            }
        }

        let all = self.read_all_entries().await?;
        let query_lower = query.to_lowercase();
        let keywords: Vec<&str> = query_lower.split_whitespace().collect();

        let mut scored: Vec<MemoryEntry> = all
            .into_iter()
            .filter_map(|mut entry| {
                if let Some(ref s) = since_dt {
                    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(&entry.timestamp) {
                        if ts < *s {
                            return None;
                        }
                    }
                }
                if let Some(ref u) = until_dt {
                    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(&entry.timestamp) {
                        if ts > *u {
                            return None;
                        }
                    }
                }
                if keywords.is_empty() {
                    entry.score = Some(1.0);
                    return Some(entry);
                }
                let content_lower = entry.content.to_lowercase();
                let matched = keywords
                    .iter()
                    .filter(|kw| content_lower.contains(**kw))
                    .count();
                if matched > 0 {
                    #[allow(clippy::cast_precision_loss)]
                    let score = matched as f64 / keywords.len() as f64;
                    entry.score = Some(score);
                    Some(entry)
                } else {
                    None
                }
            })
            .collect();

        scored.sort_by(|a, b| {
            if keywords.is_empty() {
                b.timestamp.as_str().cmp(a.timestamp.as_str())
            } else {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        });
        scored.truncate(limit);
        Ok(scored)
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<MemoryEntry>> {
        let all = self.read_all_entries().await?;
        Ok(all
            .into_iter()
            .find(|e| e.key == key || e.content.contains(key)))
    }

    async fn list(
        &self,
        category: Option<&MemoryCategory>,
        _session_id: Option<&str>,
    ) -> anyhow::Result<Vec<MemoryEntry>> {
        let all = self.read_all_entries().await?;
        match category {
            Some(cat) => Ok(all.into_iter().filter(|e| &e.category == cat).collect()),
            None => Ok(all),
        }
    }

    async fn forget(&self, _key: &str) -> anyhow::Result<bool> {
        // Markdown memory is append-only by design (audit trail)
        // Return false to indicate the entry wasn't removed
        Ok(false)
    }

    async fn count(&self) -> anyhow::Result<usize> {
        let all = self.read_all_entries().await?;
        Ok(all.len())
    }

    async fn health_check(&self) -> bool {
        self.workspace_dir.exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_workspace() -> (TempDir, MarkdownMemory) {
        let tmp = TempDir::new().unwrap();
        let mem = MarkdownMemory::new(tmp.path());
        (tmp, mem)
    }

    #[tokio::test]
    async fn markdown_name() {
        let (_tmp, mem) = temp_workspace();
        assert_eq!(mem.name(), "markdown");
    }

    #[tokio::test]
    async fn markdown_health_check() {
        let (_tmp, mem) = temp_workspace();
        assert!(mem.health_check().await);
    }

    #[tokio::test]
    async fn markdown_store_core() {
        let (_tmp, mem) = temp_workspace();
        mem.store("pref", "User likes Rust", MemoryCategory::Core, None)
            .await
            .unwrap();
        let content = fs::read_to_string(mem.core_path()).await.unwrap();
        assert!(content.contains("User likes Rust"));
    }

    #[tokio::test]
    async fn markdown_store_daily() {
        let (_tmp, mem) = temp_workspace();
        mem.store("note", "Finished tests", MemoryCategory::Daily, None)
            .await
            .unwrap();
        let path = mem.daily_path();
        let content = fs::read_to_string(path).await.unwrap();
        assert!(content.contains("Finished tests"));
    }

    #[tokio::test]
    async fn markdown_recall_keyword() {
        let (_tmp, mem) = temp_workspace();
        mem.store("a", "Rust is fast", MemoryCategory::Core, None)
            .await
            .unwrap();
        mem.store("b", "Python is slow", MemoryCategory::Core, None)
            .await
            .unwrap();
        mem.store("c", "Rust and safety", MemoryCategory::Core, None)
            .await
            .unwrap();

        let results = mem.recall("Rust", 10, None, None, None).await.unwrap();
        assert!(results.len() >= 2);
        assert!(
            results
                .iter()
                .all(|r| r.content.to_lowercase().contains("rust"))
        );
    }

    #[tokio::test]
    async fn markdown_recall_no_match() {
        let (_tmp, mem) = temp_workspace();
        mem.store("a", "Rust is great", MemoryCategory::Core, None)
            .await
            .unwrap();
        let results = mem
            .recall("javascript", 10, None, None, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn markdown_count() {
        let (_tmp, mem) = temp_workspace();
        mem.store("a", "first", MemoryCategory::Core, None)
            .await
            .unwrap();
        mem.store("b", "second", MemoryCategory::Core, None)
            .await
            .unwrap();
        let count = mem.count().await.unwrap();
        assert!(count >= 2);
    }

    #[tokio::test]
    async fn markdown_list_by_category() {
        let (_tmp, mem) = temp_workspace();
        mem.store("a", "core fact", MemoryCategory::Core, None)
            .await
            .unwrap();
        mem.store("b", "daily note", MemoryCategory::Daily, None)
            .await
            .unwrap();

        let core = mem.list(Some(&MemoryCategory::Core), None).await.unwrap();
        assert!(core.iter().all(|e| e.category == MemoryCategory::Core));

        let daily = mem.list(Some(&MemoryCategory::Daily), None).await.unwrap();
        assert!(daily.iter().all(|e| e.category == MemoryCategory::Daily));
    }

    #[tokio::test]
    async fn markdown_forget_is_noop() {
        let (_tmp, mem) = temp_workspace();
        mem.store("a", "permanent", MemoryCategory::Core, None)
            .await
            .unwrap();
        let removed = mem.forget("a").await.unwrap();
        assert!(!removed, "Markdown memory is append-only");
    }

    #[tokio::test]
    async fn markdown_empty_recall() {
        let (_tmp, mem) = temp_workspace();
        let results = mem.recall("anything", 10, None, None, None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn markdown_empty_count() {
        let (_tmp, mem) = temp_workspace();
        assert_eq!(mem.count().await.unwrap(), 0);
    }
}
