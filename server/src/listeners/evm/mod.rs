pub(crate) mod types;

use crossbeam_channel::{Sender, unbounded};
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};
use tokio::sync::broadcast;

use self::types::EvmLine;

/// Broadcast message sent to all WebSocket clients
pub(crate) struct EvmBroadcastMessage {
    /// Standard newHeads JSON
    pub header: serde_json::Value,
    /// Standard log format (all logs in this block)
    pub logs: Vec<serde_json::Value>,
}

/// File reader for EVM block data (adapted from order_book FileReader)
struct EvmFileReader {
    current_path: Option<PathBuf>,
    file_position: u64,
    partial_line: String,
    base_dir: PathBuf,
}

impl EvmFileReader {
    fn new(base_dir: PathBuf) -> Self {
        Self { current_path: None, file_position: 0, partial_line: String::new(), base_dir }
    }

    fn find_latest_file(&self) -> Option<PathBuf> {
        let hourly_dir = self.base_dir.join("hourly");
        if !hourly_dir.exists() {
            return None;
        }

        let mut latest_day: Option<PathBuf> = None;
        if let Ok(entries) = std::fs::read_dir(&hourly_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && (latest_day.is_none() || path > *latest_day.as_ref().unwrap()) {
                    latest_day = Some(path);
                }
            }
        }

        let day_dir = latest_day?;
        let mut latest_file: Option<PathBuf> = None;
        let mut latest_mtime: Option<std::time::SystemTime> = None;

        if let Ok(entries) = std::fs::read_dir(&day_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(metadata) = path.metadata() {
                        if let Ok(mtime) = metadata.modified() {
                            if latest_mtime.is_none() || mtime > latest_mtime.unwrap() {
                                latest_mtime = Some(mtime);
                                latest_file = Some(path);
                            }
                        }
                    }
                }
            }
        }

        latest_file
    }

    fn check_for_newer_file(&mut self) -> Option<PathBuf> {
        if let Some(latest) = self.find_latest_file() {
            if let Some(ref current) = self.current_path {
                if latest != *current {
                    if let (Ok(latest_meta), Ok(current_meta)) = (latest.metadata(), current.metadata()) {
                        if let (Ok(latest_mtime), Ok(current_mtime)) = (latest_meta.modified(), current_meta.modified()) {
                            if latest_mtime > current_mtime {
                                return Some(latest);
                            }
                        }
                    }
                }
            } else {
                return Some(latest);
            }
        }
        None
    }

    fn on_modify(&mut self) -> Vec<String> {
        let mut lines = Vec::new();
        if let Some(ref path) = self.current_path {
            if let Ok(mut file) = File::open(path) {
                if let Ok(metadata) = file.metadata() {
                    let file_size = metadata.len();
                    if file_size > self.file_position {
                        if file.seek(SeekFrom::Start(self.file_position)).is_ok() {
                            let mut buf = String::new();
                            match file.read_to_string(&mut buf) {
                                Ok(bytes_read) if bytes_read > 0 => {
                                    self.file_position += bytes_read as u64;
                                    let full_buf = std::mem::take(&mut self.partial_line) + &buf;
                                    let mut line_iter = full_buf.lines().peekable();

                                    while let Some(line) = line_iter.next() {
                                        if line_iter.peek().is_some() {
                                            if !line.is_empty() && line.starts_with('[') && line.ends_with(']') {
                                                lines.push(line.to_string());
                                            } else if !line.is_empty() {
                                                self.partial_line = line.to_string();
                                            }
                                        } else {
                                            // Last line
                                            if buf.ends_with('\n') && !line.is_empty() {
                                                if line.starts_with('[') && line.ends_with(']') {
                                                    lines.push(line.to_string());
                                                } else {
                                                    self.partial_line = line.to_string();
                                                }
                                            } else if !line.is_empty() {
                                                self.partial_line = line.to_string();
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        lines
    }

    fn on_create(&mut self, path: &PathBuf) -> Vec<String> {
        let old_lines = self.on_modify();
        self.current_path = Some(path.clone());
        self.file_position = 0;
        self.partial_line.clear();
        old_lines
    }

    fn start_tracking(&mut self, path: &PathBuf) {
        if let Ok(metadata) = std::fs::metadata(path) {
            self.file_position = metadata.len();
        } else {
            self.file_position = 0;
        }
        self.current_path = Some(path.clone());
        self.partial_line.clear();
    }
}

/// Spawn the EVM file watcher thread
fn spawn_evm_file_watcher(dir: PathBuf, tx: Sender<String>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        info!("EVM watcher thread started for {:?}", dir);

        let mut reader = EvmFileReader::new(dir.clone());

        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let mut watcher = match recommended_watcher(move |res: Result<Event, _>| {
            drop(event_tx.send(res));
        }) {
            Ok(w) => w,
            Err(err) => {
                error!("EVM watcher failed to create: {}", err);
                return;
            }
        };

        if let Err(err) = watcher.watch(&dir, RecursiveMode::Recursive) {
            error!("EVM watcher failed to start: {}", err);
            return;
        }

        let poll_interval = Duration::from_millis(1);
        let mut poll_count = 0u64;

        loop {
            poll_count += 1;

            match event_rx.recv_timeout(poll_interval) {
                Ok(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let path = &event.paths[0];
                        if path.is_dir() {
                            continue;
                        }

                        if event.kind.is_create() {
                            info!("EVM new file: {:?}", path.file_name());
                            let old_lines = reader.on_create(path);
                            for line in old_lines {
                                if tx.send(line).is_err() {
                                    error!("EVM channel closed, exiting");
                                    return;
                                }
                            }
                        } else if reader.current_path.is_none() {
                            info!("EVM tracking: {:?}", path.file_name());
                            reader.start_tracking(path);
                        }

                        let lines = reader.on_modify();
                        for line in lines {
                            if tx.send(line).is_err() {
                                error!("EVM channel closed, exiting");
                                return;
                            }
                        }
                    }
                }
                Ok(Err(err)) => {
                    error!("EVM watcher error: {}", err);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let lines = reader.on_modify();
                    for line in lines {
                        if tx.send(line).is_err() {
                            error!("EVM channel closed, exiting");
                            return;
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    error!("EVM event channel closed, exiting");
                    return;
                }
            }

            // Check for newer files (hourly rotation)
            if poll_count % 10_000 == 0 {
                if let Some(newer_file) = reader.check_for_newer_file() {
                    info!("EVM detected newer file: {:?}", newer_file.file_name());
                    let old_lines = reader.on_create(&newer_file);
                    for line in old_lines {
                        if tx.send(line).is_err() {
                            error!("EVM channel closed, exiting");
                            return;
                        }
                    }
                }
            }
        }
    })
}

/// Start the EVM listener: file watcher → parse → broadcast
pub(crate) fn start_evm_listener(
    evm_data_dir: PathBuf,
    broadcast_tx: broadcast::Sender<Arc<EvmBroadcastMessage>>,
) {
    let (file_tx, file_rx) = unbounded::<String>();

    // Spawn the file watcher thread
    let _handle = spawn_evm_file_watcher(evm_data_dir, file_tx);

    // Bridge crossbeam → tokio: spawn a blocking task that reads from crossbeam
    // and sends parsed messages to the broadcast channel
    tokio::spawn(async move {
        let (tokio_tx, mut tokio_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        // Crossbeam → tokio mpsc bridge (blocking thread)
        thread::spawn(move || {
            while let Ok(line) = file_rx.recv() {
                if tokio_tx.send(line).is_err() {
                    break;
                }
            }
        });

        // Process lines and broadcast
        while let Some(line) = tokio_rx.recv().await {
            match serde_json::from_str::<EvmLine>(&line) {
                Ok((_timestamp, block_and_receipts)) => {
                    let header = block_and_receipts.to_new_head();
                    let logs = block_and_receipts.to_logs();

                    let msg = Arc::new(EvmBroadcastMessage { header, logs });

                    // Ignore send error (no receivers yet)
                    drop(broadcast_tx.send(msg));
                }
                Err(err) => {
                    error!("Failed to parse EVM block data: {}", err);
                }
            }
        }
    });
}
