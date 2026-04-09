//! Compare l2Book latency: local orderbook server vs official Hyperliquid API.
//!
//! Both sides subscribe to l2Book for the same coin.
//! Matches by block_time_ms, records recv timestamps to JSONL.
//!
//! Architecture (no work on WS receive threads after timestamping):
//!   WS threads:  ws.read() → now_us() → send raw to channel
//!   Main thread: parse JSON, match by block_time, print
//!   Writer thread: write JSONL, flush
//!
//! Usage: latency_official_cmp [local_url] [coin] [count] [official_url] [output]

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufWriter, Write},
    sync::mpsc,
    time::{SystemTime, UNIX_EPOCH},
};

fn now_us() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

/// Raw message from WS thread — no parsing done yet
enum RawMsg {
    Local(String, u64),    // (raw_text, recv_us)
    Official(String, u64),
}

/// Matched record sent to writer thread
struct Record {
    bt: u64,
    lo_lat: u64,
    off_lat: u64,
    lo_recv: u64,
    off_recv: u64,
    consensus_us: u64,
}

fn spawn_ws(
    tx: mpsc::SyncSender<RawMsg>,
    url: String,
    coin: String,
    wrap: fn(String, u64) -> RawMsg,
    label: &'static str,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        loop {
            let (mut ws, _) = match tungstenite::connect(&url) {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("[{label}] connect failed: {e}, retrying in 3s...");
                    std::thread::sleep(std::time::Duration::from_secs(3));
                    continue;
                }
            };
            let sub = format!(
                r#"{{"method":"subscribe","subscription":{{"type":"l2Book","coin":"{coin}"}}}}"#
            );
            if ws.send(tungstenite::Message::Text(sub.into())).is_err() {
                eprintln!("[{label}] subscribe failed, retrying in 3s...");
                std::thread::sleep(std::time::Duration::from_secs(3));
                continue;
            }
            drop(ws.read()); // ack

            loop {
                let msg = match ws.read() {
                    Ok(tungstenite::Message::Text(t)) => t.to_string(),
                    Ok(_) => continue,
                    Err(e) => {
                        eprintln!("[{label}] WS error: {e}, reconnecting...");
                        break; // break inner loop → reconnect
                    }
                };
                let recv_us = now_us();
                if tx.send(wrap(msg, recv_us)).is_err() {
                    return; // channel closed, exit thread
                }
            }
        }
    })
}

fn spawn_writer(rx: mpsc::Receiver<Record>, output_file: String) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut writer = BufWriter::new(File::create(&output_file).expect("cannot create output file"));
        for r in rx {
            let _ = writeln!(
                writer,
                r#"{{"block_time_ms":{},"local_recv_us":{},"official_recv_us":{},"local_e2e_us":{},"official_e2e_us":{},"consensus_us":{}}}"#,
                r.bt, r.lo_recv, r.off_recv, r.lo_lat, r.off_lat, r.consensus_us
            );
            let _ = writer.flush();
        }
    })
}

/// Returns (block_time_ms, local_time_us) from an l2Book message
fn parse_l2_msg(raw: &str) -> Option<(u64, u64)> {
    let v: serde_json::Value = serde_json::from_str(raw).ok()?;
    if v.get("channel")?.as_str()? != "l2Book" {
        return None;
    }
    let t = v["data"]["time"].as_u64()?;
    if t == 0 { return None; }
    let local_time_us = v["data"]["localTimeUs"].as_u64().unwrap_or(0);
    Some((t, local_time_us))
}

fn main() {
    let local_url = std::env::args().nth(1).unwrap_or_else(|| "ws://127.0.0.1:8000/ws".into());
    let coin = std::env::args().nth(2).unwrap_or_else(|| "BTC".into());
    let count: usize = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(usize::MAX);
    let official_url =
        std::env::args().nth(4).unwrap_or_else(|| "wss://api.hyperliquid.xyz/ws".into());
    let output_file = std::env::args().nth(5).unwrap_or_else(|| "latency_cmp.jsonl".into());

    eprintln!("Local    : {local_url}");
    eprintln!("Official : {official_url}");
    eprintln!("Coin     : {coin}");
    eprintln!("Count    : {count} matched pairs");
    eprintln!("Output   : {output_file}");
    eprintln!();

    // WS threads → main thread (bounded so backpressure doesn't explode memory)
    let (raw_tx, raw_rx) = mpsc::sync_channel::<RawMsg>(4096);

    // Main thread → writer thread
    let (write_tx, write_rx) = mpsc::sync_channel::<Record>(4096);

    let _h1 = spawn_ws(raw_tx.clone(), local_url, coin.clone(), RawMsg::Local, "local");
    let _h2 = spawn_ws(raw_tx, official_url, coin, RawMsg::Official, "official");
    let _hw = spawn_writer(write_rx, output_file);

    let mut local_map: HashMap<u64, (u64, u64)> = HashMap::new(); // bt -> (recv_us, local_time_us)
    let mut official_map: HashMap<u64, u64> = HashMap::new();
    let mut matched_set: HashSet<u64> = HashSet::new();
    let mut matched_count = 0usize;

    println!(
        "{:>5}  {:>15}  {:>12}  {:>12}  {:>12}  {:>12}  {:>8}",
        "#", "block_time_ms", "local_us", "official_us", "consensus_us", "diff_us", "faster"
    );
    println!("{}", "-".repeat(95));

    while matched_count < count {
        let raw = match raw_rx.recv() {
            Ok(m) => m,
            Err(_) => break,
        };

        // Parse on main thread (off the hot WS path)
        let (block_time_ms, recv_us, local_time_us, is_local) = match &raw {
            RawMsg::Local(text, recv) => match parse_l2_msg(text) {
                Some((bt, lt)) => (bt, *recv, lt, true),
                None => continue,
            },
            RawMsg::Official(text, recv) => match parse_l2_msg(text) {
                Some((bt, _)) => (bt, *recv, 0u64, false),
                None => continue,
            },
        };

        if matched_set.contains(&block_time_ms) {
            continue;
        }

        if is_local {
            local_map.entry(block_time_ms).or_insert((recv_us, local_time_us));
            if let Some(&off_recv) = official_map.get(&block_time_ms) {
                let (lo_recv, lo_lt) = *local_map.get(&block_time_ms).unwrap();
                matched_set.insert(block_time_ms);
                print_and_send(
                    &write_tx, &mut matched_count,
                    block_time_ms, lo_recv, off_recv, lo_lt,
                );
            }
        } else {
            official_map.entry(block_time_ms).or_insert(recv_us);
            if let Some(&(lo_recv, lo_lt)) = local_map.get(&block_time_ms) {
                let off_recv = *official_map.get(&block_time_ms).unwrap();
                matched_set.insert(block_time_ms);
                print_and_send(
                    &write_tx, &mut matched_count,
                    block_time_ms, lo_recv, off_recv, lo_lt,
                );
            }
        }

        // Prune old entries
        if local_map.len() > 5000 {
            let cutoff = block_time_ms.saturating_sub(10_000);
            local_map.retain(|&k, _| k > cutoff);
            official_map.retain(|&k, _| k > cutoff);
            matched_set.retain(|&k| k > cutoff);
        }
    }

}

fn print_and_send(
    write_tx: &mpsc::SyncSender<Record>,
    matched_count: &mut usize,
    bt: u64, lo: u64, off: u64, local_time_us: u64,
) {
    *matched_count += 1;
    let bt_us = bt * 1000;
    let lo_lat = lo.saturating_sub(bt_us);
    let off_lat = off.saturating_sub(bt_us);
    let consensus_us = if local_time_us > 0 { local_time_us.saturating_sub(bt_us) } else { 0 };
    let diff = lo as i64 - off as i64;
    let faster = if diff < 0 { "LOCAL" } else { "OFFICIAL" };

    println!(
        "{:>5}  {:>15}  {:>10} us  {:>10} us  {:>10} us  {:>+10} us  {:>8}",
        matched_count, bt, lo_lat, off_lat, consensus_us, diff, faster
    );

    let _ = write_tx.send(Record { bt, lo_lat, off_lat, lo_recv: lo, off_recv: off, consensus_us });
}
