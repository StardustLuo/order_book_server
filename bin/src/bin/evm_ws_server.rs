use std::net::Ipv4Addr;
use std::path::PathBuf;

use clap::Parser;
use server::{EvmServerConfig, Result, run_evm_ws_server};

#[derive(Debug, Parser)]
#[command(author, version, about = "EVM WebSocket Server for Hyperliquid (eth_subscribe)")]
struct Args {
    /// Server address
    #[arg(long, default_value = "0.0.0.0")]
    address: Ipv4Addr,

    /// Server port
    #[arg(long, default_value = "8545")]
    port: u16,

    /// Compression level for WebSocket connections (0-9)
    #[arg(long, default_value = "1")]
    compression_level: u32,

    /// Path to evm_block_and_receipts directory
    #[arg(long)]
    evm_data_dir: PathBuf,

    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("RUST_LOG", &args.log_level);
    }
    env_logger::init();

    let config = EvmServerConfig {
        address: format!("{}:{}", args.address, args.port),
        compression_level: args.compression_level,
        evm_data_dir: args.evm_data_dir,
    };

    println!("EVM WebSocket Server");
    println!("  Address: ws://{}/ws", config.address);
    println!("  EVM data dir: {}", config.evm_data_dir.display());
    println!("  Compression: {}", config.compression_level);
    println!("  Log level: {}", args.log_level);
    println!();

    tokio::select! {
        result = run_evm_ws_server(config) => {
            if let Err(e) = result {
                log::error!("EVM server error: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            log::info!("Shutdown signal received");
        }
    }

    Ok(())
}
