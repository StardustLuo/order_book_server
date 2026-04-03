use alloy::consensus::{TxEip1559, TxLegacy, TxEnvelope, Signed};
use alloy::eips::eip2930::AccessList;
use alloy::primitives::{Address, Bytes, Signature, TxKind, B256, U256};
use serde::Deserialize;

/// Each NDJSON line: ["2026-04-02T13:59:59...", { "block": {...}, "receipts": [...], ... }]
pub(crate) type EvmLine = (String, EvmBlockAndReceipts);

#[derive(Debug, Deserialize)]
pub(crate) struct EvmBlockAndReceipts {
    pub block: EvmBlock,
    pub receipts: Vec<EvmReceipt>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EvmBlock {
    #[serde(rename = "Reth115")]
    pub reth115: Reth115Block,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Reth115Block {
    pub header: Reth115Header,
    pub body: Reth115Body,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Reth115Header {
    pub hash: String,
    pub header: Reth115HeaderInner,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Reth115HeaderInner {
    pub parent_hash: String,
    pub sha3_uncles: String,
    pub miner: String,
    pub state_root: String,
    pub transactions_root: String,
    pub receipts_root: String,
    pub logs_bloom: String,
    pub difficulty: String,
    pub number: String,
    pub gas_limit: String,
    pub gas_used: String,
    pub timestamp: String,
    pub extra_data: String,
    pub mix_hash: String,
    pub nonce: String,
    pub base_fee_per_gas: String,
    pub withdrawals_root: String,
    pub blob_gas_used: String,
    pub excess_blob_gas: String,
    pub parent_beacon_block_root: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Reth115Body {
    pub transactions: Vec<Reth115SignedTx>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Reth115SignedTx {
    pub signature: Reth115Signature,
    pub transaction: Reth115TxType,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Reth115Signature {
    pub r: String,
    pub s: String,
    pub y_parity: String,
}

#[derive(Debug, Deserialize)]
pub(crate) enum Reth115TxType {
    Eip1559(Reth115Eip1559),
    Legacy(Reth115Legacy),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Reth115Eip1559 {
    pub chain_id: String,
    pub nonce: String,
    pub gas: String,
    pub max_fee_per_gas: String,
    pub max_priority_fee_per_gas: String,
    pub to: String,
    pub value: String,
    pub access_list: Vec<AccessListItem>,
    pub input: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Reth115Legacy {
    pub nonce: String,
    pub gas_price: String,
    pub gas: String,
    pub to: String,
    pub value: String,
    pub input: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccessListItem {
    pub address: String,
    pub storage_keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub(crate) struct EvmReceipt {
    pub tx_type: String,
    pub success: bool,
    pub cumulative_gas_used: u64,
    pub logs: Vec<EvmLog>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EvmLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
}

// --- Transform to standard Ethereum format ---

fn parse_hex_u64(s: &str) -> u64 {
    u64::from_str_radix(s.strip_prefix("0x").unwrap_or(s), 16).unwrap_or(0)
}

fn parse_hex_u128(s: &str) -> u128 {
    u128::from_str_radix(s.strip_prefix("0x").unwrap_or(s), 16).unwrap_or(0)
}

fn parse_address(s: &str) -> Address {
    s.parse().unwrap_or_default()
}

fn parse_bytes(s: &str) -> Bytes {
    s.parse().unwrap_or_default()
}

fn parse_access_list(items: &[AccessListItem]) -> AccessList {
    use alloy::eips::eip2930::AccessListItem as AlloyItem;
    AccessList(
        items
            .iter()
            .map(|item| AlloyItem {
                address: parse_address(&item.address),
                storage_keys: item.storage_keys.iter().map(|k| k.parse().unwrap_or_default()).collect(),
            })
            .collect(),
    )
}

fn parse_u256(s: &str) -> U256 {
    U256::from_str_radix(s.strip_prefix("0x").unwrap_or(s), 16).unwrap_or_default()
}

impl Reth115SignedTx {
    /// Compute the transaction hash by reconstructing the signed tx envelope via alloy
    pub(crate) fn tx_hash(&self) -> B256 {
        let sig = Signature::new(
            parse_u256(&self.signature.r),
            parse_u256(&self.signature.s),
            parse_hex_u64(&self.signature.y_parity) != 0,
        );

        match &self.transaction {
            Reth115TxType::Eip1559(tx) => {
                let inner = TxEip1559 {
                    chain_id: parse_hex_u64(&tx.chain_id),
                    nonce: parse_hex_u64(&tx.nonce),
                    gas_limit: parse_hex_u64(&tx.gas),
                    max_fee_per_gas: parse_hex_u128(&tx.max_fee_per_gas),
                    max_priority_fee_per_gas: parse_hex_u128(&tx.max_priority_fee_per_gas),
                    to: if tx.to.is_empty() || tx.to == "0x" {
                        TxKind::Create
                    } else {
                        TxKind::Call(parse_address(&tx.to))
                    },
                    value: parse_u256(&tx.value),
                    access_list: parse_access_list(&tx.access_list),
                    input: parse_bytes(&tx.input),
                };
                let signed = Signed::new_unhashed(inner, sig);
                let envelope = TxEnvelope::Eip1559(signed);
                *envelope.tx_hash()
            }
            Reth115TxType::Legacy(tx) => {
                let inner = TxLegacy {
                    chain_id: Some(999), // Hyperliquid EVM chain ID
                    nonce: parse_hex_u64(&tx.nonce),
                    gas_limit: parse_hex_u64(&tx.gas),
                    gas_price: parse_hex_u128(&tx.gas_price),
                    to: if tx.to.is_empty() || tx.to == "0x" {
                        TxKind::Create
                    } else {
                        TxKind::Call(parse_address(&tx.to))
                    },
                    value: parse_u256(&tx.value),
                    input: parse_bytes(&tx.input),
                };
                let signed = Signed::new_unhashed(inner, sig);
                let envelope = TxEnvelope::Legacy(signed);
                *envelope.tx_hash()
            }
        }
    }
}

impl EvmBlockAndReceipts {
    /// Convert to standard newHeads format (serde_json::Value)
    pub(crate) fn to_new_head(&self) -> serde_json::Value {
        let h = &self.block.reth115.header;
        let inner = &h.header;
        serde_json::json!({
            "hash": h.hash,
            "parentHash": inner.parent_hash,
            "sha3Uncles": inner.sha3_uncles,
            "miner": inner.miner,
            "stateRoot": inner.state_root,
            "transactionsRoot": inner.transactions_root,
            "receiptsRoot": inner.receipts_root,
            "logsBloom": inner.logs_bloom,
            "difficulty": inner.difficulty,
            "number": inner.number,
            "gasLimit": inner.gas_limit,
            "gasUsed": inner.gas_used,
            "timestamp": inner.timestamp,
            "extraData": inner.extra_data,
            "mixHash": inner.mix_hash,
            "nonce": inner.nonce,
            "baseFeePerGas": inner.base_fee_per_gas,
            "withdrawalsRoot": inner.withdrawals_root,
            "blobGasUsed": inner.blob_gas_used,
            "excessBlobGas": inner.excess_blob_gas,
            "parentBeaconBlockRoot": inner.parent_beacon_block_root,
        })
    }

    /// Convert all logs to standard Ethereum log format
    pub(crate) fn to_logs(&self) -> Vec<serde_json::Value> {
        let block_hash = &self.block.reth115.header.hash;
        let block_number = &self.block.reth115.header.header.number;
        let block_timestamp = &self.block.reth115.header.header.timestamp;
        let txs = &self.block.reth115.body.transactions;

        let mut all_logs = Vec::new();
        let mut global_log_index: u64 = 0;

        for (tx_idx, receipt) in self.receipts.iter().enumerate() {
            let tx_hash = if tx_idx < txs.len() {
                format!("{:?}", txs[tx_idx].tx_hash())
            } else {
                String::from("0x0000000000000000000000000000000000000000000000000000000000000000")
            };
            let tx_index = format!("0x{:x}", tx_idx);

            for log in &receipt.logs {
                let log_value = serde_json::json!({
                    "address": log.address,
                    "topics": log.topics,
                    "data": log.data,
                    "blockHash": block_hash,
                    "blockNumber": block_number,
                    "transactionHash": tx_hash,
                    "transactionIndex": tx_index,
                    "logIndex": format!("0x{:x}", global_log_index),
                    "blockTimestamp": block_timestamp,
                    "removed": false,
                });
                all_logs.push(log_value);
                global_log_index += 1;
            }
        }

        all_logs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_evm_line() {
        let sample = r#"["2026-04-02T12:59:59.210956833",{"block":{"Reth115":{"header":{"hash":"0xabc123","header":{"parentHash":"0x111","sha3Uncles":"0x222","miner":"0x0000000000000000000000000000000000000000","stateRoot":"0x333","transactionsRoot":"0x444","receiptsRoot":"0x555","logsBloom":"0x00","difficulty":"0x0","number":"0x1df0c63","gasLimit":"0x2dc6c0","gasUsed":"0x1bbf6","timestamp":"0x69ce684f","extraData":"0x","mixHash":"0x666","nonce":"0x0000000000000000","baseFeePerGas":"0x5f5e100","withdrawalsRoot":"0x777","blobGasUsed":"0x0","excessBlobGas":"0x0","parentBeaconBlockRoot":"0x888"}},"body":{"transactions":[],"ommers":[],"withdrawals":[]}}},"receipts":[],"system_txs":[],"read_precompile_calls":[],"highest_precompile_address":"0x0000000000000000000000000000000000000813"}]"#;
        let parsed: EvmLine = serde_json::from_str(sample).unwrap();
        assert_eq!(parsed.0, "2026-04-02T12:59:59.210956833");
        assert_eq!(parsed.1.block.reth115.header.hash, "0xabc123");
        assert_eq!(parsed.1.block.reth115.header.header.number, "0x1df0c63");
    }

    #[test]
    fn test_to_new_head() {
        let sample = r#"["2026-04-02T12:59:59",{"block":{"Reth115":{"header":{"hash":"0xabc","header":{"parentHash":"0x111","sha3Uncles":"0x222","miner":"0x0000000000000000000000000000000000000000","stateRoot":"0x333","transactionsRoot":"0x444","receiptsRoot":"0x555","logsBloom":"0x00","difficulty":"0x0","number":"0x100","gasLimit":"0x2dc6c0","gasUsed":"0x0","timestamp":"0x69ce684f","extraData":"0x","mixHash":"0x666","nonce":"0x0000000000000000","baseFeePerGas":"0x5f5e100","withdrawalsRoot":"0x777","blobGasUsed":"0x0","excessBlobGas":"0x0","parentBeaconBlockRoot":"0x888"}},"body":{"transactions":[],"ommers":[],"withdrawals":[]}}},"receipts":[],"system_txs":[],"read_precompile_calls":[],"highest_precompile_address":"0x0000000000000000000000000000000000000813"}]"#;
        let parsed: EvmLine = serde_json::from_str(sample).unwrap();
        let head = parsed.1.to_new_head();
        assert_eq!(head["hash"], "0xabc");
        assert_eq!(head["number"], "0x100");
        assert_eq!(head["parentHash"], "0x111");
    }
}
