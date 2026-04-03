use serde::Deserialize;
use std::collections::HashMap;

/// JSON-RPC 2.0 request
#[derive(Debug, Deserialize)]
pub(crate) struct JsonRpcRequest {
    #[allow(dead_code)]
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
    pub id: serde_json::Value,
}

/// JSON-RPC 2.0 success response
pub(crate) fn json_rpc_result(id: &serde_json::Value, result: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": id,
    })
}

/// JSON-RPC 2.0 error response
pub(crate) fn json_rpc_error(id: &serde_json::Value, code: i64, message: &str) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": message },
        "id": id,
    })
}

/// eth_subscription push notification
pub(crate) fn subscription_notification(sub_id: &str, result: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_subscription",
        "params": {
            "subscription": sub_id,
            "result": result,
        },
    })
}

/// Subscription type
#[derive(Debug)]
pub(crate) enum EvmSubscriptionType {
    NewHeads,
    Logs { filter: LogFilter },
}

/// Log filter for eth_subscribe logs
#[derive(Debug, Default)]
pub(crate) struct LogFilter {
    pub address: Option<AddressFilter>,
    pub topics: Option<Vec<Option<TopicFilter>>>,
}

#[derive(Debug)]
pub(crate) enum AddressFilter {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug)]
pub(crate) enum TopicFilter {
    Single(String),
    OneOf(Vec<String>),
}

impl LogFilter {
    /// Parse from JSON params (the second element of eth_subscribe params)
    pub(crate) fn from_value(v: &serde_json::Value) -> Self {
        let mut filter = Self::default();

        if let Some(obj) = v.as_object() {
            // Parse address
            if let Some(addr) = obj.get("address") {
                if let Some(s) = addr.as_str() {
                    filter.address = Some(AddressFilter::Single(s.to_lowercase()));
                } else if let Some(arr) = addr.as_array() {
                    let addrs: Vec<String> = arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_lowercase()))
                        .collect();
                    if !addrs.is_empty() {
                        filter.address = Some(AddressFilter::Multiple(addrs));
                    }
                }
            }

            // Parse topics
            if let Some(topics) = obj.get("topics") {
                if let Some(arr) = topics.as_array() {
                    let parsed: Vec<Option<TopicFilter>> = arr
                        .iter()
                        .map(|t| {
                            if t.is_null() {
                                None
                            } else if let Some(s) = t.as_str() {
                                Some(TopicFilter::Single(s.to_lowercase()))
                            } else if let Some(arr) = t.as_array() {
                                let topics: Vec<String> = arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(|s| s.to_lowercase()))
                                    .collect();
                                if topics.is_empty() {
                                    None
                                } else {
                                    Some(TopicFilter::OneOf(topics))
                                }
                            } else {
                                None
                            }
                        })
                        .collect();
                    if !parsed.is_empty() {
                        filter.topics = Some(parsed);
                    }
                }
            }
        }

        filter
    }

    /// Check if a log matches this filter
    pub(crate) fn matches(&self, log: &serde_json::Value) -> bool {
        // Match address
        if let Some(ref addr_filter) = self.address {
            let log_addr = log["address"].as_str().unwrap_or("").to_lowercase();
            let matches = match addr_filter {
                AddressFilter::Single(a) => log_addr == *a,
                AddressFilter::Multiple(addrs) => addrs.iter().any(|a| log_addr == *a),
            };
            if !matches {
                return false;
            }
        }

        // Match topics
        if let Some(ref topic_filters) = self.topics {
            let log_topics = match log["topics"].as_array() {
                Some(arr) => arr,
                None => return false,
            };

            for (i, filter) in topic_filters.iter().enumerate() {
                if let Some(tf) = filter {
                    // If filter specifies a topic at position i but log doesn't have it, no match
                    let log_topic = match log_topics.get(i).and_then(|v| v.as_str()) {
                        Some(t) => t.to_lowercase(),
                        None => return false,
                    };
                    let matches = match tf {
                        TopicFilter::Single(t) => log_topic == *t,
                        TopicFilter::OneOf(ts) => ts.iter().any(|t| log_topic == *t),
                    };
                    if !matches {
                        return false;
                    }
                }
                // None means wildcard at this position
            }
        }

        true
    }
}

/// Per-connection subscription manager
pub(crate) struct EvmSubscriptionManager {
    subscriptions: HashMap<String, EvmSubscriptionType>,
    next_id: u64,
}

impl EvmSubscriptionManager {
    pub(crate) fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            next_id: 1,
        }
    }

    pub(crate) fn subscribe(&mut self, sub_type: EvmSubscriptionType) -> String {
        let id = format!("0x{:x}", self.next_id);
        self.next_id += 1;
        self.subscriptions.insert(id.clone(), sub_type);
        id
    }

    pub(crate) fn unsubscribe(&mut self, id: &str) -> bool {
        self.subscriptions.remove(id).is_some()
    }

    pub(crate) fn subscriptions(&self) -> &HashMap<String, EvmSubscriptionType> {
        &self.subscriptions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_filter_empty_matches_all() {
        let filter = LogFilter::default();
        let log = serde_json::json!({
            "address": "0xabc",
            "topics": ["0x123"],
            "data": "0x"
        });
        assert!(filter.matches(&log));
    }

    #[test]
    fn test_log_filter_address_single() {
        let filter = LogFilter::from_value(&serde_json::json!({
            "address": "0xABC"
        }));
        let log_match = serde_json::json!({ "address": "0xabc", "topics": [] });
        let log_no = serde_json::json!({ "address": "0xdef", "topics": [] });
        assert!(filter.matches(&log_match));
        assert!(!filter.matches(&log_no));
    }

    #[test]
    fn test_log_filter_address_array() {
        let filter = LogFilter::from_value(&serde_json::json!({
            "address": ["0xABC", "0xDEF"]
        }));
        let log1 = serde_json::json!({ "address": "0xabc", "topics": [] });
        let log2 = serde_json::json!({ "address": "0xdef", "topics": [] });
        let log3 = serde_json::json!({ "address": "0x999", "topics": [] });
        assert!(filter.matches(&log1));
        assert!(filter.matches(&log2));
        assert!(!filter.matches(&log3));
    }

    #[test]
    fn test_log_filter_topics() {
        let filter = LogFilter::from_value(&serde_json::json!({
            "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", null]
        }));
        // Transfer event signature at position 0, any at position 1
        let log_match = serde_json::json!({
            "address": "0xabc",
            "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", "0x111"]
        });
        let log_no = serde_json::json!({
            "address": "0xabc",
            "topics": ["0x999"]
        });
        assert!(filter.matches(&log_match));
        assert!(!filter.matches(&log_no));
    }

    #[test]
    fn test_subscription_manager() {
        let mut mgr = EvmSubscriptionManager::new();
        let id1 = mgr.subscribe(EvmSubscriptionType::NewHeads);
        let id2 = mgr.subscribe(EvmSubscriptionType::Logs { filter: LogFilter::default() });
        assert_eq!(id1, "0x1");
        assert_eq!(id2, "0x2");
        assert_eq!(mgr.subscriptions().len(), 2);
        assert!(mgr.unsubscribe(&id1));
        assert_eq!(mgr.subscriptions().len(), 1);
        assert!(!mgr.unsubscribe(&id1)); // already removed
    }
}
