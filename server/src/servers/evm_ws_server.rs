use crate::{
    EvmServerConfig,
    listeners::evm::{EvmBroadcastMessage, start_evm_listener},
    types::evm::{
        EvmSubscriptionManager, EvmSubscriptionType, JsonRpcRequest, LogFilter,
        json_rpc_error, json_rpc_result, subscription_notification,
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::sync::Arc;
use tokio::{net::TcpListener, select, sync::broadcast};
use yawc::{FrameView, OpCode, WebSocket};

use crate::prelude::*;

pub async fn run_evm_ws_server(config: EvmServerConfig) -> Result<()> {
    let (broadcast_tx, _) = broadcast::channel::<Arc<EvmBroadcastMessage>>(256);

    // Start EVM listener (file watcher + parser + broadcaster)
    start_evm_listener(config.evm_data_dir.clone(), broadcast_tx.clone());

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(config.compression_level));

    let app: Router = Router::new()
        .route(
            "/ws",
            get({
                let broadcast_tx = broadcast_tx.clone();
                move |ws_upgrade| async move {
                    ws_handler(ws_upgrade, broadcast_tx.clone(), websocket_opts)
                }
            }),
        )
        .route(
            "/health",
            get(|| async {
                axum::response::Response::builder()
                    .header("content-type", "application/json")
                    .body(r#"{"status":"ok"}"#.to_string())
                    .unwrap()
            }),
        );

    let tcp_listener = TcpListener::bind(&config.address).await?;
    info!("EVM WebSocket server running at ws://{}/ws", config.address);

    if let Err(err) = axum::serve(tcp_listener, app).await {
        error!("EVM server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    broadcast_tx: broadcast::Sender<Arc<EvmBroadcastMessage>>,
    websocket_opts: yawc::Options,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                error!("Failed to upgrade EVM websocket: {err}");
                return;
            }
        };
        handle_evm_socket(ws, broadcast_tx).await;
    });
    resp
}

async fn handle_evm_socket(
    mut socket: WebSocket,
    broadcast_tx: broadcast::Sender<Arc<EvmBroadcastMessage>>,
) {
    let mut broadcast_rx = broadcast_tx.subscribe();
    let mut sub_manager = EvmSubscriptionManager::new();

    loop {
        select! {
            // Broadcast messages from EVM listener
            recv_result = broadcast_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        for (sub_id, sub_type) in sub_manager.subscriptions() {
                            match sub_type {
                                EvmSubscriptionType::NewHeads => {
                                    let notification = subscription_notification(sub_id, &msg.header);
                                    send_json(&mut socket, &notification).await;
                                }
                                EvmSubscriptionType::Logs { filter } => {
                                    for log in &msg.logs {
                                        if filter.matches(log) {
                                            let notification = subscription_notification(sub_id, log);
                                            send_json(&mut socket, &notification).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::debug!("EVM receiver lagged: {n} messages");
                    }
                    Err(_) => {
                        error!("EVM broadcast channel closed");
                        return;
                    }
                }
            }

            // Incoming client messages
            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(t) => t,
                                Err(_) => return,
                            };

                            handle_client_message(&mut socket, &mut sub_manager, text).await;
                        }
                        OpCode::Close => {
                            info!("EVM client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("EVM client connection closed");
                    return;
                }
            }
        }
    }
}

async fn handle_client_message(
    socket: &mut WebSocket,
    sub_manager: &mut EvmSubscriptionManager,
    text: &str,
) {
    let req: JsonRpcRequest = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(_) => {
            let err = json_rpc_error(&serde_json::Value::Null, -32700, "Parse error");
            send_json(socket, &err).await;
            return;
        }
    };

    match req.method.as_str() {
        "eth_subscribe" => {
            let sub_name = req.params.first().and_then(|v| v.as_str()).unwrap_or("");
            match sub_name {
                "newHeads" => {
                    let id = sub_manager.subscribe(EvmSubscriptionType::NewHeads);
                    let resp = json_rpc_result(&req.id, serde_json::Value::String(id));
                    send_json(socket, &resp).await;
                }
                "logs" => {
                    let filter = if let Some(filter_param) = req.params.get(1) {
                        LogFilter::from_value(filter_param)
                    } else {
                        LogFilter::default()
                    };
                    let id = sub_manager.subscribe(EvmSubscriptionType::Logs { filter });
                    let resp = json_rpc_result(&req.id, serde_json::Value::String(id));
                    send_json(socket, &resp).await;
                }
                _ => {
                    let err = json_rpc_error(&req.id, -32602, &format!("Unsupported subscription type: {sub_name}"));
                    send_json(socket, &err).await;
                }
            }
        }
        "eth_unsubscribe" => {
            let sub_id = req.params.first().and_then(|v| v.as_str()).unwrap_or("");
            let success = sub_manager.unsubscribe(sub_id);
            let resp = json_rpc_result(&req.id, serde_json::Value::Bool(success));
            send_json(socket, &resp).await;
        }
        _ => {
            let err = json_rpc_error(&req.id, -32601, &format!("Method not found: {}", req.method));
            send_json(socket, &err).await;
        }
    }
}

async fn send_json(socket: &mut WebSocket, value: &serde_json::Value) {
    match serde_json::to_string(value) {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("EVM WS send error: {err}");
            }
        }
        Err(err) => {
            error!("EVM JSON serialization error: {err}");
        }
    }
}
