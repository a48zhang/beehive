use super::{api::ApiError, protocol, service::State};
use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, client::IntoClientRequest, protocol::WebSocketConfig},
};

#[derive(Debug, Error)]
#[error("QQ fatal gateway error: {0}")]
pub struct Fatal(pub String);
#[derive(Default, Serialize, Deserialize)]
struct Session {
    id: Option<String>,
    seq: Option<i64>,
}
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
async fn send(socket: &mut Socket, payload: Value) -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(payload.to_string().into())),
    )
    .await
    .context("QQ WebSocket write timeout")??;
    Ok(())
}
fn save(state: &State, session: &Session) -> Result<()> {
    state
        .store
        .set_meta("ws_session", &serde_json::to_string(session)?)
}

pub async fn run(state: Arc<State>) -> Result<()> {
    let mut attempt = 0_u32;
    loop {
        if state.stop.is_cancelled() {
            return Ok(());
        }
        state
            .config
            .ensure_hermes_stopped(state.api.app_id())
            .map_err(|e| Fatal(e.to_string()))?;
        let started = Instant::now();
        let result = tokio::select! {_=state.stop.cancelled()=>return Ok(()),result=connection(&state)=>result};
        state.status.lock().unwrap().qq_ready = false;
        if let Err(error) = &result {
            if error.downcast_ref::<Fatal>().is_some() {
                return result;
            }
            // ApiError contains only status/code, never a response body or URL.
            if let Some(api_error) = error.downcast_ref::<ApiError>() {
                if matches!(
                    api_error,
                    ApiError::Rejected {
                        status: 401 | 403,
                        ..
                    }
                ) {
                    return Err(Fatal(api_error.to_string()).into());
                }
                tracing::warn!(attempt, error = %api_error, "QQ API unavailable; reconnecting");
            } else {
                // Arbitrary WS errors may contain the remote URL; don't log their detail.
                tracing::warn!(attempt, "QQ connection ended; reconnecting");
            }
        }
        if state.stop.is_cancelled() {
            return Ok(());
        }
        if started.elapsed() > Duration::from_secs(30) {
            attempt = 0;
        }
        let delay = [2, 5, 10, 30, 60][attempt.min(4) as usize];
        attempt += 1;
        state.status.lock().unwrap().reconnects += 1;
        tokio::select! {_=state.stop.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(delay))=>{}}
    }
}

async fn connection(state: &State) -> Result<()> {
    let gateway = state.api.gateway().await?;
    let token = state.api.token().await?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(1024 * 1024))
        .max_frame_size(Some(1024 * 1024));
    let mut request = gateway.into_client_request()?;
    request
        .headers_mut()
        .insert("User-Agent", "PiQQ/0.2.0".parse()?);
    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(20),
        connect_async_with_config(request, Some(config), false),
    )
    .await
    .context("QQ gateway connect timeout")??;
    let mut session: Session = state
        .store
        .meta("ws_session")?
        .map(|value| serde_json::from_str(&value))
        .transpose()?
        .unwrap_or_default();
    let mut hello = false;
    let mut waiting_ack = false;
    let hello_deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(hello_deadline);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _=state.stop.cancelled()=>{
                let _=tokio::time::timeout(Duration::from_secs(2),socket.close(None)).await;
                return Ok(());
            }
            _=&mut hello_deadline,if !hello=>bail!("QQ gateway Hello timeout"),
            _=heartbeat.tick(),if hello=>{
                ensure!(!waiting_ack,"QQ heartbeat ACK missing");
                send(&mut socket,json!({"op":1,"d":session.seq})).await?;waiting_ack=true;
            }
            message=socket.next()=>{
                let message=message.context("QQ WebSocket ended")??;
                match message {
                    Message::Text(text)=>{
                        let payload:Value=serde_json::from_str(text.as_str()).context("Malformed QQ JSON frame")?;
                        match payload["op"].as_i64(){
                            Some(10)=>{
                                let ms=payload["d"]["heartbeat_interval"].as_u64().context("Missing QQ heartbeat interval")?;
                                ensure!((100..=300000).contains(&ms),"Invalid QQ heartbeat interval");
                                let period=Duration::from_millis(ms*8/10);
                                heartbeat=tokio::time::interval_at(tokio::time::Instant::now()+period,period);
                                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                                hello=true;waiting_ack=false;
                                if session.id.is_some()&&session.seq.is_some(){
                                    send(&mut socket,json!({"op":6,"d":{"token":format!("QQBot {token}"),"session_id":session.id,"seq":session.seq}})).await?;
                                }else{
                                    send(&mut socket,json!({"op":2,"d":{"token":format!("QQBot {token}"),"intents":1_u64<<25,"shard":[0,1],"properties":{"$os":"linux","$browser":"pi-qq","$device":"pi-qq"}}})).await?;
                                }
                            }
                            Some(0)=>{
                                ensure!(hello,"QQ dispatch before Hello");
                                let kind=payload["t"].as_str().unwrap_or("");
                                match kind {
                                    "READY"=>{
                                        let id=payload["d"]["session_id"].as_str().filter(|id|!id.is_empty()&&id.len()<512).context("Missing QQ session ID")?;
                                        if session.id.as_deref()!=Some(id){session.seq=None;}
                                        session.id=Some(id.into());
                                        state.status.lock().unwrap().qq_ready=true;
                                        tracing::info!("QQ gateway READY");
                                    }
                                    "RESUMED"=>{state.status.lock().unwrap().qq_ready=true;tracing::info!("QQ gateway RESUMED");}
                                    "C2C_MESSAGE_CREATE"|"GROUP_AT_MESSAGE_CREATE"=>{
                                        if let Some(event)=protocol::inbound(state.api.app_id(),kind,&payload["d"],&state.config)
                                            && state.store.insert(event)? {state.notify.notify_one();}
                                    }
                                    _=>{}
                                }
                                // Advance the durable resume cursor only after recording the event.
                                if let Some(seq)=payload["s"].as_i64().filter(|seq|*seq>=0)
                                    && session.seq.is_none_or(|old|seq>old){session.seq=Some(seq);}
                                save(state,&session)?;
                            }
                            Some(11)=>waiting_ack=false,
                            Some(7)=>bail!("QQ requested reconnect"),
                            Some(9)=>{
                                if payload["d"]!=true{state.store.clear_session()?;}
                                bail!("QQ invalid session; reconnecting");
                            }
                            _=>{}
                        }
                    }
                    Message::Ping(payload)=>{tokio::time::timeout(Duration::from_secs(5),socket.send(Message::Pong(payload))).await??;}
                    Message::Close(frame)=>{
                        let code=frame.as_ref().map(|frame|u16::from(frame.code)).unwrap_or(1000);
                        if matches!(code,4001|4002|4010..=4014|4914|4915){return Err(Fatal(format!("WebSocket close code {code}")).into());}
                        if code==4004{state.api.invalidate().await;}
                        if matches!(code,4006|4007|4900..=4913){state.store.clear_session()?;}
                        if code==4008{
                            tokio::select!{_=state.stop.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(60))=>{}}
                        }
                        bail!("QQ WebSocket closed with code {code}");
                    }
                    _=>{}
                }
            }
        }
    }
}
