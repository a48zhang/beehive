//! Deterministic Pi RPC protocol peer used only by integration tests.
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Write},
    path::PathBuf,
    time::Duration,
};
fn emit(event: Value) {
    let mut out = io::stdout().lock();
    writeln!(out, "{event}").unwrap();
    out.flush().unwrap();
}
fn fresh(dir: &std::path::Path) -> PathBuf {
    let path = dir.join(format!("{}.jsonl", pi_runtime::store::id()));
    std::fs::write(&path, "0").unwrap();
    path
}
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(&args[args.iter().position(|a| a == "--session-dir").unwrap() + 1]);
    let mut session = fresh(&dir);
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let id = request["id"].clone();
        let kind = request["type"].as_str().unwrap();
        let mut data = json!({});
        let mut success = true;
        match kind {
            "get_state" => {
                data = json!({"sessionFile":session.to_str().unwrap(),"isStreaming":false})
            }
            "new_session" => {
                session = fresh(&dir);
                data = json!({"cancelled":false});
            }
            "switch_session" => {
                session = PathBuf::from(request["sessionPath"].as_str().unwrap());
                assert!(session.is_file());
            }
            "prompt" => {
                let message = request["message"].as_str().unwrap();
                if message == "reject" {
                    success = false;
                } else if message == "/handled" {
                    data = json!({"disposition":"handled"});
                } else {
                    if message == "slow" {
                        std::thread::sleep(Duration::from_secs(60));
                    }
                    let count = std::fs::read_to_string(&session)
                        .unwrap()
                        .parse::<u32>()
                        .unwrap()
                        + 1;
                    std::fs::write(&session, count.to_string()).unwrap();
                    emit(
                        json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"discarded delta"}}),
                    );
                    if message == "dialog" {
                        emit(
                            json!({"type":"extension_ui_request","id":"dialog-id","method":"confirm","title":"Approve?"}),
                        );
                        // The test peer's main input loop accepts the cancellation below.
                    }
                    // Deliberately emit the final event BEFORE prompt acknowledgement.
                    emit(
                        json!({"type":"message_end","message":{"role":"assistant","stopReason":if message == "error" {"error"} else {"stop"},"errorMessage":if message == "error" {"mock provider failure"} else {""},"content":[{"type":"text","text":format!("{count}:{message}")} ]}}),
                    );
                    emit(json!({"type":"agent_end","willRetry":false}));
                    emit(json!({"type":"agent_settled","aborted":false}));
                    data = json!({"disposition":"started"});
                }
            }
            "extension_ui_response" => continue,
            _ => {}
        }
        emit(
            json!({"type":"response","command":kind,"id":id,"success":success,"data":data,"error":if success {""} else {"mock rejection"}}),
        );
    }
}
