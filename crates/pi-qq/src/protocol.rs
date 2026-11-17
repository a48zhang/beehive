use super::config::Config;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn hash(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update(part.as_bytes());
        digest.update([0]);
    }
    format!("{:x}", digest.finalize())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Inbound {
    pub key: String,
    pub channel: String,
    pub kind: String,
    pub target: String,
    pub user: String,
    pub message_id: String,
    pub text: String,
    pub timestamp: i64,
}
fn identifier(value: &Value) -> Option<&str> {
    value.as_str().filter(|id| {
        !id.is_empty()
            && id.len() <= 256
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    })
}

pub fn inbound(app: &str, event_type: &str, data: &Value, config: &Config) -> Option<Inbound> {
    let (kind, target, user) = match event_type {
        "C2C_MESSAGE_CREATE" => {
            let user = identifier(&data["author"]["user_openid"])?;
            ("c2c", user, user)
        }
        "GROUP_AT_MESSAGE_CREATE" => {
            let group = identifier(&data["group_openid"])?;
            let user = identifier(&data["author"]["member_openid"])?;
            if !config.allowed_groups.iter().any(|g| g == group) {
                return None;
            }
            ("group", group, user)
        }
        _ => return None,
    };
    if !config.allowed_users.iter().any(|allowed| allowed == user) {
        return None;
    }
    let message_id = data["id"]
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 512)?;
    let timestamp = data["timestamp"]
        .as_str()
        .and_then(|v| v.parse::<chrono::DateTime<chrono::FixedOffset>>().ok())
        .map(|d| d.timestamp())
        .or_else(|| data["timestamp"].as_i64())?;
    let age = Utc::now().timestamp() - timestamp;
    if age < -60 || age > config.reply_ttl_seconds {
        return None;
    }
    let mut content = data["content"].as_str().unwrap_or("").trim();
    if kind == "group" {
        while content.starts_with("<@") {
            if let Some(end) = content.find('>') {
                content = content[end + 1..].trim_start();
            } else {
                break;
            }
        }
    }
    if content.is_empty() || content.len() > 16000 {
        return None;
    }
    Some(Inbound {
        key: format!("qq:{}", hash(&[app, kind, target, message_id])),
        channel: format!("qq:{}", &hash(&[app, kind, target, user])[..60]),
        kind: kind.into(),
        target: target.into(),
        user: user.into(),
        message_id: message_id.into(),
        text: content.into(),
        timestamp,
    })
}

pub fn chunks(text: &str) -> Vec<String> {
    let text = if text.trim().is_empty() {
        "（任务已完成，无文本回复。）"
    } else {
        text
    };
    let mut result = Vec::new();
    let mut rest = text;
    const LIMIT: usize = 3500;
    const NOTE: &str = "\n[回复已截断，完整结果保存在 pi-runtime 中]";
    while !rest.is_empty() && result.len() < 5 {
        let max = if result.len() == 4 && rest.len() > LIMIT {
            LIMIT - NOTE.len()
        } else {
            LIMIT
        };
        let mut end = rest.len().min(max);
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        let mut part = rest[..end].to_string();
        rest = &rest[end..];
        if result.len() == 4 && !rest.is_empty() {
            part.push_str(NOTE);
        }
        result.push(part);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> Config {
        Config {
            runtime_rpc_url: "http://127.0.0.1:8787/rpc".into(),
            runtime_token_file: "/tmp/token".into(),
            credentials_file: "/tmp/creds".into(),
            health_port: 8788,
            allowed_users: vec!["allowed".into()],
            allowed_groups: vec!["group1".into()],
            poll_interval_ms: 1000,
            reply_ttl_seconds: 240,
            hermes_home: None,
        }
    }
    #[test]
    fn allowlist_is_fail_closed_and_contexts_are_separate() {
        let config = config();
        let mut data = serde_json::json!({"id":"msg", "content":"hello", "timestamp":Utc::now().to_rfc3339(),"author":{"user_openid":"allowed"}});
        let dm = inbound("123", "C2C_MESSAGE_CREATE", &data, &config).unwrap();
        assert!(dm.channel.len() <= 64);
        data["author"]["user_openid"] = "stranger".into();
        assert!(inbound("123", "C2C_MESSAGE_CREATE", &data, &config).is_none());
        data["author"]["member_openid"] = "allowed".into();
        data["group_openid"] = "group1".into();
        data["content"] = "<@!123> hi".into();
        let group = inbound("123", "GROUP_AT_MESSAGE_CREATE", &data, &config).unwrap();
        assert_eq!(group.text, "hi");
        assert_ne!(group.channel, dm.channel);
        data["group_openid"] = "other".into();
        assert!(inbound("123", "GROUP_AT_MESSAGE_CREATE", &data, &config).is_none());
        data["timestamp"] = 1.into();
        assert!(inbound("123", "C2C_MESSAGE_CREATE", &data, &config).is_none());
    }
    #[test]
    fn chunks_bound_bytes_and_preserve_unicode() {
        let short = "中文\u{2028}hello";
        assert_eq!(chunks(short), vec![short]);
        let chunks = chunks(&"中文🦀".repeat(5000));
        assert_eq!(chunks.len(), 5);
        assert!(chunks.iter().all(|c| c.len() <= 3500));
        assert!(chunks.last().unwrap().contains("截断"));
    }
}
