use anyhow::{Result, bail, ensure};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Timing {
    Every {
        seconds: u64,
    },
    At {
        at: DateTime<Utc>,
    },
    Cron {
        expression: String,
        timezone: String,
    },
}

impl Timing {
    pub fn next(&self, after: DateTime<Utc>, initial: bool) -> Result<Option<DateTime<Utc>>> {
        match self {
            Self::Every { seconds } => {
                ensure!(
                    (10..=31_536_000).contains(seconds),
                    "seconds must be 10..31536000"
                );
                Ok(Some(after + chrono::Duration::seconds(*seconds as i64)))
            }
            Self::At { at } => {
                if initial && *at <= after {
                    bail!("at must be in the future");
                }
                Ok(if initial { Some(*at) } else { None })
            }
            Self::Cron {
                expression,
                timezone,
            } => {
                ensure!(expression.len() <= 256, "cron expression too long");
                let schedule = Schedule::from_str(expression)?;
                let tz = Tz::from_str(timezone)?;
                let next = schedule
                    .after(&after.with_timezone(&tz))
                    .next()
                    .map(|date| date.with_timezone(&Utc));
                ensure!(!initial || next.is_some(), "cron has no future occurrence");
                // Avoid per-second model calls by mistake.
                if let Some(next) = next
                    && let Some(following) = schedule.after(&next.with_timezone(&tz)).next()
                {
                    ensure!(
                        (following.with_timezone(&Utc) - next).num_seconds() >= 10,
                        "cron must be at least 10 seconds apart"
                    );
                }
                Ok(next)
            }
        }
    }
}

pub fn heartbeat_content(input: &str) -> String {
    let mut comment = false;
    let mut output = String::new();
    for line in input.lines() {
        let mut rest = line;
        let mut visible = String::new();
        loop {
            if comment {
                if let Some(end) = rest.find("-->") {
                    rest = &rest[end + 3..];
                    comment = false;
                } else {
                    break;
                }
            } else if let Some(start) = rest.find("<!--") {
                visible.push_str(&rest[..start]);
                rest = &rest[start + 4..];
                comment = true;
            } else {
                visible.push_str(rest);
                break;
            }
        }
        let trimmed = visible.trim();
        if trimmed.is_empty()
            || trimmed.starts_with('#')
            || trimmed.starts_with("- [x]")
            || trimmed.starts_with("- [X]")
        {
            continue;
        }
        output.push_str(trimmed);
        output.push('\n');
    }
    output.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn heartbeat_skips_template_and_completed_items() {
        assert_eq!(
            heartbeat_content("# Heartbeat\n<!--\nsecret\n-->\n- [x] done"),
            ""
        );
        assert_eq!(
            heartbeat_content("<!-- x -->\n- [ ] inspect logs"),
            "- [ ] inspect logs"
        );
    }
    #[test]
    fn cron_respects_timezone() {
        let now = "2026-01-01T00:00:00Z".parse().unwrap();
        let timing = Timing::Cron {
            expression: "0 0 9 * * *".into(),
            timezone: "Asia/Shanghai".into(),
        };
        assert_eq!(
            timing.next(now, true).unwrap().unwrap().to_rfc3339(),
            "2026-01-01T01:00:00+00:00"
        );
        assert!(Timing::Every { seconds: 1 }.next(now, true).is_err());
        assert!(
            Timing::Cron {
                expression: "* * * * * *".into(),
                timezone: "UTC".into()
            }
            .next(now, true)
            .is_err()
        );
    }
}
