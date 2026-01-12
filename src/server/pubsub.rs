use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

use crate::resp::RespValue;

/// Message published to a pub/sub channel
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub channel: String,
    pub payload: Vec<u8>,
}

/// Command queued during MULTI
///
/// Stores a command for deferred execution in a transaction.
/// Commands are buffered as-is and executed atomically on EXEC.
#[derive(Debug, Clone)]
pub struct QueuedCommand {
    /// Command name (uppercase) for dispatcher routing
    pub cmd: String,
    /// Command arguments (raw bytes, no parsing overhead)
    pub args: Vec<Vec<u8>>,
}

/// Tracks the connection's subscription state
#[derive(Debug)]
pub enum ConnectionState {
    /// Normal command processing mode
    Normal,
    /// Subscription mode - receiving messages from channels/patterns
    Subscribed {
        /// Exact channel subscriptions
        channels: HashSet<String>,
        /// Pattern subscriptions
        patterns: HashSet<String>,
        /// Receivers for each exact channel
        channel_receivers: HashMap<String, broadcast::Receiver<PubSubMessage>>,
        /// Receivers for each pattern (pattern, receiver) tuples
        pattern_receivers: Vec<(String, broadcast::Receiver<PubSubMessage>)>,
    },
    /// Transaction mode - buffering commands for atomic execution
    Transaction {
        /// Commands queued for execution
        queue: Vec<QueuedCommand>,
    },
}

impl ConnectionState {
    /// Get total subscription count (exact channels + patterns)
    pub fn subscription_count(&self) -> usize {
        match self {
            ConnectionState::Normal => 0,
            ConnectionState::Subscribed { channels, patterns, .. } => {
                channels.len() + patterns.len()
            }
            ConnectionState::Transaction { .. } => 0,
        }
    }

    /// Check if currently in subscription mode with any subscriptions
    pub fn is_subscribed(&self) -> bool {
        self.subscription_count() > 0
    }

    /// Check if currently in transaction mode
    pub fn is_transaction(&self) -> bool {
        matches!(self, ConnectionState::Transaction { .. })
    }
}

/// PUBLISH channel message
/// Publish message to a channel and all matching patterns.
/// Returns the number of subscribers that received the message.
pub fn cmd_publish(
    args: &[Vec<u8>],
    pubsub_channels: &Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'publish' command");
    }

    let channel = match std::str::from_utf8(&args[0]) {
        Ok(c) => c.to_string(),
        Err(_) => return RespValue::error("invalid channel name"),
    };

    let payload = args[1].clone();
    let msg = PubSubMessage {
        channel: channel.clone(),
        payload,
    };

    let channels_map = pubsub_channels.read().unwrap();

    // Count subscribers to exact channels
    let mut subscriber_count = 0;

    // Send to exact channel subscribers
    if let Some(sender) = channels_map.get(&channel) {
        subscriber_count += sender.send(msg.clone()).unwrap_or(0);
    }

    // Send to pattern subscribers
    for (pattern_key, sender) in channels_map.iter() {
        if pattern_key.starts_with("pattern:") {
            let pattern = &pattern_key[8..]; // Remove "pattern:" prefix
            if glob_match(&channel, pattern) {
                subscriber_count += sender.send(msg.clone()).unwrap_or(0);
            }
        }
    }

    RespValue::Integer(subscriber_count as i64)
}

/// SUBSCRIBE channel [channel ...]
/// Subscribe to one or more channels.
/// Transitions connection to subscription mode.
pub fn cmd_subscribe(
    state: &mut ConnectionState,
    args: &[Vec<u8>],
    pubsub_channels: &Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'subscribe' command");
    }

    // Check for transaction mode
    if let ConnectionState::Transaction { .. } = state {
        return RespValue::error("ERR pub/sub commands not allowed in transaction");
    }

    // Parse channel names
    let mut new_channels = Vec::new();
    for arg in args {
        match std::str::from_utf8(arg) {
            Ok(channel) => new_channels.push(channel.to_string()),
            Err(_) => return RespValue::error("invalid channel name"),
        }
    }

    // Initialize or access subscription state
    if matches!(state, ConnectionState::Normal) {
        // Transition to subscribed mode
        *state = ConnectionState::Subscribed {
            channels: HashSet::new(),
            patterns: HashSet::new(),
            channel_receivers: HashMap::new(),
            pattern_receivers: Vec::new(),
        };
    }

    // Extract mutable references from state
    let (channels, channel_receivers, patterns) = match state {
        ConnectionState::Subscribed {
            channels,
            channel_receivers,
            patterns,
            ..
        } => (channels, channel_receivers, patterns),
        _ => panic!("Expected subscribed state"),
    };

    // Subscribe to each channel
    let mut responses = Vec::new();
    let mut pubsub_map = pubsub_channels.write().unwrap();

    for channel in new_channels {
        // Skip if already subscribed to this channel
        if channels.contains(&channel) {
            continue;
        }

        // Get or create broadcast channel
        let sender = pubsub_map
            .entry(channel.clone())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(128);
                tx
            });

        // Subscribe
        let receiver = sender.subscribe();
        channel_receivers.insert(channel.clone(), receiver);
        channels.insert(channel.clone());

        // Send subscription confirmation
        let count = channels.len() + patterns.len();
        responses.push(RespValue::Array(Some(vec![
            RespValue::from_string("subscribe".to_string()),
            RespValue::from_bytes(channel.into_bytes()),
            RespValue::Integer(count as i64),
        ])));
    }

    // Return the first confirmation (caller will send each)
    if responses.is_empty() {
        RespValue::Array(Some(vec![
            RespValue::from_string("subscribe".to_string()),
            RespValue::null(),
            RespValue::Integer(state.subscription_count() as i64),
        ]))
    } else {
        responses.into_iter().next().unwrap()
    }
}

/// UNSUBSCRIBE [channel ...]
/// Unsubscribe from channels. If no channels specified, unsubscribe from all.
pub fn cmd_unsubscribe(state: &mut ConnectionState, args: &[Vec<u8>]) -> RespValue {
    match state {
        ConnectionState::Normal => {
            // Not in subscription mode
            RespValue::Array(Some(vec![
                RespValue::from_string("unsubscribe".to_string()),
                RespValue::null(),
                RespValue::Integer(0),
            ]))
        }
        ConnectionState::Subscribed {
            channels,
            channel_receivers,
            patterns,
            ..
        } => {
            let mut responses = Vec::new();

            if args.is_empty() {
                // Unsubscribe from all channels
                let all_channels: Vec<String> = channels.iter().cloned().collect();
                for channel in all_channels {
                    channels.remove(&channel);
                    channel_receivers.remove(&channel);

                    // Calculate count from current state
                    let count = channels.len() + patterns.len();
                    responses.push(RespValue::Array(Some(vec![
                        RespValue::from_string("unsubscribe".to_string()),
                        RespValue::from_bytes(channel.into_bytes()),
                        RespValue::Integer(count as i64),
                    ])));
                }
            } else {
                // Unsubscribe from specific channels
                for arg in args {
                    let channel = match std::str::from_utf8(arg) {
                        Ok(c) => c.to_string(),
                        Err(_) => continue,
                    };

                    if channels.remove(&channel) {
                        channel_receivers.remove(&channel);

                        // Calculate count from current state
                        let count = channels.len() + patterns.len();
                        responses.push(RespValue::Array(Some(vec![
                            RespValue::from_string("unsubscribe".to_string()),
                            RespValue::from_bytes(channel.into_bytes()),
                            RespValue::Integer(count as i64),
                        ])));
                    }
                }
            }

            // Exit subscription mode if no subscriptions remain
            if channels.is_empty() && patterns.is_empty() {
                *state = ConnectionState::Normal;
            }

            if responses.is_empty() {
                RespValue::Array(Some(vec![
                    RespValue::from_string("unsubscribe".to_string()),
                    RespValue::null(),
                    RespValue::Integer(0),
                ]))
            } else {
                responses.into_iter().next().unwrap()
            }
        }
        ConnectionState::Transaction { .. } => {
            RespValue::error("ERR pub/sub commands not allowed in transaction")
        }
    }
}

/// PSUBSCRIBE pattern [pattern ...]
/// Subscribe to channels matching glob patterns.
pub fn cmd_psubscribe(
    state: &mut ConnectionState,
    args: &[Vec<u8>],
    pubsub_channels: &Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'psubscribe' command");
    }

    // Check for transaction mode
    if let ConnectionState::Transaction { .. } = state {
        return RespValue::error("ERR pub/sub commands not allowed in transaction");
    }

    // Parse patterns
    let mut new_patterns = Vec::new();
    for arg in args {
        match std::str::from_utf8(arg) {
            Ok(pattern) => new_patterns.push(pattern.to_string()),
            Err(_) => return RespValue::error("invalid pattern"),
        }
    }

    // Initialize or access subscription state
    if matches!(state, ConnectionState::Normal) {
        // Transition to subscribed mode
        *state = ConnectionState::Subscribed {
            channels: HashSet::new(),
            patterns: HashSet::new(),
            channel_receivers: HashMap::new(),
            pattern_receivers: Vec::new(),
        };
    }

    // Extract mutable references from state
    let (patterns, pattern_receivers, channels) = match state {
        ConnectionState::Subscribed {
            patterns,
            pattern_receivers,
            channels,
            ..
        } => (patterns, pattern_receivers, channels),
        _ => panic!("Expected subscribed state"),
    };

    // Subscribe to each pattern
    let mut responses = Vec::new();
    let mut pubsub_map = pubsub_channels.write().unwrap();

    for pattern in new_patterns {
        // Skip if already subscribed to this pattern
        if patterns.contains(&pattern) {
            continue;
        }

        // Create a special channel key for patterns
        let pattern_key = format!("pattern:{}", pattern);
        let sender = pubsub_map
            .entry(pattern_key)
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(128);
                tx
            });

        // Subscribe
        let receiver = sender.subscribe();
        pattern_receivers.push((pattern.clone(), receiver));
        patterns.insert(pattern.clone());

        // Send subscription confirmation
        let count = channels.len() + patterns.len();
        responses.push(RespValue::Array(Some(vec![
            RespValue::from_string("psubscribe".to_string()),
            RespValue::from_bytes(pattern.into_bytes()),
            RespValue::Integer(count as i64),
        ])));
    }

    if responses.is_empty() {
        RespValue::Array(Some(vec![
            RespValue::from_string("psubscribe".to_string()),
            RespValue::null(),
            RespValue::Integer(state.subscription_count() as i64),
        ]))
    } else {
        responses.into_iter().next().unwrap()
    }
}

/// PUNSUBSCRIBE [pattern ...]
/// Unsubscribe from pattern subscriptions.
pub fn cmd_punsubscribe(state: &mut ConnectionState, args: &[Vec<u8>]) -> RespValue {
    match state {
        ConnectionState::Normal => {
            // Not in subscription mode
            RespValue::Array(Some(vec![
                RespValue::from_string("punsubscribe".to_string()),
                RespValue::null(),
                RespValue::Integer(0),
            ]))
        }
        ConnectionState::Subscribed {
            patterns,
            pattern_receivers,
            channels,
            ..
        } => {
            let mut responses = Vec::new();

            if args.is_empty() {
                // Unsubscribe from all patterns
                let all_patterns: Vec<String> = patterns.iter().cloned().collect();
                for pattern in all_patterns {
                    patterns.remove(&pattern);
                    pattern_receivers.retain(|(p, _)| p != &pattern);

                    // Calculate count from current state
                    let count = channels.len() + patterns.len();
                    responses.push(RespValue::Array(Some(vec![
                        RespValue::from_string("punsubscribe".to_string()),
                        RespValue::from_bytes(pattern.into_bytes()),
                        RespValue::Integer(count as i64),
                    ])));
                }
            } else {
                // Unsubscribe from specific patterns
                for arg in args {
                    let pattern = match std::str::from_utf8(arg) {
                        Ok(p) => p.to_string(),
                        Err(_) => continue,
                    };

                    if patterns.remove(&pattern) {
                        pattern_receivers.retain(|(p, _)| p != &pattern);

                        // Calculate count from current state
                        let count = channels.len() + patterns.len();
                        responses.push(RespValue::Array(Some(vec![
                            RespValue::from_string("punsubscribe".to_string()),
                            RespValue::from_bytes(pattern.into_bytes()),
                            RespValue::Integer(count as i64),
                        ])));
                    }
                }
            }

            // Exit subscription mode if no subscriptions remain
            if channels.is_empty() && patterns.is_empty() {
                *state = ConnectionState::Normal;
            }

            if responses.is_empty() {
                RespValue::Array(Some(vec![
                    RespValue::from_string("punsubscribe".to_string()),
                    RespValue::null(),
                    RespValue::Integer(0),
                ]))
            } else {
                responses.into_iter().next().unwrap()
            }
        }
        ConnectionState::Transaction { .. } => {
            RespValue::error("ERR pub/sub commands not allowed in transaction")
        }
    }
}

/// Receive a message from subscribed channels or patterns.
/// Returns Some(RespValue) if a message is available, None otherwise.
pub async fn receive_pubsub_message(state: &mut ConnectionState) -> Option<RespValue> {
    match state {
        ConnectionState::Normal => None,
        ConnectionState::Transaction { .. } => None,
        ConnectionState::Subscribed {
            channel_receivers,
            pattern_receivers,
            ..
        } => {
            // Try to receive from exact channels
            for (channel, rx) in channel_receivers.iter_mut() {
                match rx.try_recv() {
                    Ok(msg) => {
                        return Some(RespValue::Array(Some(vec![
                            RespValue::from_string("message".to_string()),
                            RespValue::from_bytes(channel.as_bytes().to_vec()),
                            RespValue::from_bytes(msg.payload),
                        ])));
                    }
                    Err(broadcast::error::TryRecvError::Empty) => continue,
                    Err(broadcast::error::TryRecvError::Closed) => continue,
                    Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                }
            }

            // Try to receive from pattern subscriptions
            for (pattern, rx) in pattern_receivers.iter_mut() {
                match rx.try_recv() {
                    Ok(msg) => {
                        return Some(RespValue::Array(Some(vec![
                            RespValue::from_string("pmessage".to_string()),
                            RespValue::from_bytes(pattern.as_bytes().to_vec()),
                            RespValue::from_bytes(msg.channel.as_bytes().to_vec()),
                            RespValue::from_bytes(msg.payload),
                        ])));
                    }
                    Err(_) => continue,
                }
            }

            None
        }
    }
}

/// Simple glob pattern matching supporting Redis patterns.
/// Patterns:
/// - `*` matches any sequence of characters
/// - `?` matches a single character
/// - `[abc]` matches any character in the set (basic support)
fn glob_match(text: &str, pattern: &str) -> bool {
    let mut text_idx = 0;
    let mut pattern_idx = 0;
    let text_bytes = text.as_bytes();
    let pattern_bytes = pattern.as_bytes();

    while pattern_idx < pattern_bytes.len() {
        match pattern_bytes[pattern_idx] {
            b'*' => {
                // Skip consecutive '*'
                while pattern_idx < pattern_bytes.len() && pattern_bytes[pattern_idx] == b'*' {
                    pattern_idx += 1;
                }

                // If pattern ends with '*', it matches the rest
                if pattern_idx >= pattern_bytes.len() {
                    return true;
                }

                // Find next match position
                let remaining_pattern = std::str::from_utf8(&pattern_bytes[pattern_idx..])
                    .unwrap_or("");
                while text_idx <= text_bytes.len() {
                    if glob_match(&text[text_idx..], remaining_pattern) {
                        return true;
                    }
                    text_idx += 1;
                }
                return false;
            }
            b'?' => {
                // Match single character
                if text_idx >= text_bytes.len() {
                    return false;
                }
                text_idx += 1;
                pattern_idx += 1;
            }
            b'[' => {
                // Character set matching - basic implementation
                pattern_idx += 1;
                let mut found = false;
                let mut negate = false;

                if pattern_idx < pattern_bytes.len() && pattern_bytes[pattern_idx] == b'^' {
                    negate = true;
                    pattern_idx += 1;
                }

                while pattern_idx < pattern_bytes.len() && pattern_bytes[pattern_idx] != b']' {
                    let ch = pattern_bytes[pattern_idx];
                    if text_idx < text_bytes.len() && text_bytes[text_idx] == ch {
                        found = true;
                    }
                    pattern_idx += 1;
                }

                if found == negate {
                    return false;
                }

                if pattern_idx < pattern_bytes.len() && pattern_bytes[pattern_idx] == b']' {
                    pattern_idx += 1;
                }

                text_idx += 1;
            }
            _ => {
                // Literal character
                if text_idx >= text_bytes.len()
                    || text_bytes[text_idx] != pattern_bytes[pattern_idx]
                {
                    return false;
                }
                text_idx += 1;
                pattern_idx += 1;
            }
        }
    }

    text_idx == text_bytes.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match("events", "events"));
        assert!(!glob_match("events", "other"));
    }

    #[test]
    fn test_glob_match_wildcard() {
        assert!(glob_match("events.login", "events.*"));
        assert!(glob_match("events.logout", "events.*"));
        assert!(!glob_match("other.login", "events.*"));
        assert!(glob_match("hello", "*"));
        assert!(glob_match("", "*"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match("events1", "events?"));
        assert!(glob_match("events2", "events?"));
        assert!(!glob_match("events12", "events?"));
        assert!(!glob_match("events", "events?"));
    }

    #[test]
    fn test_glob_match_multiple_wildcards() {
        assert!(glob_match("a.b.c", "*.*.*"));
        assert!(glob_match("events.user.login", "events.*.*"));
        assert!(!glob_match("a.b", "*.*.*"));
    }

    #[test]
    fn test_glob_match_mixed() {
        assert!(glob_match("events.login123", "events.login???"));
        assert!(!glob_match("events.login", "events.login???"));
    }

    #[test]
    fn test_connection_state_counts() {
        let state = ConnectionState::Normal;
        assert_eq!(state.subscription_count(), 0);
        assert!(!state.is_subscribed());

        let state = ConnectionState::Subscribed {
            channels: {
                let mut set = HashSet::new();
                set.insert("ch1".to_string());
                set.insert("ch2".to_string());
                set
            },
            patterns: {
                let mut set = HashSet::new();
                set.insert("pattern1".to_string());
                set
            },
            channel_receivers: HashMap::new(),
            pattern_receivers: Vec::new(),
        };

        assert_eq!(state.subscription_count(), 3);
        assert!(state.is_subscribed());
    }

    #[test]
    fn test_transaction_state_creation() {
        let state = ConnectionState::Transaction {
            queue: Vec::new(),
        };
        assert!(state.is_transaction());
        assert!(!state.is_subscribed());
        assert_eq!(state.subscription_count(), 0);
    }

    #[test]
    fn test_normal_state_not_transaction() {
        let state = ConnectionState::Normal;
        assert!(!state.is_transaction());
        assert!(!state.is_subscribed());
    }

    #[test]
    fn test_subscribed_state_not_transaction() {
        let state = ConnectionState::Subscribed {
            channels: HashSet::new(),
            patterns: HashSet::new(),
            channel_receivers: HashMap::new(),
            pattern_receivers: Vec::new(),
        };
        assert!(!state.is_transaction());
        assert!(!state.is_subscribed());
    }

    #[test]
    fn test_queued_command_creation() {
        let cmd = QueuedCommand {
            cmd: "SET".to_string(),
            args: vec![b"key".to_vec(), b"value".to_vec()],
        };
        assert_eq!(cmd.cmd, "SET");
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(cmd.args[0], b"key");
        assert_eq!(cmd.args[1], b"value");
    }

    #[test]
    fn test_transaction_queue_operations() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
        };

        if let ConnectionState::Transaction { queue } = &mut state {
            queue.push(QueuedCommand {
                cmd: "SET".to_string(),
                args: vec![b"k1".to_vec(), b"v1".to_vec()],
            });
            queue.push(QueuedCommand {
                cmd: "INCR".to_string(),
                args: vec![b"counter".to_vec()],
            });
            assert_eq!(queue.len(), 2);
        } else {
            panic!("Expected Transaction state");
        }
    }

    #[test]
    fn test_glob_match_empty_pattern() {
        assert!(glob_match("", ""));
        assert!(!glob_match("a", ""));
        assert!(glob_match("a", "*"));
    }

    #[test]
    fn test_glob_match_bracket_set() {
        assert!(glob_match("a", "[abc]"));
        assert!(glob_match("b", "[abc]"));
        assert!(!glob_match("d", "[abc]"));
    }
}
