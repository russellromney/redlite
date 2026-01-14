use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Connection type (mode)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionType {
    Normal,
    PubSub,
    Master,
    Replica,
}

impl ConnectionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConnectionType::Normal => "NORMAL",
            ConnectionType::PubSub => "PUBSUB",
            ConnectionType::Master => "MASTER",
            ConnectionType::Replica => "REPLICA",
        }
    }
}

/// Tracks metadata for a single connection
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Unique connection ID
    pub id: u64,
    /// Connection name (set via CLIENT SETNAME)
    pub name: Option<String>,
    /// Timestamp when connection was created (milliseconds since UNIX_EPOCH)
    pub created_at_ms: i64,
    /// Last command time (milliseconds since UNIX_EPOCH)
    pub last_command_ms: i64,
    /// Connection type (NORMAL, PUBSUB, MASTER, REPLICA)
    pub connection_type: ConnectionType,
    /// Number of commands executed
    pub command_count: u64,
    /// Current database index
    pub db: i32,
    /// Client address
    pub addr: String,
    /// Number of subscriptions (pub/sub)
    pub sub_count: usize,
    /// Number of pattern subscriptions
    pub psub_count: usize,
    /// Transaction depth (0 if not in transaction)
    pub multi_count: i32,
}

impl ConnectionInfo {
    pub fn new(id: u64, addr: SocketAddr) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Self {
            id,
            name: None,
            created_at_ms: now_ms,
            last_command_ms: now_ms,
            connection_type: ConnectionType::Normal,
            command_count: 0,
            db: 0,
            addr: addr.to_string(),
            sub_count: 0,
            psub_count: 0,
            multi_count: 0,
        }
    }

    /// Get connection age in seconds
    pub fn age_seconds(&self) -> i64 {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        (now_ms - self.created_at_ms) / 1000
    }

    /// Get idle time in seconds
    pub fn idle_seconds(&self) -> i64 {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        (now_ms - self.last_command_ms) / 1000
    }

    /// Update last command timestamp
    pub fn touch(&mut self) {
        self.last_command_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
    }

    /// Increment command count
    pub fn increment_command_count(&mut self) {
        self.command_count += 1;
    }

    /// Format connection info for CLIENT LIST output (Redis 7.0+ format)
    pub fn format_list_entry(&self) -> String {
        let flags = self.format_flags();
        format!(
            "id={} addr={} fd=0 name={} age={} idle={} flags={} db={} sub={} psub={} multi={} qbuf=0 qbuf-free=0 argv-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client",
            self.id,
            self.addr,
            self.name.as_deref().unwrap_or(""),
            self.age_seconds(),
            self.idle_seconds(),
            flags,
            self.db,
            self.sub_count,
            self.psub_count,
            self.multi_count
        )
    }

    /// Format flags for CLIENT LIST output
    fn format_flags(&self) -> String {
        let mut flags = String::new();

        match self.connection_type {
            ConnectionType::Normal => flags.push('N'),
            ConnectionType::PubSub => flags.push('P'),
            ConnectionType::Master => flags.push('M'),
            ConnectionType::Replica => flags.push('S'),
        }

        if self.multi_count > 0 {
            flags.push('t');
        }

        flags
    }
}

/// Global connection pool - tracks all active connections
pub struct ConnectionPool {
    connections: Arc<RwLock<HashMap<u64, Arc<RwLock<ConnectionInfo>>>>>,
    next_id: Arc<AtomicU64>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn clone_handle(&self) -> Self {
        Self {
            connections: Arc::clone(&self.connections),
            next_id: Arc::clone(&self.next_id),
        }
    }

    /// Generate a new unique connection ID
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Register a new connection
    pub fn register(&self, info: ConnectionInfo) -> Arc<RwLock<ConnectionInfo>> {
        let conn_info = Arc::new(RwLock::new(info));
        let mut conns = self.connections.write().unwrap();
        conns.insert(conn_info.read().unwrap().id, Arc::clone(&conn_info));
        conn_info
    }

    /// Unregister a connection
    pub fn unregister(&self, id: u64) {
        let mut conns = self.connections.write().unwrap();
        conns.remove(&id);
    }

    /// Get a connection by ID
    pub fn get(&self, id: u64) -> Option<Arc<RwLock<ConnectionInfo>>> {
        let conns = self.connections.read().unwrap();
        conns.get(&id).cloned()
    }

    /// Get all connections
    pub fn get_all(&self) -> Vec<Arc<RwLock<ConnectionInfo>>> {
        let conns = self.connections.read().unwrap();
        conns.values().cloned().collect()
    }

    /// Get all connections with optional type filter
    pub fn get_by_type(&self, conn_type: ConnectionType) -> Vec<Arc<RwLock<ConnectionInfo>>> {
        self.get_all()
            .into_iter()
            .filter(|conn| {
                let info = conn.read().unwrap();
                info.connection_type == conn_type
            })
            .collect()
    }

    /// Get all connections with optional ID filter
    pub fn get_by_ids(&self, ids: &[u64]) -> Vec<Arc<RwLock<ConnectionInfo>>> {
        let conns = self.connections.read().unwrap();
        ids.iter().filter_map(|id| conns.get(id).cloned()).collect()
    }

    /// Format all connections for CLIENT LIST
    pub fn format_list(
        &self,
        filter_type: Option<ConnectionType>,
        filter_ids: Option<&[u64]>,
    ) -> String {
        let conns = match (filter_type, filter_ids) {
            (Some(ty), _) => self.get_by_type(ty),
            (None, Some(ids)) => self.get_by_ids(ids),
            (None, None) => self.get_all(),
        };

        conns
            .iter()
            .map(|conn| {
                let info = conn.read().unwrap();
                info.format_list_entry()
            })
            .collect::<Vec<_>>()
            .join("\r\n")
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_info_new() {
        let addr = "127.0.0.1:12345".parse().unwrap();
        let info = ConnectionInfo::new(1, addr);
        assert_eq!(info.id, 1);
        assert_eq!(info.db, 0);
        assert_eq!(info.command_count, 0);
        assert_eq!(info.sub_count, 0);
        assert_eq!(info.psub_count, 0);
        assert_eq!(info.multi_count, 0);
        assert_eq!(info.connection_type, ConnectionType::Normal);
        assert!(info.name.is_none());
    }

    #[test]
    fn test_connection_type_as_str() {
        assert_eq!(ConnectionType::Normal.as_str(), "NORMAL");
        assert_eq!(ConnectionType::PubSub.as_str(), "PUBSUB");
        assert_eq!(ConnectionType::Master.as_str(), "MASTER");
        assert_eq!(ConnectionType::Replica.as_str(), "REPLICA");
    }

    #[test]
    fn test_connection_pool_next_id() {
        let pool = ConnectionPool::new();
        let id1 = pool.next_id();
        let id2 = pool.next_id();
        let id3 = pool.next_id();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn test_connection_pool_register_unregister() {
        let pool = ConnectionPool::new();
        let addr = "127.0.0.1:12345".parse().unwrap();
        let info = ConnectionInfo::new(1, addr);

        pool.register(info.clone());
        assert!(pool.get(1).is_some());

        pool.unregister(1);
        assert!(pool.get(1).is_none());
    }

    #[test]
    fn test_connection_pool_get_all() {
        let pool = ConnectionPool::new();
        let addr = "127.0.0.1:12345".parse().unwrap();

        pool.register(ConnectionInfo::new(1, addr));
        pool.register(ConnectionInfo::new(2, addr));
        pool.register(ConnectionInfo::new(3, addr));

        let all = pool.get_all();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_connection_pool_get_by_type() {
        let pool = ConnectionPool::new();
        let addr = "127.0.0.1:12345".parse().unwrap();

        let mut info1 = ConnectionInfo::new(1, addr);
        info1.connection_type = ConnectionType::Normal;
        pool.register(info1);

        let mut info2 = ConnectionInfo::new(2, addr);
        info2.connection_type = ConnectionType::PubSub;
        pool.register(info2);

        let mut info3 = ConnectionInfo::new(3, addr);
        info3.connection_type = ConnectionType::Normal;
        pool.register(info3);

        let normal = pool.get_by_type(ConnectionType::Normal);
        assert_eq!(normal.len(), 2);

        let pubsub = pool.get_by_type(ConnectionType::PubSub);
        assert_eq!(pubsub.len(), 1);
    }
}
