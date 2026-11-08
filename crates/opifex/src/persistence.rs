use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SessionState {
    pub subscriptions: BTreeSet<String>,
    pub inflight_packets: BTreeMap<u16, InflightPacket>,
    pub next_packet_id: u16,
}

impl SessionState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_subscription(&mut self, topic_filter: impl Into<String>) {
        self.subscriptions.insert(topic_filter.into());
    }

    pub fn remove_subscription(&mut self, topic_filter: &str) -> bool {
        self.subscriptions.remove(topic_filter)
    }

    pub fn next_packet_id(&mut self) -> u16 {
        let packet_id = if self.next_packet_id == 0 {
            1
        } else {
            self.next_packet_id
        };
        self.next_packet_id = packet_id.wrapping_add(1);
        packet_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InflightPacket {
    pub packet_id: u16,
    pub qos: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
}

pub trait SessionStore {
    fn get_or_create_session(&mut self, client_id: &str) -> &mut SessionState;
    fn session(&self, client_id: &str) -> Option<&SessionState>;
    fn add_subscription(&mut self, client_id: &str, topic_filter: impl Into<String>) -> bool;
    fn remove_subscription(&mut self, client_id: &str, topic_filter: &str) -> bool;
    fn store_retained_message(&mut self, message: RetainedMessage);
    fn retained_message(&self, topic: &str) -> Option<&RetainedMessage>;
    fn clear_retained_message(&mut self, topic: &str) -> Option<RetainedMessage>;
    fn next_packet_id(&mut self, client_id: &str) -> u16;
    fn track_inflight(&mut self, client_id: &str, packet_id: u16, payload: Vec<u8>, qos: u8);
    fn ack_inflight(&mut self, client_id: &str, packet_id: u16) -> Option<InflightPacket>;
}

#[derive(Debug, Clone, Default)]
pub struct MemoryPersistence {
    sessions: HashMap<String, SessionState>,
    retained_messages: BTreeMap<String, RetainedMessage>,
}

impl MemoryPersistence {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
pub struct SQLitePersistence {
    conn: Connection,
    session_cache: HashMap<String, SessionState>,
    retained_messages: BTreeMap<String, RetainedMessage>,
}

impl SQLitePersistence {
    pub fn new(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        let mut store = Self {
            conn,
            session_cache: HashMap::new(),
            retained_messages: BTreeMap::new(),
        };
        store.initialize_schema()?;
        Ok(store)
    }

    pub fn new_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        let mut store = Self {
            conn,
            session_cache: HashMap::new(),
            retained_messages: BTreeMap::new(),
        };
        store.initialize_schema()?;
        Ok(store)
    }

    fn initialize_schema(&mut self) -> rusqlite::Result<()> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS sessions (
                client_id TEXT PRIMARY KEY,
                next_packet_id INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                client_id TEXT NOT NULL,
                topic_filter TEXT NOT NULL,
                PRIMARY KEY (client_id, topic_filter)
            );

            CREATE TABLE IF NOT EXISTS inflight_packets (
                client_id TEXT NOT NULL,
                packet_id INTEGER NOT NULL,
                qos INTEGER NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY (client_id, packet_id)
            );

            CREATE TABLE IF NOT EXISTS retained_messages (
                topic TEXT PRIMARY KEY,
                payload BLOB NOT NULL,
                qos INTEGER NOT NULL
            );
            ",
        )?;
        Ok(())
    }

    fn sync_session_from_db(&mut self, client_id: &str) -> rusqlite::Result<SessionState> {
        let mut state = SessionState::default();

        let next_packet_id = self
            .conn
            .query_row(
                "SELECT next_packet_id FROM sessions WHERE client_id = ?1",
                params![client_id],
                |row| row.get::<_, u16>(0),
            )
            .optional()?
            .unwrap_or(1);
        state.next_packet_id = next_packet_id;

        let mut subs = self.conn.prepare(
            "SELECT topic_filter FROM subscriptions WHERE client_id = ?1 ORDER BY topic_filter",
        )?;
        for row in subs.query_map(params![client_id], |row| row.get::<_, String>(0))? {
            state.subscriptions.insert(row?);
        }

        let mut inflight = self.conn.prepare(
            "SELECT packet_id, qos, payload FROM inflight_packets WHERE client_id = ?1 ORDER BY packet_id",
        )?;
        for row in inflight.query_map(params![client_id], |row| {
            Ok((
                row.get::<_, u16>(0)?,
                row.get::<_, u8>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })? {
            let (packet_id, qos, payload) = row?;
            state.inflight_packets.insert(
                packet_id,
                InflightPacket {
                    packet_id,
                    qos,
                    payload,
                },
            );
        }

        Ok(state)
    }
}

impl SessionStore for SQLitePersistence {
    fn get_or_create_session(&mut self, client_id: &str) -> &mut SessionState {
        if !self.session_cache.contains_key(client_id) {
            let state = self
                .sync_session_from_db(client_id)
                .unwrap_or_else(|_| SessionState::new());
            self.conn
                .execute(
                    "INSERT OR IGNORE INTO sessions (client_id, next_packet_id) VALUES (?1, ?2)",
                    params![client_id, state.next_packet_id],
                )
                .unwrap();
            self.session_cache.insert(client_id.to_string(), state);
        }
        self.session_cache.get_mut(client_id).unwrap()
    }

    fn session(&self, client_id: &str) -> Option<&SessionState> {
        self.session_cache.get(client_id)
    }

    fn add_subscription(&mut self, client_id: &str, topic_filter: impl Into<String>) -> bool {
        let topic_filter = topic_filter.into();
        let mut session = self
            .session_cache
            .get(client_id)
            .cloned()
            .unwrap_or_else(|| self.sync_session_from_db(client_id).unwrap_or_default());
        let before = session.subscriptions.len();
        let inserted = session.subscriptions.insert(topic_filter.clone());
        let result = if inserted {
            let tx = self.conn.transaction().unwrap();
            tx.execute(
                "INSERT OR IGNORE INTO subscriptions (client_id, topic_filter) VALUES (?1, ?2)",
                params![client_id, topic_filter],
            )
            .unwrap();
            tx.commit().unwrap();
            session.subscriptions.len() != before
        } else {
            false
        };
        self.session_cache.insert(client_id.to_string(), session);
        result
    }

    fn remove_subscription(&mut self, client_id: &str, topic_filter: &str) -> bool {
        let mut session = self
            .session_cache
            .get(client_id)
            .cloned()
            .unwrap_or_else(|| self.sync_session_from_db(client_id).unwrap_or_default());
        let removed = session.subscriptions.remove(topic_filter);
        if removed {
            self.conn
                .execute(
                    "DELETE FROM subscriptions WHERE client_id = ?1 AND topic_filter = ?2",
                    params![client_id, topic_filter],
                )
                .unwrap();
        }
        self.session_cache.insert(client_id.to_string(), session);
        removed
    }

    fn store_retained_message(&mut self, message: RetainedMessage) {
        let tx = self.conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO retained_messages (topic, payload, qos) VALUES (?1, ?2, ?3)
             ON CONFLICT(topic) DO UPDATE SET payload = excluded.payload, qos = excluded.qos",
            params![message.topic, message.payload, message.qos],
        )
        .unwrap();
        tx.commit().unwrap();
        self.retained_messages
            .insert(message.topic.clone(), message);
    }

    fn retained_message(&self, topic: &str) -> Option<&RetainedMessage> {
        self.retained_messages.get(topic)
    }

    fn clear_retained_message(&mut self, topic: &str) -> Option<RetainedMessage> {
        let removed = self.retained_messages.remove(topic);
        if removed.is_some() {
            self.conn
                .execute(
                    "DELETE FROM retained_messages WHERE topic = ?1",
                    params![topic],
                )
                .unwrap();
        }
        removed
    }

    fn next_packet_id(&mut self, client_id: &str) -> u16 {
        let mut session = self
            .session_cache
            .get(client_id)
            .cloned()
            .unwrap_or_else(|| self.sync_session_from_db(client_id).unwrap_or_default());
        let packet_id = if session.next_packet_id == 0 {
            1
        } else {
            session.next_packet_id
        };
        let next = packet_id.wrapping_add(1);
        session.next_packet_id = next;
        self.conn
            .execute(
                "INSERT INTO sessions (client_id, next_packet_id) VALUES (?1, ?2)
                 ON CONFLICT(client_id) DO UPDATE SET next_packet_id = excluded.next_packet_id",
                params![client_id, next],
            )
            .unwrap();
        self.session_cache.insert(client_id.to_string(), session);
        packet_id
    }

    fn track_inflight(&mut self, client_id: &str, packet_id: u16, payload: Vec<u8>, qos: u8) {
        let mut session = self
            .session_cache
            .get(client_id)
            .cloned()
            .unwrap_or_else(|| self.sync_session_from_db(client_id).unwrap_or_default());
        session.inflight_packets.insert(
            packet_id,
            InflightPacket {
                packet_id,
                qos,
                payload: payload.clone(),
            },
        );
        self.conn
            .execute(
                "INSERT OR REPLACE INTO inflight_packets (client_id, packet_id, qos, payload) VALUES (?1, ?2, ?3, ?4)",
                params![client_id, packet_id, qos, payload],
            )
            .unwrap();
        self.session_cache.insert(client_id.to_string(), session);
    }

    fn ack_inflight(&mut self, client_id: &str, packet_id: u16) -> Option<InflightPacket> {
        let mut session = self
            .session_cache
            .get(client_id)
            .cloned()
            .unwrap_or_else(|| self.sync_session_from_db(client_id).unwrap_or_default());
        let removed = session.inflight_packets.remove(&packet_id);
        if removed.is_some() {
            self.conn
                .execute(
                    "DELETE FROM inflight_packets WHERE client_id = ?1 AND packet_id = ?2",
                    params![client_id, packet_id],
                )
                .unwrap();
        }
        self.session_cache.insert(client_id.to_string(), session);
        removed
    }
}

impl SessionStore for MemoryPersistence {
    fn get_or_create_session(&mut self, client_id: &str) -> &mut SessionState {
        self.sessions.entry(client_id.to_string()).or_default()
    }

    fn session(&self, client_id: &str) -> Option<&SessionState> {
        self.sessions.get(client_id)
    }

    fn add_subscription(&mut self, client_id: &str, topic_filter: impl Into<String>) -> bool {
        let topic_filter = topic_filter.into();
        let session = self.get_or_create_session(client_id);
        let before = session.subscriptions.len();
        session.subscriptions.insert(topic_filter);
        session.subscriptions.len() != before
    }

    fn remove_subscription(&mut self, client_id: &str, topic_filter: &str) -> bool {
        if let Some(session) = self.sessions.get_mut(client_id) {
            return session.subscriptions.remove(topic_filter);
        }
        false
    }

    fn store_retained_message(&mut self, message: RetainedMessage) {
        self.retained_messages
            .insert(message.topic.clone(), message);
    }

    fn retained_message(&self, topic: &str) -> Option<&RetainedMessage> {
        self.retained_messages.get(topic)
    }

    fn clear_retained_message(&mut self, topic: &str) -> Option<RetainedMessage> {
        self.retained_messages.remove(topic)
    }

    fn next_packet_id(&mut self, client_id: &str) -> u16 {
        let session = self.get_or_create_session(client_id);
        let packet_id = if session.next_packet_id == 0 {
            1
        } else {
            session.next_packet_id
        };
        session.next_packet_id = packet_id.wrapping_add(1);
        packet_id
    }

    fn track_inflight(&mut self, client_id: &str, packet_id: u16, payload: Vec<u8>, qos: u8) {
        let session = self.get_or_create_session(client_id);
        session.inflight_packets.insert(
            packet_id,
            InflightPacket {
                packet_id,
                qos,
                payload,
            },
        );
    }

    fn ack_inflight(&mut self, client_id: &str, packet_id: u16) -> Option<InflightPacket> {
        let session = self.sessions.get_mut(client_id)?;
        session.inflight_packets.remove(&packet_id)
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryPersistence, SQLitePersistence, SessionStore};

    #[test]
    fn memory_store_tracks_session_state_and_packet_ids() {
        let mut store = MemoryPersistence::new();

        assert_eq!(store.next_packet_id("device-1"), 1);
        assert_eq!(store.next_packet_id("device-1"), 2);

        assert!(store.add_subscription("device-1", "sensor/#"));
        assert!(!store.add_subscription("device-1", "sensor/#"));
        assert!(store
            .session("device-1")
            .unwrap()
            .subscriptions
            .contains("sensor/#"));

        store.track_inflight("device-1", 7, b"payload".to_vec(), 1);
        assert_eq!(store.ack_inflight("device-1", 7).unwrap().packet_id, 7);
    }

    #[test]
    fn memory_store_manages_retained_messages() {
        let mut store = MemoryPersistence::new();
        store.store_retained_message(super::RetainedMessage {
            topic: "sensor/temp".to_string(),
            payload: b"22".to_vec(),
            qos: 0,
        });

        assert_eq!(
            store.retained_message("sensor/temp").unwrap().payload,
            b"22"
        );
        assert!(store.clear_retained_message("sensor/temp").is_some());
        assert!(store.retained_message("sensor/temp").is_none());
    }

    #[test]
    fn sqlite_store_persists_session_state_and_retained_messages() {
        let mut store = SQLitePersistence::new_in_memory().unwrap();

        assert_eq!(store.next_packet_id("device-sqlite"), 1);
        assert_eq!(store.next_packet_id("device-sqlite"), 2);
        assert!(store.add_subscription("device-sqlite", "sensor/#"));
        assert!(store
            .session("device-sqlite")
            .unwrap()
            .subscriptions
            .contains("sensor/#"));

        store.track_inflight("device-sqlite", 5, b"payload".to_vec(), 1);
        assert_eq!(store.ack_inflight("device-sqlite", 5).unwrap().packet_id, 5);

        store.store_retained_message(super::RetainedMessage {
            topic: "sensor/temp".to_string(),
            payload: b"24".to_vec(),
            qos: 0,
        });

        assert_eq!(
            store.retained_message("sensor/temp").unwrap().payload,
            b"24"
        );
    }
}
