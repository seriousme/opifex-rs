use std::collections::HashMap;
use std::time::{Duration, Instant};

use opifex_protocol::{ConnectPacket, ProtocolLevel, QoS};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Authenticating,
    Authenticated,
    Disconnecting,
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub allow_anonymous: bool,
    pub max_connections: usize,
    pub max_packet_size: usize,
    pub require_authentication: bool,
    pub keepalive_seconds: u16,
    pub session_expiry_seconds: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            allow_anonymous: true,
            max_connections: 1024,
            max_packet_size: 256 * 1024,
            require_authentication: false,
            keepalive_seconds: 60,
            session_expiry_seconds: 3600,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientWill {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSession {
    pub client_id: String,
    pub username: Option<String>,
    pub protocol_level: ProtocolLevel,
    pub clean_session: bool,
    pub state: ConnectionState,
    pub authenticated: bool,
    pub subscriptions: Vec<String>,
    pub keepalive: u16,
    pub last_seen: Instant,
    pub will: Option<ClientWill>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMessage {
    pub client_id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectError {
    ClientLimitReached,
    AuthenticationRequired,
    InvalidClientId,
    UnsupportedProtocolLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionTask {
    pub client_id: String,
    pub state: ConnectionState,
    pub keepalive_deadline: Instant,
    pub session_deadline: Instant,
}

impl ConnectionTask {
    pub fn new(
        client_id: impl Into<String>,
        state: ConnectionState,
        keepalive: u16,
        session_expiry_seconds: u64,
    ) -> Self {
        let now = Instant::now();
        let keepalive_seconds = u64::from(keepalive.max(1));
        Self {
            client_id: client_id.into(),
            state,
            keepalive_deadline: now + Duration::from_secs(keepalive_seconds),
            session_deadline: now + Duration::from_secs(session_expiry_seconds),
        }
    }

    pub fn refresh_keepalive(&mut self, now: Instant, keepalive: u16) {
        self.keepalive_deadline = now + Duration::from_secs(u64::from(keepalive.max(1)));
        self.state = ConnectionState::Authenticated;
    }

    pub fn keepalive_due(&self, now: Instant) -> bool {
        now >= self.keepalive_deadline
    }

    pub fn session_expired(&self, now: Instant) -> bool {
        now >= self.session_deadline
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broker {
    config: ServerConfig,
    sessions: HashMap<String, ClientSession>,
    tasks: HashMap<String, ConnectionTask>,
    pending_messages: Vec<BrokerMessage>,
    will_messages: HashMap<String, ClientWill>,
}

impl Broker {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            sessions: HashMap::new(),
            tasks: HashMap::new(),
            pending_messages: Vec::new(),
            will_messages: HashMap::new(),
        }
    }

    pub fn session(&self, client_id: &str) -> Option<&ClientSession> {
        self.sessions.get(client_id)
    }

    pub fn connection_task(&self, client_id: &str) -> Option<&ConnectionTask> {
        self.tasks.get(client_id)
    }

    pub fn connect(
        &mut self,
        client_id: &str,
        packet: &ConnectPacket,
    ) -> Result<&ClientSession, ConnectError> {
        if self.sessions.len() >= self.config.max_connections {
            return Err(ConnectError::ClientLimitReached);
        }
        if client_id.trim().is_empty() {
            return Err(ConnectError::InvalidClientId);
        }
        if packet.protocol_level == ProtocolLevel::V3 {
            return Err(ConnectError::UnsupportedProtocolLevel);
        }

        let session = ClientSession {
            client_id: client_id.to_string(),
            username: packet.username.clone(),
            protocol_level: packet.protocol_level,
            clean_session: packet.clean_session,
            state: ConnectionState::Connecting,
            authenticated: false,
            subscriptions: Vec::new(),
            keepalive: packet.keep_alive,
            last_seen: Instant::now(),
            will: packet.will.as_ref().map(|will| ClientWill {
                topic: will.topic.clone(),
                payload: will.payload.clone(),
                qos: will.qos,
                retain: will.retain,
            }),
        };

        self.sessions.insert(client_id.to_string(), session);
        if let Some(will) = self
            .sessions
            .get(client_id)
            .and_then(|session| session.will.clone())
        {
            self.will_messages.insert(client_id.to_string(), will);
        }
        self.tasks.insert(
            client_id.to_string(),
            ConnectionTask::new(
                client_id,
                ConnectionState::Connecting,
                packet.keep_alive,
                self.config.session_expiry_seconds,
            ),
        );
        self.authenticate(
            client_id,
            packet.username.as_deref(),
            packet.password.as_deref(),
        )?;
        let task = self.tasks.get_mut(client_id).unwrap();
        task.state = ConnectionState::Authenticated;
        task.keepalive_deadline =
            Instant::now() + Duration::from_secs(u64::from(packet.keep_alive.max(1)));
        task.session_deadline =
            Instant::now() + Duration::from_secs(self.config.session_expiry_seconds);
        Ok(self.sessions.get(client_id).unwrap())
    }

    pub fn authenticate(
        &mut self,
        client_id: &str,
        username: Option<&str>,
        password: Option<&[u8]>,
    ) -> Result<(), ConnectError> {
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or(ConnectError::InvalidClientId)?;

        if self.config.require_authentication && username.is_none() && !self.config.allow_anonymous
        {
            return Err(ConnectError::AuthenticationRequired);
        }

        if password.is_some() && username.is_none() {
            return Err(ConnectError::AuthenticationRequired);
        }

        session.username = username.map(str::to_string);
        session.authenticated = true;
        session.state = ConnectionState::Authenticated;
        session.last_seen = Instant::now();
        if let Some(task) = self.tasks.get_mut(client_id) {
            task.state = ConnectionState::Authenticated;
            task.session_deadline =
                session.last_seen + Duration::from_secs(self.config.session_expiry_seconds);
        }
        Ok(())
    }

    pub fn touch_session(&mut self, client_id: &str) -> bool {
        let Some(session) = self.sessions.get_mut(client_id) else {
            return false;
        };
        let now = Instant::now();
        session.last_seen = now;
        if let Some(task) = self.tasks.get_mut(client_id) {
            task.refresh_keepalive(now, session.keepalive);
            task.session_deadline = now + Duration::from_secs(self.config.session_expiry_seconds);
        }
        true
    }

    pub fn set_keepalive(&mut self, client_id: &str, keepalive: u16) -> bool {
        let Some(session) = self.sessions.get_mut(client_id) else {
            return false;
        };
        session.keepalive = keepalive;
        let now = Instant::now();
        session.last_seen = now;
        if let Some(task) = self.tasks.get_mut(client_id) {
            task.refresh_keepalive(now, keepalive);
        }
        true
    }

    pub fn add_subscription(&mut self, client_id: &str, topic_filter: impl Into<String>) -> bool {
        let Some(session) = self.sessions.get_mut(client_id) else {
            return false;
        };
        let value = topic_filter.into();
        if session
            .subscriptions
            .iter()
            .any(|existing| existing == &value)
        {
            return false;
        }
        session.subscriptions.push(value);
        session.last_seen = Instant::now();
        true
    }

    pub fn remove_subscription(&mut self, client_id: &str, topic_filter: &str) -> bool {
        let Some(session) = self.sessions.get_mut(client_id) else {
            return false;
        };
        let before = session.subscriptions.len();
        session
            .subscriptions
            .retain(|existing| existing != topic_filter);
        let removed = session.subscriptions.len() != before;
        if removed {
            session.last_seen = Instant::now();
        }
        removed
    }

    pub fn disconnect(&mut self, client_id: &str) -> Option<ClientSession> {
        let session = self.sessions.remove(client_id)?;
        self.tasks.remove(client_id);
        if let Some(will) = self.will_messages.remove(client_id) {
            self.pending_messages.push(BrokerMessage {
                client_id: client_id.to_string(),
                topic: will.topic,
                payload: will.payload,
                qos: will.qos,
                retain: will.retain,
            });
        }
        Some(ClientSession {
            state: ConnectionState::Disconnected,
            ..session
        })
    }

    pub fn queue_message(
        &mut self,
        client_id: &str,
        topic: &str,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
    ) -> bool {
        if !self.authorize_publish(client_id, topic, qos) {
            return false;
        }
        self.pending_messages.push(BrokerMessage {
            client_id: client_id.to_string(),
            topic: topic.to_string(),
            payload,
            qos,
            retain,
        });
        true
    }

    pub fn take_pending_messages(&mut self, client_id: &str) -> Vec<BrokerMessage> {
        let mut messages = Vec::new();
        self.pending_messages.retain(|message| {
            if message.client_id == client_id {
                messages.push(message.clone());
                false
            } else {
                true
            }
        });
        messages
    }

    pub fn register_will(
        &mut self,
        client_id: &str,
        topic: impl Into<String>,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
    ) -> bool {
        let Some(session) = self.sessions.get_mut(client_id) else {
            return false;
        };
        let will = ClientWill {
            topic: topic.into(),
            payload,
            qos,
            retain,
        };
        session.will = Some(will.clone());
        self.will_messages.insert(client_id.to_string(), will);
        session.last_seen = Instant::now();
        true
    }

    pub fn shutdown(&mut self) -> Vec<String> {
        let client_ids: Vec<_> = self.sessions.keys().cloned().collect();
        for client_id in &client_ids {
            self.disconnect(client_id);
        }
        client_ids
    }

    pub fn advance_timers(&mut self) -> Vec<String> {
        let due: Vec<_> = self
            .sessions
            .iter()
            .filter_map(|(client_id, session)| {
                let keepalive_due = session.keepalive > 0
                    && session.last_seen.elapsed()
                        >= Duration::from_secs(u64::from(session.keepalive));
                if keepalive_due
                    || session.last_seen.elapsed()
                        > Duration::from_secs(self.config.session_expiry_seconds)
                {
                    Some(client_id.clone())
                } else {
                    None
                }
            })
            .collect();

        for client_id in &due {
            self.disconnect(client_id);
        }
        due
    }

    pub fn is_expired(&self, client_id: &str) -> bool {
        let Some(session) = self.sessions.get(client_id) else {
            return true;
        };
        session.last_seen.elapsed() > Duration::from_secs(self.config.session_expiry_seconds)
    }

    pub fn prune_expired(&mut self) -> Vec<String> {
        let expired: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, session)| {
                session.last_seen.elapsed()
                    > Duration::from_secs(self.config.session_expiry_seconds)
            })
            .map(|(client_id, _)| client_id.clone())
            .collect();

        for client_id in &expired {
            self.sessions.remove(client_id);
            self.tasks.remove(client_id);
            if let Some(will) = self.will_messages.remove(client_id) {
                self.pending_messages.push(BrokerMessage {
                    client_id: client_id.clone(),
                    topic: will.topic,
                    payload: will.payload,
                    qos: will.qos,
                    retain: will.retain,
                });
            }
        }
        expired
    }

    pub fn authorize_publish(&self, client_id: &str, topic: &str, qos: QoS) -> bool {
        let Some(session) = self.sessions.get(client_id) else {
            return false;
        };
        if !session.authenticated || session.state != ConnectionState::Authenticated {
            return false;
        }
        !topic.is_empty() && matches!(qos, QoS::AtMostOnce | QoS::AtLeastOnce | QoS::ExactlyOnce)
    }

    pub fn authorize_subscribe(&self, client_id: &str, topic_filter: &str) -> bool {
        let Some(session) = self.sessions.get(client_id) else {
            return false;
        };
        if !session.authenticated || session.state != ConnectionState::Authenticated {
            return false;
        }
        !topic_filter.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{Broker, ConnectionState, ServerConfig};
    use opifex_protocol::{ConnectPacket, ProtocolLevel};

    #[test]
    fn broker_tracks_authentication_and_disconnect_lifecycle() {
        let mut broker = Broker::new(ServerConfig::default());
        let packet = ConnectPacket {
            protocol_level: ProtocolLevel::V5,
            client_id: "broker-client".to_string(),
            clean_session: true,
            keep_alive: 30,
            username: Some("user".to_string()),
            password: Some(b"secret".to_vec()),
            will: None,
        };

        let session = broker.connect("broker-client", &packet).unwrap();
        assert_eq!(session.state, ConnectionState::Authenticated);
        assert!(session.authenticated);
        assert!(broker.authorize_publish(
            "broker-client",
            "home/lights",
            opifex_protocol::QoS::AtLeastOnce
        ));

        let disconnected = broker.disconnect("broker-client").unwrap();
        assert_eq!(disconnected.state, ConnectionState::Disconnected);
        assert!(broker.session("broker-client").is_none());
    }

    #[test]
    fn broker_rejects_unacceptable_connect_state() {
        let mut broker = Broker::new(ServerConfig {
            require_authentication: true,
            allow_anonymous: false,
            ..ServerConfig::default()
        });

        let packet = ConnectPacket {
            protocol_level: ProtocolLevel::V5,
            client_id: "blocked".to_string(),
            clean_session: true,
            keep_alive: 30,
            username: None,
            password: None,
            will: None,
        };

        let err = broker.connect("blocked", &packet).unwrap_err();
        assert_eq!(err, super::ConnectError::AuthenticationRequired);
    }

    #[test]
    fn broker_tracks_subscription_and_expiry_state() {
        let mut broker = Broker::new(ServerConfig::default());
        let packet = ConnectPacket {
            protocol_level: ProtocolLevel::V5,
            client_id: "sub-client".to_string(),
            clean_session: true,
            keep_alive: 30,
            username: Some("user".to_string()),
            password: Some(b"pass".to_vec()),
            will: None,
        };

        broker.connect("sub-client", &packet).unwrap();
        assert!(broker.add_subscription("sub-client", "sensor/#"));
        assert!(!broker.add_subscription("sub-client", "sensor/#"));
        assert!(broker.remove_subscription("sub-client", "sensor/#"));

        let session = broker.sessions.get_mut("sub-client").unwrap();
        session.last_seen = std::time::Instant::now() - std::time::Duration::from_secs(60_000);
        assert!(broker.is_expired("sub-client"));
        let expired = broker.prune_expired();
        assert!(expired.contains(&"sub-client".to_string()));
    }

    #[test]
    fn broker_tracks_connection_tasks_and_keepalive_timers() {
        let mut broker = Broker::new(ServerConfig::default());
        let packet = ConnectPacket {
            protocol_level: ProtocolLevel::V5,
            client_id: "timer-client".to_string(),
            clean_session: true,
            keep_alive: 5,
            username: Some("timer".to_string()),
            password: Some(b"secret".to_vec()),
            will: None,
        };

        broker.connect("timer-client", &packet).unwrap();
        let task = broker.connection_task("timer-client").unwrap();
        assert_eq!(task.state, ConnectionState::Authenticated);
        assert!(task.keepalive_deadline > std::time::Instant::now());

        let session = broker.sessions.get_mut("timer-client").unwrap();
        session.keepalive = 1;
        session.last_seen = std::time::Instant::now() - std::time::Duration::from_secs(2);
        let timed_out = broker.advance_timers();
        assert!(timed_out.contains(&"timer-client".to_string()));
        assert!(broker.session("timer-client").is_none());
    }

    #[test]
    fn broker_tracks_pending_message_queue_and_wills() {
        let mut broker = Broker::new(ServerConfig::default());
        let packet = ConnectPacket {
            protocol_level: ProtocolLevel::V5,
            client_id: "queue-client".to_string(),
            clean_session: true,
            keep_alive: 30,
            username: Some("queue".to_string()),
            password: Some(b"pass".to_vec()),
            will: Some(opifex_protocol::WillMessage {
                topic: "device/offline".to_string(),
                payload: b"gone".to_vec(),
                qos: opifex_protocol::QoS::AtMostOnce,
                retain: false,
            }),
        };

        broker.connect("queue-client", &packet).unwrap();
        assert!(broker.queue_message(
            "queue-client",
            "presence",
            b"online".to_vec(),
            opifex_protocol::QoS::AtLeastOnce,
            false,
        ));

        let pending = broker.take_pending_messages("queue-client");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].topic, "presence");

        broker.disconnect("queue-client");
        let will = broker.take_pending_messages("queue-client");
        assert_eq!(will[0].topic, "device/offline");
        assert_eq!(will[0].payload, b"gone".as_slice());
    }
}
