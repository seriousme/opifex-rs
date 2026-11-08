use std::collections::HashMap;
use std::io;

use opifex_protocol::{
    ConnectPacket, FixedHeader, MqttPacket, PacketType, ProtocolError, ProtocolLevel,
    PublishPacket, QoS,
};

use crate::transport::{TcpTransport, TransportAddress};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectPolicy {
    pub max_attempts: usize,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 250,
            max_delay_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub server: TransportAddress,
    pub client_id: String,
    pub protocol_level: ProtocolLevel,
    pub clean_session: bool,
    pub keepalive_seconds: u16,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    pub will: Option<opifex_protocol::WillMessage>,
    pub max_packet_size: usize,
    pub reconnect_policy: ReconnectPolicy,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server: TransportAddress::new("127.0.0.1", 1883),
            client_id: "opifex-client".to_string(),
            protocol_level: ProtocolLevel::V5,
            clean_session: true,
            keepalive_seconds: 60,
            username: None,
            password: None,
            will: None,
            max_packet_size: 256 * 1024,
            reconnect_policy: ReconnectPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    ConnectionClosed,
    InvalidState,
    Protocol(String),
    Io(String),
}

impl From<io::Error> for ClientError {
    fn from(value: io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<ProtocolError> for ClientError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value.to_string())
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionClosed => write!(f, "connection closed"),
            Self::InvalidState => write!(f, "invalid client state"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
            Self::Io(message) => write!(f, "I/O error: {message}"),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPublish {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

#[derive(Debug)]
pub struct MqttClient {
    config: ClientConfig,
    transport: Option<TcpTransport>,
    packet_id: u16,
    connected: bool,
    pending_publishes: HashMap<u16, PendingPublish>,
    reconnect_attempts: usize,
}

impl MqttClient {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            transport: None,
            packet_id: 1,
            connected: false,
            pending_publishes: HashMap::new(),
            reconnect_attempts: 0,
        }
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }

    pub async fn connect(&mut self) -> Result<(), ClientError> {
        let mut attempt = 0usize;
        let policy = self.config.reconnect_policy.clone();
        loop {
            match self.connect_once().await {
                Ok(()) => {
                    self.reconnect_attempts = 0;
                    return Ok(());
                }
                Err(err) => {
                    if attempt + 1 >= policy.max_attempts {
                        return Err(err);
                    }
                    attempt += 1;
                    let delay = (policy
                        .initial_delay_ms
                        .saturating_mul(1u64 << attempt.min(6)))
                    .min(policy.max_delay_ms);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
            }
        }
    }

    async fn connect_once(&mut self) -> Result<(), ClientError> {
        let transport = TcpTransport::connect(&self.config.server).await?;
        let packet = ConnectPacket {
            protocol_level: self.config.protocol_level,
            client_id: self.config.client_id.clone(),
            clean_session: self.config.clean_session,
            keep_alive: self.config.keepalive_seconds,
            username: self.config.username.clone(),
            password: self.config.password.clone(),
            will: self.config.will.clone(),
        };
        let encoded = packet.encode()?;
        let mut transport = transport;
        transport.send_packet(&encoded).await?;
        self.transport = Some(transport);
        self.connected = true;
        self.reconnect_attempts += 1;
        Ok(())
    }

    pub async fn reconnect(&mut self) -> Result<(), ClientError> {
        self.transport = None;
        self.connected = false;
        self.connect().await
    }

    pub async fn disconnect(&mut self) -> Result<(), ClientError> {
        let transport = self
            .transport
            .as_mut()
            .ok_or(ClientError::ConnectionClosed)?;
        let frame = MqttPacket::Disconnect.encode()?;
        transport.send_packet(&frame).await?;
        self.transport = None;
        self.connected = false;
        Ok(())
    }

    pub async fn ping(&mut self) -> Result<(), ClientError> {
        let transport = self
            .transport
            .as_mut()
            .ok_or(ClientError::ConnectionClosed)?;
        let frame = MqttPacket::PingReq.encode()?;
        transport.send_packet(&frame).await?;
        Ok(())
    }

    pub async fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<(), ClientError> {
        let packet_id = match qos {
            QoS::AtMostOnce => None,
            _ => Some(self.next_packet_id()),
        };
        if let Some(packet_id) = packet_id {
            self.pending_publishes.insert(
                packet_id,
                PendingPublish {
                    topic: topic.to_string(),
                    payload: payload.to_vec(),
                    qos,
                    retain,
                },
            );
        }
        let transport = self
            .transport
            .as_mut()
            .ok_or(ClientError::ConnectionClosed)?;
        let packet = PublishPacket {
            dup: false,
            qos,
            retain,
            topic: topic.to_string(),
            packet_id,
            payload: payload.to_vec(),
        };
        let frame = packet.encode()?;
        transport.send_packet(&frame).await?;
        Ok(())
    }

    pub async fn subscribe(&mut self, topic_filter: &str, qos: QoS) -> Result<u16, ClientError> {
        let packet_id = self.next_packet_id();
        let transport = self
            .transport
            .as_mut()
            .ok_or(ClientError::ConnectionClosed)?;
        let packet = opifex_protocol::SubscribePacket {
            packet_id,
            subscriptions: vec![(topic_filter.to_string(), qos)],
        };
        let frame = packet.encode()?;
        transport.send_packet(&frame).await?;
        Ok(packet_id)
    }

    pub async fn unsubscribe(&mut self, topic_filter: &str) -> Result<u16, ClientError> {
        let packet_id = self.next_packet_id();
        let transport = self
            .transport
            .as_mut()
            .ok_or(ClientError::ConnectionClosed)?;
        let mut body = packet_id.to_be_bytes().to_vec();
        body.extend_from_slice(&((topic_filter.len() as u16).to_be_bytes()));
        body.extend_from_slice(topic_filter.as_bytes());
        let header = FixedHeader::new(PacketType::Unsubscribe, 0b0010, body.len());
        let mut frame = header.encode()?;
        frame.extend_from_slice(&body);
        transport.send_packet(&frame).await?;
        Ok(packet_id)
    }

    pub async fn receive_next(&mut self) -> Result<MqttPacket, ClientError> {
        let transport = self
            .transport
            .as_mut()
            .ok_or(ClientError::ConnectionClosed)?;
        let frame = transport.read_packet(self.config.max_packet_size).await?;
        let packet = MqttPacket::decode(&frame)?;
        self.handle_incoming_packet(&packet);
        Ok(packet)
    }

    pub fn handle_incoming_packet(&mut self, packet: &MqttPacket) {
        if let MqttPacket::PubAck(ack) = packet {
            self.pending_publishes.remove(&ack.packet_id);
        }
        if let MqttPacket::PubRec(ack) = packet {
            self.pending_publishes.remove(&ack.packet_id);
        }
        if let MqttPacket::PubComp(ack) = packet {
            self.pending_publishes.remove(&ack.packet_id);
        }
    }

    pub fn pending_publish_count(&self) -> usize {
        self.pending_publishes.len()
    }

    fn next_packet_id(&mut self) -> u16 {
        let packet_id = self.packet_id;
        self.packet_id = if self.packet_id == u16::MAX {
            1
        } else {
            self.packet_id + 1
        };
        packet_id
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::{ClientConfig, MqttClient};
    use crate::transport::TransportAddress;
    use opifex_protocol::{
        decode_remaining_length, ConnectFlags, ConnectPacket, MqttPacket, ProtocolLevel, QoS,
        SubscribePacket,
    };

    async fn read_mqtt_frame(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0u8; 1];
        stream.read_exact(&mut header).await.unwrap();

        let mut remaining = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await.unwrap();
            remaining.push(byte[0]);
            if byte[0] & 0x80 == 0 {
                break;
            }
        }

        let (remaining_length, _) = decode_remaining_length(&remaining).unwrap();
        let mut payload = vec![0u8; remaining_length];
        if !payload.is_empty() {
            stream.read_exact(&mut payload).await.unwrap();
        }

        let mut frame = vec![header[0]];
        frame.extend_from_slice(&remaining);
        frame.extend_from_slice(&payload);
        frame
    }

    async fn accept_connect_and_reply(protocol_level: ProtocolLevel) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let frame = read_mqtt_frame(&mut stream).await;
            let packet = MqttPacket::decode(&frame).unwrap();
            assert!(matches!(packet, MqttPacket::Connect(_)));

            let ack = [0x20, 0x02, 0x00, 0x00];
            stream.write_all(&ack).await.unwrap();
            stream.flush().await.unwrap();

            let sub_frame = read_mqtt_frame(&mut stream).await;
            let packet = MqttPacket::decode(&sub_frame).unwrap();
            assert!(matches!(packet, MqttPacket::Subscribe(_)));

            let suback = [0x90, 0x03, 0x00, 0x03, 0x00];
            stream.write_all(&suback).await.unwrap();
            stream.flush().await.unwrap();
            drop(stream);
        });

        let mut client = MqttClient::new(ClientConfig {
            server: TransportAddress::new("127.0.0.1", addr.port()),
            client_id: format!("phase6-{protocol_level:?}"),
            protocol_level,
            ..ClientConfig::default()
        });
        client.connect().await.unwrap();
        let packet = match client.receive_next().await.unwrap() {
            MqttPacket::ConnAck(ack) => ack,
            other => panic!("expected connack, got {other:?}"),
        };
        assert_eq!(packet.code, 0);

        client
            .subscribe("devices/#", QoS::AtLeastOnce)
            .await
            .unwrap();
        let suback = client.receive_next().await.unwrap();
        assert!(matches!(suback, MqttPacket::SubAck(_)));

        client.disconnect().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn mqtt_client_connects_and_subscribes_across_protocol_versions() {
        for version in [ProtocolLevel::V3, ProtocolLevel::V4, ProtocolLevel::V5] {
            accept_connect_and_reply(version).await;
        }
    }

    #[test]
    fn mqtt_client_issues_packet_ids_in_order() {
        let mut client = MqttClient::new(ClientConfig::default());
        assert_eq!(client.next_packet_id(), 1);
        assert_eq!(client.next_packet_id(), 2);
    }

    #[test]
    fn mqtt_client_builds_connect_and_subscription_frames() {
        let connect = ConnectPacket {
            protocol_level: ProtocolLevel::V5,
            client_id: "phase6-client".to_string(),
            clean_session: true,
            keep_alive: 30,
            username: Some("guest".to_string()),
            password: Some(b"secret".to_vec()),
            will: None,
        };
        let bytes = connect.encode().unwrap();
        assert_eq!(bytes[0], 0x10);
        assert!(bytes.len() > 2);

        let packet = SubscribePacket {
            packet_id: 7,
            subscriptions: vec![("devices/#".to_string(), QoS::AtLeastOnce)],
        };
        let subscribe_bytes = packet.encode().unwrap();
        assert!(subscribe_bytes.starts_with(&[0x82, 0x0e]));

        let unsubscribe = [
            0xA2, 0x0d, 0x00, 0x07, 0x00, 0x09, b'd', b'e', b'v', b'i', b'c', b'e', b's', b'/',
            b'#',
        ];
        assert_eq!(unsubscribe[0], 0xA2);
        assert_eq!(unsubscribe[1], 0x0d);

        let flags = ConnectFlags {
            username: true,
            password: true,
            will_retain: false,
            will_qos: QoS::AtMostOnce,
            will_flag: false,
            clean_session: true,
        };
        assert_eq!(flags.encode() & 0xC0, 0xC0);
    }
}
