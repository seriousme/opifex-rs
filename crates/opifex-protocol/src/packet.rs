use std::convert::TryFrom;

use crate::{length::decode_remaining_length, ProtocolError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    PubRec = 5,
    PubRel = 6,
    PubComp = 7,
    Subscribe = 8,
    SubAck = 9,
    Unsubscribe = 10,
    UnsubAck = 11,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
    Auth = 15,
}

impl TryFrom<u8> for PacketType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Connect),
            2 => Ok(Self::ConnAck),
            3 => Ok(Self::Publish),
            4 => Ok(Self::PubAck),
            5 => Ok(Self::PubRec),
            6 => Ok(Self::PubRel),
            7 => Ok(Self::PubComp),
            8 => Ok(Self::Subscribe),
            9 => Ok(Self::SubAck),
            10 => Ok(Self::Unsubscribe),
            11 => Ok(Self::UnsubAck),
            12 => Ok(Self::PingReq),
            13 => Ok(Self::PingResp),
            14 => Ok(Self::Disconnect),
            15 => Ok(Self::Auth),
            _ => Err(ProtocolError::InvalidPacketType(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolLevel {
    V3 = 3,
    V4 = 4,
    V5 = 5,
}

impl TryFrom<u8> for ProtocolLevel {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            3 => Ok(Self::V3),
            4 => Ok(Self::V4),
            5 => Ok(Self::V5),
            _ => Err(ProtocolError::InvalidProtocolLevel(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            _ => Err(ProtocolError::MalformedPayload(format!(
                "invalid QoS value: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasonCode {
    Success = 0,
    UnspecifiedError = 128,
}

impl TryFrom<u8> for ReasonCode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Success),
            128 => Ok(Self::UnspecifiedError),
            _ => Err(ProtocolError::MalformedPayload(format!(
                "unsupported reason code: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectFlags {
    pub username: bool,
    pub password: bool,
    pub will_retain: bool,
    pub will_qos: QoS,
    pub will_flag: bool,
    pub clean_session: bool,
}

impl ConnectFlags {
    pub fn encode(&self) -> u8 {
        let mut flags = 0u8;
        if self.username {
            flags |= 0x80;
        }
        if self.password {
            flags |= 0x40;
        }
        if self.will_retain {
            flags |= 0x20;
        }
        flags |= (self.will_qos as u8) << 3;
        if self.will_flag {
            flags |= 0x04;
        }
        if self.clean_session {
            flags |= 0x02;
        }
        flags
    }

    pub fn decode(byte: u8) -> Result<Self, ProtocolError> {
        if byte & 0x01 != 0 {
            return Err(ProtocolError::MalformedPayload(
                "reserved CONNECT flag bit must be zero".to_string(),
            ));
        }

        Ok(Self {
            username: (byte & 0x80) != 0,
            password: (byte & 0x40) != 0,
            will_retain: (byte & 0x20) != 0,
            will_qos: QoS::try_from((byte >> 3) & 0x03)?,
            will_flag: (byte & 0x04) != 0,
            clean_session: (byte & 0x02) != 0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WillMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectPacket {
    pub protocol_level: ProtocolLevel,
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive: u16,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    pub will: Option<WillMessage>,
}

impl ConnectPacket {
    fn mqtt_string(value: &str) -> Vec<u8> {
        let bytes = value.as_bytes();
        let mut out = Vec::with_capacity(2 + bytes.len());
        out.extend_from_slice(&((bytes.len() as u16).to_be_bytes()));
        out.extend_from_slice(bytes);
        out
    }

    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let protocol_name = match self.protocol_level {
            ProtocolLevel::V3 => "MQIsdp",
            ProtocolLevel::V4 | ProtocolLevel::V5 => "MQTT",
        };

        let mut body = Vec::new();
        body.extend(Self::mqtt_string(protocol_name));
        body.push(self.protocol_level as u8);

        let flags = ConnectFlags {
            username: self.username.is_some(),
            password: self.password.is_some(),
            will_retain: self.will.as_ref().map(|w| w.retain).unwrap_or(false),
            will_qos: self.will.as_ref().map(|w| w.qos).unwrap_or(QoS::AtMostOnce),
            will_flag: self.will.is_some(),
            clean_session: self.clean_session,
        };
        body.push(flags.encode());
        body.extend_from_slice(&self.keep_alive.to_be_bytes());

        if self.protocol_level == ProtocolLevel::V5 {
            body.push(0);
        }

        body.extend(Self::mqtt_string(&self.client_id));

        if let Some(will) = &self.will {
            body.extend(Self::mqtt_string(&will.topic));
            let payload_len = will.payload.len() as u16;
            body.extend_from_slice(&payload_len.to_be_bytes());
            body.extend_from_slice(&will.payload);
        }

        if let Some(username) = &self.username {
            body.extend(Self::mqtt_string(username));
        }
        if let Some(password) = &self.password {
            let mut pass = Vec::with_capacity(2 + password.len());
            pass.extend_from_slice(&(password.len() as u16).to_be_bytes());
            pass.extend_from_slice(password);
            body.extend(pass);
        }

        let header = FixedHeader::new(PacketType::Connect, 0, body.len());
        let mut encoded = header.encode()?;
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        let mut cursor = 0usize;
        let protocol_name = decode_mqtt_string(bytes, &mut cursor)?;
        let raw_level = read_u8(bytes, &mut cursor)?;
        let protocol_level = ProtocolLevel::try_from(raw_level)?;
        let raw_flags = read_u8(bytes, &mut cursor)?;
        let flags = ConnectFlags::decode(raw_flags)?;
        let keep_alive = read_u16(bytes, &mut cursor)?;

        if protocol_level == ProtocolLevel::V5 {
            let _props = read_u8(bytes, &mut cursor)?;
        }

        let client_id = decode_mqtt_string(bytes, &mut cursor)?;
        let mut will = None;
        if flags.will_flag {
            let topic = decode_mqtt_string(bytes, &mut cursor)?;
            let payload_len = read_u16(bytes, &mut cursor)? as usize;
            let payload_end = cursor + payload_len;
            if payload_end > bytes.len() {
                return Err(ProtocolError::TruncatedPacket);
            }
            let payload = bytes[cursor..payload_end].to_vec();
            cursor = payload_end;
            will = Some(WillMessage {
                topic,
                payload,
                qos: flags.will_qos,
                retain: flags.will_retain,
            });
        }

        let username = if flags.username {
            Some(decode_mqtt_string(bytes, &mut cursor)?)
        } else {
            None
        };

        let password = if flags.password {
            let len = read_u16(bytes, &mut cursor)? as usize;
            let end = cursor + len;
            if end > bytes.len() {
                return Err(ProtocolError::TruncatedPacket);
            }
            let bytes_out = bytes[cursor..end].to_vec();
            cursor = end;
            Some(bytes_out)
        } else {
            None
        };

        if cursor != bytes.len() {
            return Err(ProtocolError::MalformedPayload(
                "extra CONNECT bytes remain after decoding".to_string(),
            ));
        }

        if !(protocol_name == "MQTT" || protocol_name == "MQIsdp") {
            return Err(ProtocolError::MalformedPayload(
                "invalid MQTT protocol name".to_string(),
            ));
        }

        Ok(Self {
            protocol_level,
            client_id,
            clean_session: flags.clean_session,
            keep_alive,
            username,
            password,
            will,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPacket {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: String,
    pub packet_id: Option<u16>,
    pub payload: Vec<u8>,
}

impl PublishPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut body = Vec::new();
        body.extend_from_slice(&((self.topic.len() as u16).to_be_bytes()));
        body.extend_from_slice(self.topic.as_bytes());

        if self.qos != QoS::AtMostOnce {
            let packet_id = self.packet_id.ok_or_else(|| {
                ProtocolError::MalformedPayload("packet id is required for QoS > 0".to_string())
            })?;
            body.extend_from_slice(&packet_id.to_be_bytes());
        }

        body.extend_from_slice(&self.payload);

        let flags = (if self.dup { 0x08 } else { 0 })
            | ((self.qos as u8) << 1)
            | (if self.retain { 1 } else { 0 });
        let header = FixedHeader::new(PacketType::Publish, flags, body.len());
        let mut encoded = header.encode()?;
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8], flags: u8) -> Result<Self, ProtocolError> {
        let dup = flags & 0x08 != 0;
        let qos = QoS::try_from((flags >> 1) & 0x03)?;
        let retain = flags & 0x01 != 0;

        let mut cursor = 0usize;
        let topic = decode_mqtt_string(bytes, &mut cursor)?;

        let packet_id = if qos != QoS::AtMostOnce {
            Some(read_u16(bytes, &mut cursor)?)
        } else {
            None
        };

        let payload = bytes[cursor..].to_vec();
        Ok(Self {
            dup,
            qos,
            retain,
            topic,
            packet_id,
            payload,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PingreqPacket;

impl PingreqPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let header = FixedHeader::new(PacketType::PingReq, 0, 0);
        header.encode()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAckPacket {
    pub session_present: bool,
    pub code: u8,
    pub reason_code: Option<ReasonCode>,
}

impl ConnAckPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut body = vec![if self.session_present { 1 } else { 0 }];
        body.push(self.code);
        if let Some(reason) = self.reason_code {
            body.push(reason as u8);
            body.push(0);
        }
        let header = FixedHeader::new(PacketType::ConnAck, 0, body.len());
        let mut encoded = header.encode()?;
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() < 2 {
            return Err(ProtocolError::TruncatedPacket);
        }
        let session_present = bytes[0] != 0;
        let code = bytes[1];
        let reason = if bytes.len() >= 4 {
            Some(ReasonCode::try_from(bytes[2])?)
        } else {
            None
        };
        Ok(Self {
            session_present,
            code,
            reason_code: reason,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubAckPacket {
    pub packet_id: u16,
}

impl PubAckPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let body = self.packet_id.to_be_bytes().to_vec();
        let header = FixedHeader::new(PacketType::PubAck, 0, body.len());
        let mut encoded = header.encode()?;
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribePacket {
    pub packet_id: u16,
    pub subscriptions: Vec<(String, QoS)>,
}

impl SubscribePacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut body = self.packet_id.to_be_bytes().to_vec();
        for (topic, qos) in &self.subscriptions {
            body.extend_from_slice(&((topic.len() as u16).to_be_bytes()));
            body.extend_from_slice(topic.as_bytes());
            body.push(*qos as u8);
        }
        let header = FixedHeader::new(PacketType::Subscribe, 0b0010, body.len());
        let mut encoded = header.encode()?;
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() < 2 {
            return Err(ProtocolError::TruncatedPacket);
        }
        let mut cursor = 0usize;
        let packet_id = read_u16(bytes, &mut cursor)?;
        let mut subscriptions = Vec::new();
        while cursor < bytes.len() {
            let topic = decode_mqtt_string(bytes, &mut cursor)?;
            let qos = QoS::try_from(read_u8(bytes, &mut cursor)?)?;
            subscriptions.push((topic, qos));
        }
        Ok(Self {
            packet_id,
            subscriptions,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAckPacket {
    pub packet_id: u16,
    pub reason_codes: Vec<u8>,
}

impl SubAckPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut body = self.packet_id.to_be_bytes().to_vec();
        body.extend_from_slice(&self.reason_codes);
        let header = FixedHeader::new(PacketType::SubAck, 0, body.len());
        let mut encoded = header.encode()?;
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAckPacket {
    pub packet_id: u16,
}

impl UnsubAckPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let body = self.packet_id.to_be_bytes().to_vec();
        let header = FixedHeader::new(PacketType::UnsubAck, 0, body.len());
        let mut encoded = header.encode()?;
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PingRespPacket;

impl PingRespPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let header = FixedHeader::new(PacketType::PingResp, 0, 0);
        header.encode()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct DisconnectPacket {
    pub reason_code: Option<ReasonCode>,
}

#[allow(dead_code)]
impl DisconnectPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let body = if let Some(code) = self.reason_code {
            vec![code as u8]
        } else {
            Vec::new()
        };
        let header = FixedHeader::new(PacketType::Disconnect, 0, body.len());
        let mut encoded = header.encode()?;
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct AuthPacket {
    pub reason_code: Option<ReasonCode>,
}

#[allow(dead_code)]
impl AuthPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let body = if let Some(code) = self.reason_code {
            vec![code as u8]
        } else {
            Vec::new()
        };
        let header = FixedHeader::new(PacketType::Auth, 0, body.len());
        let mut encoded = header.encode()?;
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttPacket {
    Connect(ConnectPacket),
    ConnAck(ConnAckPacket),
    Publish(PublishPacket),
    PubAck(PubAckPacket),
    PubRec(PubAckPacket),
    PubRel(PubAckPacket),
    PubComp(PubAckPacket),
    Subscribe(SubscribePacket),
    SubAck(SubAckPacket),
    Unsubscribe(SubscribePacket),
    UnsubAck(UnsubAckPacket),
    PingReq,
    PingResp,
    Disconnect,
    Auth,
}

impl MqttPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        match self {
            Self::Connect(packet) => packet.encode(),
            Self::ConnAck(packet) => packet.encode(),
            Self::Publish(packet) => packet.encode(),
            Self::PubAck(packet) => packet.encode(),
            Self::PubRec(packet) => {
                let body = packet.packet_id.to_be_bytes().to_vec();
                let header = FixedHeader::new(PacketType::PubRec, 0, body.len());
                let mut encoded = header.encode()?;
                encoded.extend_from_slice(&body);
                Ok(encoded)
            }
            Self::PubRel(packet) => {
                let body = packet.packet_id.to_be_bytes().to_vec();
                let header = FixedHeader::new(PacketType::PubRel, 0b0010, body.len());
                let mut encoded = header.encode()?;
                encoded.extend_from_slice(&body);
                Ok(encoded)
            }
            Self::PubComp(packet) => packet.encode(),
            Self::Subscribe(packet) => packet.encode(),
            Self::SubAck(packet) => packet.encode(),
            Self::Unsubscribe(_) => {
                let header = FixedHeader::new(PacketType::Unsubscribe, 0b0010, 0);
                header.encode()
            }
            Self::UnsubAck(packet) => packet.encode(),
            Self::PingReq => PingreqPacket.encode(),
            Self::PingResp => PingRespPacket.encode(),
            Self::Disconnect => {
                let header = FixedHeader::new(PacketType::Disconnect, 0, 0);
                header.encode()
            }
            Self::Auth => {
                let header = FixedHeader::new(PacketType::Auth, 0, 0);
                header.encode()
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        let (header, body_start, body_end) = FixedHeader::decode(bytes)?;
        let payload = &bytes[body_start..body_end];

        match header.packet_type {
            PacketType::Connect => Ok(Self::Connect(ConnectPacket::decode(payload)?)),
            PacketType::ConnAck => Ok(Self::ConnAck(ConnAckPacket::decode(payload)?)),
            PacketType::Publish => Ok(Self::Publish(PublishPacket::decode(payload, header.flags)?)),
            PacketType::PubAck => {
                let packet_id = read_u16(payload, &mut 0)?;
                Ok(Self::PubAck(PubAckPacket { packet_id }))
            }
            PacketType::PubRec => {
                let packet_id = read_u16(payload, &mut 0)?;
                Ok(Self::PubRec(PubAckPacket { packet_id }))
            }
            PacketType::PubRel => {
                let packet_id = read_u16(payload, &mut 0)?;
                Ok(Self::PubRel(PubAckPacket { packet_id }))
            }
            PacketType::PubComp => {
                let packet_id = read_u16(payload, &mut 0)?;
                Ok(Self::PubComp(PubAckPacket { packet_id }))
            }
            PacketType::Subscribe => Ok(Self::Subscribe(SubscribePacket::decode(payload)?)),
            PacketType::SubAck => {
                let mut cursor = 0usize;
                let packet_id = read_u16(payload, &mut cursor)?;
                let reason_codes = payload[cursor..].to_vec();
                Ok(Self::SubAck(SubAckPacket {
                    packet_id,
                    reason_codes,
                }))
            }
            PacketType::Unsubscribe => Ok(Self::Unsubscribe(SubscribePacket::decode(payload)?)),
            PacketType::UnsubAck => {
                let packet_id = read_u16(payload, &mut 0)?;
                Ok(Self::UnsubAck(UnsubAckPacket { packet_id }))
            }
            PacketType::PingReq => Ok(Self::PingReq),
            PacketType::PingResp => Ok(Self::PingResp),
            PacketType::Disconnect => Ok(Self::Disconnect),
            PacketType::Auth => Ok(Self::Auth),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedHeader {
    pub packet_type: PacketType,
    pub flags: u8,
    pub remaining_length: usize,
    pub body_start: usize,
}

impl FixedHeader {
    pub fn new(packet_type: PacketType, flags: u8, remaining_length: usize) -> Self {
        Self {
            packet_type,
            flags,
            remaining_length,
            body_start: 1,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut encoded = vec![((self.packet_type as u8) << 4) | (self.flags & 0x0f)];
        encoded.extend_from_slice(&crate::length::encode_remaining_length(
            self.remaining_length,
        )?);
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<(Self, usize, usize), ProtocolError> {
        if bytes.is_empty() {
            return Err(ProtocolError::InvalidFixedHeader);
        }

        let packet_type = PacketType::try_from(bytes[0] >> 4)?;
        let flags = bytes[0] & 0x0f;
        let (remaining_length, length_bytes_used) = decode_remaining_length(&bytes[1..])?;
        let body_start = 1 + length_bytes_used;
        let body_end = body_start + remaining_length;

        if body_end > bytes.len() {
            return Err(ProtocolError::TruncatedPacket);
        }

        Ok((
            Self {
                packet_type,
                flags,
                remaining_length,
                body_start,
            },
            body_start,
            body_end,
        ))
    }
}

fn decode_mqtt_string(bytes: &[u8], cursor: &mut usize) -> Result<String, ProtocolError> {
    let len = read_u16(bytes, cursor)? as usize;
    let end = *cursor + len;
    if end > bytes.len() {
        return Err(ProtocolError::TruncatedPacket);
    }
    let value =
        String::from_utf8(bytes[*cursor..end].to_vec()).map_err(ProtocolError::InvalidUtf8)?;
    *cursor = end;
    Ok(value)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, ProtocolError> {
    if *cursor >= bytes.len() {
        return Err(ProtocolError::TruncatedPacket);
    }
    let value = bytes[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, ProtocolError> {
    if *cursor + 2 > bytes.len() {
        return Err(ProtocolError::TruncatedPacket);
    }
    let value = u16::from_be_bytes([bytes[*cursor], bytes[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectPacket, MqttPacket, ProtocolLevel, PublishPacket, QoS, SubAckPacket, SubscribePacket,
    };

    #[test]
    fn connect_packet_round_trips() {
        let packet = ConnectPacket {
            protocol_level: ProtocolLevel::V4,
            client_id: "mqtt-client".to_string(),
            clean_session: true,
            keep_alive: 30,
            username: Some("user".to_string()),
            password: Some(b"secret".to_vec()),
            will: None,
        };

        let encoded = packet.encode().unwrap();
        let decoded = MqttPacket::decode(&encoded).unwrap();
        match decoded {
            MqttPacket::Connect(decoded) => {
                assert_eq!(decoded.client_id, "mqtt-client");
                assert!(decoded.clean_session);
                assert_eq!(decoded.keep_alive, 30);
                assert_eq!(decoded.username.as_deref(), Some("user"));
            }
            _ => panic!("expected connect packet"),
        }
    }

    #[test]
    fn publish_packet_round_trips() {
        let packet = PublishPacket {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "home/lights".to_string(),
            packet_id: Some(7),
            payload: b"on".to_vec(),
        };

        let encoded = packet.encode().unwrap();
        let decoded = MqttPacket::decode(&encoded).unwrap();
        match decoded {
            MqttPacket::Publish(decoded) => {
                assert_eq!(decoded.topic, "home/lights");
                assert_eq!(decoded.packet_id, Some(7));
                assert_eq!(decoded.payload, b"on");
                assert_eq!(decoded.qos, QoS::AtLeastOnce);
            }
            _ => panic!("expected publish packet"),
        }
    }

    #[test]
    fn subscribe_packet_round_trips() {
        let packet = SubscribePacket {
            packet_id: 11,
            subscriptions: vec![("sensor/+/temp".to_string(), QoS::AtLeastOnce)],
        };
        let encoded = packet.encode().unwrap();
        let decoded = MqttPacket::decode(&encoded).unwrap();
        match decoded {
            MqttPacket::Subscribe(decoded) => {
                assert_eq!(decoded.packet_id, 11);
                assert_eq!(decoded.subscriptions[0].0, "sensor/+/temp");
                assert_eq!(decoded.subscriptions[0].1, QoS::AtLeastOnce);
            }
            _ => panic!("expected subscribe packet"),
        }
    }

    #[test]
    fn suback_packet_round_trips() {
        let packet = SubAckPacket {
            packet_id: 55,
            reason_codes: vec![0, 1, 2],
        };
        let encoded = packet.encode().unwrap();
        let decoded = MqttPacket::decode(&encoded).unwrap();
        match decoded {
            MqttPacket::SubAck(decoded) => {
                assert_eq!(decoded.packet_id, 55);
                assert_eq!(decoded.reason_codes, vec![0, 1, 2]);
            }
            _ => panic!("expected suback packet"),
        }
    }
}
