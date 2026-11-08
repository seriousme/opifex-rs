use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid packet type: {0}")]
    InvalidPacketType(u8),
    #[error("invalid protocol level: {0}")]
    InvalidProtocolLevel(u8),
    #[error("remaining length exceeds the MQTT maximum of 268435455 bytes")]
    RemainingLengthOverflow,
    #[error("remaining length is malformed or truncated")]
    MalformedRemainingLength,
    #[error("fixed header is incomplete")]
    InvalidFixedHeader,
    #[error("packet body is truncated")]
    TruncatedPacket,
    #[error("packet payload is malformed: {0}")]
    MalformedPayload(String),
    #[error("UTF-8 payload is invalid: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("unsupported MQTT packet type while decoding: {0}")]
    UnsupportedPacketType(u8),
}
