mod errors;
mod length;
mod packet;

pub use errors::ProtocolError;
pub use length::{decode_remaining_length, encode_remaining_length, MAX_REMAINING_LENGTH};
pub use packet::{
    ConnectFlags, ConnectPacket, FixedHeader, MqttPacket, PacketType, PingreqPacket, ProtocolLevel,
    PublishPacket, QoS, ReasonCode, SubAckPacket, SubscribePacket, UnsubAckPacket, WillMessage,
};

pub const PROTOCOL_NAME: &str = "opifex-protocol";

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::{packet::PacketType, PROTOCOL_NAME};

    #[test]
    fn protocol_crate_has_expected_identity() {
        assert_eq!(PROTOCOL_NAME, "opifex-protocol");
        assert_eq!(PacketType::Connect as u8, 1);
    }
}
