pub mod client;
pub mod persistence;
pub mod server;
pub mod topic;
pub mod transport;

pub use client::{ClientConfig, ClientError, MqttClient};
pub use opifex_protocol::{version, PROTOCOL_NAME};
pub use persistence::{
    InflightPacket, MemoryPersistence, RetainedMessage, SQLitePersistence, SessionState,
    SessionStore,
};
pub use server::{Broker, ClientSession, ConnectError, ConnectionState, ServerConfig};
pub use topic::{matches_topic_filter, TopicFilter};
pub use transport::{TcpTransport, TransportAddress};

pub const LIBRARY_NAME: &str = "opifex";

pub fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::{crate_version, LIBRARY_NAME};

    #[test]
    fn public_library_has_expected_identity() {
        assert_eq!(LIBRARY_NAME, "opifex");
        assert!(!crate_version().is_empty());
    }
}
