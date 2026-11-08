use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportAddress {
    pub host: String,
    pub port: u16,
}

impl TransportAddress {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub fn as_socket_addr(&self) -> io::Result<SocketAddr> {
        (self.host.as_str(), self.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    format!("could not resolve {}:{}", self.host, self.port),
                )
            })
    }
}

#[derive(Debug)]
pub struct TcpTransport {
    stream: TcpStream,
}

impl TcpTransport {
    pub async fn connect(address: &TransportAddress) -> io::Result<Self> {
        let stream = TcpStream::connect((address.host.as_str(), address.port)).await?;
        Ok(Self { stream })
    }

    pub async fn send_packet(&mut self, packet: &[u8]) -> io::Result<()> {
        self.stream.write_all(packet).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn read_packet(&mut self, max_packet_size: usize) -> io::Result<Vec<u8>> {
        let mut fixed_header = [0u8; 1];
        self.stream.read_exact(&mut fixed_header).await?;

        let mut remaining_length = 0usize;
        let mut multiplier = 1usize;
        let mut length_bytes = Vec::new();

        for _ in 0..4 {
            let mut byte = [0u8; 1];
            self.stream.read_exact(&mut byte).await?;
            let value = byte[0];
            let digit = (value & 0x7f) as usize;
            remaining_length = remaining_length.saturating_add(digit.saturating_mul(multiplier));
            length_bytes.push(value);

            if remaining_length > max_packet_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("packet exceeds max size: {remaining_length} > {max_packet_size}"),
                ));
            }

            if value & 0x80 == 0 {
                break;
            }
            multiplier *= 128;
        }

        let mut payload = vec![0u8; remaining_length];
        if !payload.is_empty() {
            self.stream.read_exact(&mut payload).await?;
        }

        let mut packet = Vec::with_capacity(1 + length_bytes.len() + payload.len());
        packet.push(fixed_header[0]);
        packet.extend_from_slice(&length_bytes);
        packet.extend_from_slice(&payload);
        Ok(packet)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{TcpTransport, TransportAddress};

    #[tokio::test]
    async fn tcp_transport_round_trips_mqtt_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut client_bytes = vec![0u8; 4];
            stream.read_exact(&mut client_bytes).await.unwrap();
            assert_eq!(&client_bytes[..], b"PING");

            let reply = [0xd0, 0x00];
            stream.write_all(&reply).await.unwrap();
            stream.flush().await.unwrap();
        });

        let mut transport = TcpTransport::connect(&TransportAddress::new("127.0.0.1", addr.port()))
            .await
            .unwrap();
        transport.send_packet(b"PING").await.unwrap();
        let frame = transport.read_packet(1024).await.unwrap();
        assert_eq!(frame, vec![0xd0, 0x00]);

        server.await.unwrap();
    }
}
