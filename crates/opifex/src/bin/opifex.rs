use std::error::Error;

use clap::{Parser, Subcommand};
use opifex::{ClientConfig, MqttClient, TransportAddress};
use opifex_protocol::{ProtocolLevel, QoS};

#[derive(Parser, Debug)]
#[command(name = "opifex")]
#[command(about = "Minimal MQTT client CLI for the Rust Opifex library")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Connect {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 1883)]
        port: u16,
        #[arg(long, default_value = "opifex-cli")]
        client_id: String,
    },
    Publish {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 1883)]
        port: u16,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        payload: String,
        #[arg(long, default_value = "at-most-once")]
        qos: String,
        #[arg(long, default_value_t = false)]
        retain: bool,
    },
    Subscribe {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 1883)]
        port: u16,
        #[arg(long)]
        topic: String,
        #[arg(long, default_value = "at-least-once")]
        qos: String,
    },
    Ping {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 1883)]
        port: u16,
    },
}

fn qos_from_arg(value: &str) -> Result<QoS, String> {
    match value.to_ascii_lowercase().as_str() {
        "at-most-once" | "0" => Ok(QoS::AtMostOnce),
        "at-least-once" | "1" => Ok(QoS::AtLeastOnce),
        "exactly-once" | "2" => Ok(QoS::ExactlyOnce),
        _ => Err(format!("unsupported QoS value: {value}")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Connect {
            host,
            port,
            client_id,
        } => {
            let mut client = MqttClient::new(ClientConfig {
                server: TransportAddress::new(host.clone(), port),
                client_id,
                protocol_level: ProtocolLevel::V5,
                ..ClientConfig::default()
            });
            client.connect().await?;
            println!("connected to {}:{}", host, port);
        }
        Command::Publish {
            host,
            port,
            topic,
            payload,
            qos,
            retain,
        } => {
            let mut client = MqttClient::new(ClientConfig {
                server: TransportAddress::new(host.clone(), port),
                ..ClientConfig::default()
            });
            client.connect().await?;
            client
                .publish(&topic, payload.as_bytes(), qos_from_arg(&qos)?, retain)
                .await?;
            println!("published to {topic}");
        }
        Command::Subscribe {
            host,
            port,
            topic,
            qos,
        } => {
            let mut client = MqttClient::new(ClientConfig {
                server: TransportAddress::new(host.clone(), port),
                ..ClientConfig::default()
            });
            client.connect().await?;
            client.subscribe(&topic, qos_from_arg(&qos)?).await?;
            println!("subscribed to {topic}");
        }
        Command::Ping { host, port } => {
            let mut client = MqttClient::new(ClientConfig {
                server: TransportAddress::new(host.clone(), port),
                ..ClientConfig::default()
            });
            client.connect().await?;
            client.ping().await?;
            println!("ping sent to {}:{}", host, port);
        }
    }
    Ok(())
}
