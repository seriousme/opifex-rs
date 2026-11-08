# Rust workspace

This workspace is the first Rust-only implementation path for Opifex. It keeps
an explicit compatibility baseline with the existing TypeScript implementation,
while isolating optional browser bindings and follow-up product work from the
core runtime.

## Installation and local build

From a fresh checkout:

```bash
rustup toolchain install 1.81
cargo build --workspace
```

The workspace currently contains:

- `opifex-protocol` — MQTT packet model, framing, and protocol validation
- `opifex` — public Rust client/server library, TCP transport, persistence, and
  topic-filter logic
- `opifex-wasm` — optional browser binding kept outside the default workspace

## Quickstart

Use the library directly from the workspace:

```toml
# Cargo.toml
[dependencies]
opifex = { path = "./crates/opifex" }
```

```rust
use opifex::{ClientConfig, MqttClient, TransportAddress};
use opifex_protocol::{ProtocolLevel, QoS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MqttClient::new(ClientConfig {
        server: TransportAddress::new("127.0.0.1", 1883),
        client_id: "rust-client".to_string(),
        protocol_level: ProtocolLevel::V5,
        ..ClientConfig::default()
    });

    client.connect().await?;
    client.subscribe("devices/#", QoS::AtLeastOnce).await?;
    client.publish("devices/room1", b"hello", QoS::AtMostOnce, false).await?;
    client.disconnect().await?;
    Ok(())
}
```

## Supported protocol and transport surface

The initial Rust release intentionally mirrors the compatibility baseline:

- MQTT protocol levels: 3.1, 3.1.1, and 5.0
- Client support includes connect, publish, subscribe, unsubscribe, ping, and
  disconnect flows
- Server lifecycle support includes typed config, connection/auth state,
  keepalive/session expiry, and queued will handling
- Native TCP transport is implemented in the public library
- TLS and WebSocket transports remain separate follow-up work outside the core
  Rust runtime

## Persistence and configuration

The library exposes a trait-based persistence model and concrete backends:

- `SessionStore` trait with in-memory and SQLite-backed implementations
- `MemoryPersistence` and `SQLitePersistence`
- `ServerConfig` and `ClientConfig` for typed API configuration
- explicit `ReconnectPolicy` for client retries and backoff

## Compliance and compatibility notes

The Rust port is intentionally narrow and compatibility-focused:

- TypeScript remains the baseline behavior and compatibility reference
- No Node.js native addon is included in the first Rust release
- WASM/browser and Deno integrations are optional follow-ups, not a default core
  requirement
- The CLI is a convenience tool for the Rust library; not a replacement for the
  TypeScript user-facing package ecosystem
- The project documents known gaps and intentional follow-up work instead of
  broadening the first release scope

## Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Release and transition policy

The Rust implementation is a parallel product line, not a removal of the
TypeScript implementation. The TypeScript packages remain the compatibility and
release baseline until the Rust build is independently validated and published.

Publishing the Rust crates and optional bindings is a release-time action that
requires the appropriate registry credentials and packaging steps. This repo
prepares the code and documentation for that release, but the final `cargo
publish` and binding publishing steps are performed outside the workspace after
credentials are available.
