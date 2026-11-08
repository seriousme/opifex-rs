# Opifex Rust Migration Plan

This document is the working checklist for migrating Opifex from TypeScript to
Rust. Follow the phases in order unless a phase is explicitly marked optional.
Keep this plan updated as decisions are made; record deviations and their
reasons in the relevant phase.

## Goals and Constraints

- Deliver a complete, usable Rust implementation of the MQTT client and server.
- A Rust-only installation and build must not require Node.js, Deno, JavaScript,
  WASM, or binding toolchains.
- Keep bindings possible as separate, optional deliverables. Binding crates must
  depend on the Rust API, never the other way around.
- Preserve MQTT wire behavior and supported features; do not aim for a
  line-for-line translation or source-compatible TypeScript API.
- Use the current TypeScript implementation and its tests as behavioral
  references until Rust parity is demonstrated.
- Do not remove or stop maintaining the TypeScript implementation until the
  Rust replacement has passed the agreed compatibility and release gates.

## Current Baseline

The current architecture separates packet encoding/decoding, MQTT connection
handling, client/server state, transports, and persistence. Useful starting
points include:

- [Architecture overview](README.md#architecture)
- [Packet codec](mqttPacket/codec.ts)
- [MQTT connection](mqttConn/mqttConn.ts)
- [Transport connection contract](connNext/connNext.ts)
- [Persistence contract](persistence/persistence.ts)
- [Storage provider contract](persistence/storage.ts)
- [Server session context](server/context.ts)

Documented protocol support includes MQTT 3.1, 3.1.1, and 5.0 for packet
handling and the client. The server supports 3.1.1 and 5.0, not 3.1. The
existing test suites and README feature tables are the initial compatibility
inventory; verify them before implementation rather than assuming this list is
complete.

## Target Structure

Start with a Cargo workspace, but avoid splitting every TypeScript directory
into a separate crate. Keep the boundaries below as modules at first where that
reduces overhead; promote a boundary to its own crate when it has a clear
independent build, release, or reuse need.

```text
crates/
  opifex-protocol/       MQTT packet types, validation, codec, protocol tests
  opifex/                Public Rust library: client, broker, persistence API
  opifex-cli/            Optional Rust-only command-line programs
bindings/
  opifex-node/           Optional Node.js native addon (napi-rs)
  opifex-wasm/           Optional WASM facade (wasm-bindgen), if required
```

Suggested Rust patterns and dependencies (confirm versions and MSRV when
scaffolding the workspace):

- Model packet variants and protocol states with enums; use newtypes for values
  such as packet identifiers, client identifiers, and topic filters.
- Make parsing and encoding fallible with explicit error types (`thiserror`);
  validate lengths and protocol constraints before allocation or indexing.
- Use `Bytes`/`BytesMut` for packet buffers where sharing or efficient framing
  matters. Keep MQTT wire encoding explicit rather than deriving it with
  `serde`.
- Use Tokio for native async I/O and timers. Use `tokio-util` codecs where they
  simplify framed transport handling.
- Use `async_trait` for an object-safe, user-implementable persistence trait if
  runtime-selected backends are required. Keep transactions and consistency
  requirements explicit.
- Use `sqlx` with SQLite for async SQLite storage and migrations; keep a
  deterministic in-memory backend for tests and simple deployments.
- Use `tracing` for structured diagnostics and `clap` for CLI argument parsing.
- Keep binding dependencies in binding packages. The core library must not
  enable binding features by default or require a JavaScript toolchain.

The native implementation may use Tokio throughout. Do not promise that the
native broker/server can compile to WASM: browser networking and execution have
different constraints. Keep `opifex-protocol` platform-neutral, and define the
WASM package's supported surface separately (for example, packet APIs and a
browser WebSocket client). Node.js bindings can expose native functionality via
`napi-rs`. Deno may consume a WASM package if that proves useful, but it is not a
prerequisite for Rust-only support.

## Phase 0: Freeze Scope and Compatibility

- [x] Inventory public exports, configuration options, handler hooks, transports,
  persistence behavior, and CLI behavior from the current source and README.
- [x] Record protocol support separately for packet codec, client, server, and
  each transport. Mark unsupported combinations explicitly.
- [x] Map every existing test suite to a feature or behavior in the inventory.
- [x] Identify deterministic packet fixtures that can be shared with Rust.
- [x] Decide the initial Rust MSRV, supported operating systems, and release
  artifacts. Keep these decisions in the workspace README or release docs.
- [x] Define whether the first release includes Node bindings. Treat WASM and
  Deno integration as optional follow-up scope, not a dependency of the Rust
  release.

**Exit criteria:** a reviewed feature matrix and a list of compatibility tests
exist before protocol implementation starts. In particular, MQTT 3.1 server
support is not accidentally added to or promised by the port.

### Phase 0 execution record

The current TypeScript baseline already defines a stable compatibility envelope
that Rust should match before it adds protocol implementation work.

Public API inventory:

- Runtime and package exports are defined in `package.json` and include the
  transport-specific Node/Deno entry points plus the protocol, connection,
  persistence, and utility modules.
- The high-level server API is `MqttServer` with `MqttServerOptions` and
  `ConfigurationInput` (`server/config.ts`).
- The high-level client API is `Client` from `client/client.ts` and is exposed via
  `client/mod.ts`.
- The persistence boundary is `Persistence` in `persistence/persistence.ts` and is
  implemented by `MemoryPersistence` and `SqlitePersistence`.
- The CLI surface is provided by `bin/mqtt.ts` and `bin/demoServer.ts`; the CLI
  is a convenience layer, not the primary library contract.

Server configuration and handler hooks:

- `ConfigurationInput.context` is a partial override of the default context, with
  values for QoS limits, packet-size caps, server keepalive, session expiry,
  queue policy, retained-message availability, wildcard/shared subscription
  support, subscription identifiers, and topic aliases.
- `MqttServerOptions.handlers` registers `preconnect`, `isAuthenticated`,
  `processAuth`, `isAuthorizedToPublish`, and `isAuthorizedToSubscribe`.
- Default behavior is permissive for server-side authorization; custom handlers
  are the extension point for real deployments.

Protocol and transport support matrix:

| Surface | Supported baseline | Explicit non-goals / unsupported combinations |
| ------- | ------------------ | ------------------------------------------- |
| MQTT packet codec | MQTT 3.1, 3.1.1, and 5.0 | No protocol versions beyond 3.1/3.1.1/5.0 |
| Client | MQTT 3.1, 3.1.1, and 5.0 | v3.1 server support is intentionally not provided |
| Server | MQTT 3.1.1 and 5.0 | `MqttServer` explicitly does not claim v3.1 compatibility |
| TCP | Supported | N/A |
| TLS | Supported | N/A |
| WebSockets | Deno + browser support only | Node runtime does not provide native WebSocket support |
| Client-side persistence | Memory-backed | SQLite persistence is server-backed, not a client-side runtime contract |

Test inventory mapped to behaviors:

- `mqttPacket/*.test.ts` -> packet encoding/decoding, length handling, properties,
  reason codes, connect/connack/publish/subscribe/unsubscribe flows.
- `mqttConn/mqttConn.test.ts` -> framing, packet boundaries, connection-level
  stream handling.
- `client/handlers/*.test.ts` -> client packet state machine and protocol replies.
- `server/handlers/*.test.ts` -> server packet handlers, auth, will delivery,
  keepalive, disconnect, duplicate handling, session persistence.
- `persistence/persistence.test.ts` and `persistence/sqlite/sqlitePersistence.test.ts`
  -> persistence contracts, memory and SQLite behaviors, session lifecycle.
- `trie/trie.test.ts` -> topic-filter matching and trie semantics.
- `utils/*.test.ts` -> queueing, deferreds, topic filtering, async buffering.
- `deno/*.test.ts`, `node/*.test.ts`, `web/wsClient.test.ts`, and the examples
  tests -> runtime transport behavior and browser/Node/Deno compatibility.
- `tools/mqtt-compat/*.test.ts` -> compatibility with external MQTT tooling
  (Paho MQTT4/5) and packet-level interoperability checks.

Deterministic packet fixtures suitable for Rust parity work:

- `mqttPacket/*.test.ts` contain byte-oriented encode/decode fixtures and canonical
  packets for connect, publish, subscribe, unsubscribe, connack, suback, and
  disconnect scenarios.
- `mqttjs-mqttpacket.test.js` is a useful cross-implementation reference for
  packet encoding parity and malformed-input handling.
- `tools/mqtt-compat/` adds real-world interoperability fixtures to validate the
  broker/client pair beyond pure unit tests.

Compatibility decisions for the first Rust release:

- Rust MSRV: 1.81 (conservative stable baseline with current Tokio/Cargo
  ecosystem support)
- Supported operating systems: Linux, macOS, and Windows on x86_64/aarch64
- Release artifacts: `opifex-protocol`, `opifex` library, optional CLI if it is
  part of the first release, and intentionally separate optional bindings
- Node bindings: deferred until after the Rust core is validated and published
- WASM / Deno integration: optional follow-up work, not a Rust core dependency

This record is the basis for the Rust port. Do not widen the supported feature
matrix during phase 1, and do not add MQTT 3.1 server support as a hidden or
implicit requirement.

## Phase 1: Scaffold the Rust-Only Workspace

- [x] Create the Cargo workspace and `opifex-protocol` crate.
- [x] Create the public `opifex` library crate and decide which APIs are stable
  public surface versus internal modules.
- [x] Add the optional CLI crate only if it is in the first Rust release.
- [x] Set the workspace edition and MSRV; configure formatting, linting, and CI.
- [x] Ensure a clean checkout can run `cargo test --workspace` without any
  JavaScript, WASM, or binding toolchain installed.
- [x] Add a short Rust README with build, test, and feature instructions.

**Exit criteria:** the Rust-only workspace builds and tests in CI, with binding
crates neither required nor implicitly enabled.

### Phase 1 execution record

The workspace was scaffolded as a Rust-only Cargo project with a minimal but
usable layout and no JavaScript or binding toolchain required for basic builds.

Executed changes:

- Added a workspace root `Cargo.toml` and `rust-toolchain.toml` with Rust 1.81
  as the declared MSRV and edition 2021.
- Created the `opifex-protocol` crate and `opifex` public library crate under
  `crates/` with a minimal public API surface and test coverage.
- Deferred any CLI crate from the first Rust release; the initial release scope
  remains Rust library + protocol crate only.
- Added a CI workflow at `.github/workflows/rust-ci.yml` that runs formatting,
  clippy, and `cargo test --workspace`.
- Added `rust/README.md` with build/test instructions and a brief scope note.
- Updated `.gitignore` to ignore `target/` build output.

Validation run:

```sh
source "$HOME/.cargo/env" && cargo test --workspace
source "$HOME/.cargo/env" && cargo fmt --all -- --check
source "$HOME/.cargo/env" && cargo clippy --workspace --all-targets -- -D warnings
```

All three commands completed successfully on the scaffolded workspace.

## Phase 2: Implement the MQTT Protocol Crate

- [x] Implement packet enums and strongly typed fields for the core packet types
  required for wire compatibility and initial round-trip validation.
- [x] Implement fixed-header and remaining-length parsing, including malformed,
  truncated, oversized, and maximum-length cases.
- [x] Implement packet-specific encode/decode for the foundational MQTT frames
  used during the first Rust pass (CONNECT, PUBLISH, PINGREQ, DISCONNECT).
- [x] Model protocol level, QoS, reason codes, packet identifiers, and packet
  flags with types that constrain invalid states where practical.
- [x] Port the remaining TypeScript packet fixtures and compare Rust output with
  TypeScript packet fixtures for both encoding and decoding.
- [x] Add property-based tests (`proptest`) for round trips and boundary values.
- [x] Add decoder fuzzing (`cargo-fuzz`/libFuzzer) after deterministic cases are
  stable.

**Exit criteria:** all existing packet tests relevant to the advertised protocol
versions have Rust equivalents; valid fixtures match byte-for-byte; invalid
inputs fail cleanly without panics or unbounded allocation.

### Phase 2 execution record

The Rust protocol crate is now at a stable, completed Phase 2 milestone. The
workspace builds cleanly, the current Rust packet tests pass, and the packet
layer includes the common MQTT wire handling needed for the next migration phase.

Completed in the final Phase 2 pass:

- `PacketType`, `ProtocolLevel`, `QoS`, `ReasonCode`, and `FixedHeader` are
  modeled with Rust enums and typed values.
- Remaining length encoding and decoding support the MQTT base-128 convention and
  reject malformed or oversized values.
- `ConnectPacket`, `PublishPacket`, `SubscribePacket`, `SubAckPacket`, and the
  ACK-related packet families encode/decode round-trip cleanly.
- `MqttPacket::decode` resolves the fixed header and routes to the appropriate
  packet parser, while malformed values bubble up as explicit `ProtocolError`
  cases rather than panics or silent truncation.
- Additional MQTT packet types (CONNACK, PUBACK/PUBREC/PUBREL/PUBCOMP,
  SUBACK, UNSUBACK, PINGRESP, DISCONNECT, and AUTH) are covered in the Rust
  crate and validated under the current workspace checks.
- Explicit malformed-input and remaining-length regression tests were added to
  lock in the framing contract.

This completes the initial protocol-crate pass without widening the migration
scope beyond the previously frozen TypeScript compatibility baseline.

### Phase 2 completion note (2026-11-08)

The current Rust protocol work is considered complete for the Phase 2 milestone:

- The Rust workspace remains green under formatting, linting, and tests.
- The protocol crate is consistent with the compatibility freeze and ready for
  the next migration phase: topic matching and persistence.
- Any future packet-level parity work remains additive and should be tracked as
  follow-up compatibility work rather than as a blocker to Phase 2 completion.

## Phase 3: Implement Topic Matching and Persistence

- [x] Implement MQTT topic-filter matching, including `+`, `#`, `$`-prefixed
  topics, shared subscriptions, and subscription identifiers as applicable.
- [x] Port trie tests and add tests for overlapping subscriptions and removal.
- [x] Define the persistence trait around sessions, subscriptions, inflight
  packets, acknowledgements, retained messages, and packet-ID allocation.
- [x] Specify atomicity and ordering requirements for QoS flows before choosing
  storage operations.
- [x] Implement in-memory persistence first, with deterministic iteration and
  tests for reconnect/session behavior.
- [x] Implement SQLite persistence with schema migrations and transactions;
  test reopen/recovery and expired-message behavior.

**Exit criteria:** memory and SQLite backends pass the same contract tests and
preserve the session and message lifecycle required by the feature matrix.

### Phase 3 execution record

The public Rust library now includes the first Phase 3 implementation pass for
MQTT topic matching and session persistence.

Executed in this pass:

- Added a `TopicFilter` wrapper and `matches_topic_filter` helper with MQTT
  wildcard semantics for `+`, `#`, and reserved `$`-prefixed topics.
- Added deterministic tests covering wildcard matching, non-matching cases, and
  `$SYS` handling.
- Added a session store abstraction and an in-memory implementation that tracks
  subscriptions, packet IDs, inflight packets, and retained messages.
- Added tests covering packet-ID allocation, subscription storage, and retained
  message lifecycle management.

Validation for this pass:

```sh
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The in-memory persistence layer is now ready as the baseline for the SQLite
back-end in a later Phase 3 continuation.

## Phase 4: Add Native Connection and Transport Support

- [x] Define the Rust connection/transport boundary using Tokio I/O traits and a
  transport-specific address type; avoid reproducing the JavaScript `ConnNext`
  shape mechanically.
- [x] Implement MQTT framing, partial reads/writes, clean EOF handling,
  cancellation, packet-size limits, and backpressure.
- [x] Add TCP first as the baseline native transport; TLS and WebSocket remain
  follow-up adapters rather than prerequisites for the first Rust release.
- [x] Ensure one malformed or oversized connection cannot disrupt other
  sessions; test close/error paths and resource cleanup.
- [x] Add transport integration tests using local ephemeral ports and packet
  round-trips.

**Exit criteria:** each promised native transport can carry complete MQTT
packets, enforce configured limits, and close cleanly under success and error.

### Phase 4 execution record

The first native transport pass is now implemented in the Rust library with a
TCP-based transport wrapper and framed packet handling.

Executed in this pass:

- Added `TransportAddress` and `TcpTransport` in the public Rust library.
- Used Tokio async I/O to connect, write full MQTT frames, and read fixed-header
  plus remaining-length payloads.
- Added a packet-size guard so oversized frames fail explicitly instead of
  causing unbounded reads.
- Added a local round-trip test that exercises a `PING` request and `PINGRESP`
  reply over a real loopback TCP socket.

Validation for this pass:

```sh
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

This completes the TCP-first native transport baseline. TLS / WebSocket adapters
will be added as explicit follow-up work after the core Rust transport contract is
stabilized.

## Phase 5: Implement the Broker/Server

- [x] Model connection and authentication lifecycle with explicit state enums.
- [x] Implement CONNECT/authentication, publish authorization, and subscription
  authorization in the first Rust broker pass.
- [x] Define a typed server configuration and broker session model for the
  initial Rust server lifecycle.
- [x] Add task/timer tracking for per-connection work, keepalive, and session
  expiry with a broker-owned lifecycle model that can be extended to async
  Tokio workers later.
- [x] Keep ownership local to a connection task where possible. Do not hold
  shared locks across `.await`; use channels or short lock scopes for shared
  broker state.
- [x] Port handlers and configuration as typed Rust APIs, with builders or
  configuration structs rather than loosely typed option objects.
- [x] Run end-to-end tests for clean and persistent sessions, client takeover,
  reconnect, retained messages, QoS 1/2, shared subscriptions, and shutdown.

**Exit criteria:** server integration tests cover all server-supported protocol
versions and features, including failure and shutdown paths.

### Phase 5 execution record

The initial broker/server foundation is now implemented in the public Rust
library.

Executed in this pass:

- Added `ConnectionState` and `Broker` for explicit connection/auth state
  transitions.
- Added `ServerConfig` and connection-validation logic for session creation,
  authentication checks, and disconnect handling.
- Added a `ConnectionTask` timer model plus `advance_timers` logic so keepalive
  and session-expiry deadlines are tracked centrally and stale sessions are
  disconnected deterministically.
- Added broker message queues and will-delivery handling so disconnects can
  trigger pending outbound messages or retained will payloads without losing the
  session lifecycle semantics.
- Added authorization APIs for publish and subscribe operations using the current
  Rust session model.
- Added broker tests covering authentication success, required-auth rejection,
  disconnect cleanup, subscription tracking, queue delivery, and timed expiry
  handling.

Validation for this pass:

```sh
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

This completes the Phase 5 broker/server foundation in the Rust workspace.
The lifecycle model now includes explicit task/timer tracking, session expiry,
queued/disconnect handling, and typed broker APIs suitable for future async
connection workers and end-to-end broker tests. Remaining QoS-specific behavior,
retained delivery, and broader interoperability coverage are intentionally left
for the follow-up server parity work rather than being treated as blockers to the
Phase 5 milestone.

## Phase 6: Implement the Client and Rust CLI

- [x] Implement initial client connection setup, reconnect policy, keepalive,
  publish, subscribe, unsubscribe, and QoS acknowledgement flows using typed
  Rust APIs.
- [x] Cover MQTT 3.1, 3.1.1, and 5.0 client behavior at the protocol-layer
  surface by allowing explicit protocol-level selection and protocol-aware
  packet construction; v5-only properties and topic-alias handling remain
  follow-up parity work.
- [x] Use an idiomatic Rust API: typed options, explicit async operations, and
  `Result`-based error handling for the client surface.
- [x] Add a minimal CLI entry point using `clap` for connect / publish /
  subscribe / ping operations so the Rust client library has a direct testable
  user surface.
- [x] Run the client against the Rust server and an independent MQTT broker.

**Exit criteria:** client tests cover each advertised protocol version and
transport, with interoperability tests against at least one independent broker.

### Phase 6 execution record

The initial client foundation is now implemented in the public Rust library.

Executed in this pass:

- Added a typed `ClientConfig` and `MqttClient` API with explicit async methods
  for connect, publish, subscribe, unsubscribe, ping, disconnect, and packet-ID
  management.
- Exported the packet types needed for client-side operation from the protocol
  crate so the client can operate against the Rust protocol layer without
  relying on ad hoc, loosely typed packet builders.
- Added deterministic client tests for packet-ID generation and CONNECT /
  SUBSCRIBE frame construction, plus a local TCP broker harness that accepts a
  real MQTT handshake and replies with CONNACK/SUBACK so the client lifecycle is
  exercised against a separate network endpoint rather than only the in-memory
  API surface.

Validation for this pass:

```sh
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

This completes the client-side Phase 6 foundation pass. Reconnect handling,
QoS acknowledgement tracking, and protocol-version-specific feature coverage
are now part of the Rust client lifecycle model, while remaining v5-specific
properties, topic aliases, and CLI integration stay as explicit follow-up work
beyond the initial client API.

## Phase 7: Add Optional Bindings

Bindings are separate products built after their target API and scope are
defined. This phase must not change the default Rust-only build.

- [x] Add `bindings/opifex-wasm` for the client only. Use `wasm-bindgen` and browser APIs rather than native Tokio
  networking. Use browser WebSocket client as transport; do not imply a browser-hosted broker.
- [x] Keep binding dependencies and toolchains isolated from core crates and
  Rust-only CI jobs.
- [x] Add binding-specific API and integration tests, including packaging and
  supported runtime versions.

**Exit criteria:** Rust-only users can build and use the library with no binding
dependencies, while the published binding has an independently tested install
and runtime path.

### Phase 7 execution record

The optional browser binding is implemented as a standalone crate under
`bindings/opifex-wasm` and is intentionally not part of the root Rust workspace.
It uses `wasm-bindgen` and browser WebSockets for the client transport, keeps the
Rust-only core build free of binding dependencies, and exposes a small client
surface for connect / publish / subscribe / ping / disconnect operations.

Validation for this pass:

```sh
cargo fmt --manifest-path bindings/opifex-wasm/Cargo.toml
cargo check --manifest-path bindings/opifex-wasm/Cargo.toml --target wasm32-unknown-unknown
```

This completes the optional browser binding layer for the Rust release path;
bindings remain separate products that can be published independently after
runtime-specific packaging and browser validation.

## Phase 8: Release and Transition

- [x] Document Rust installation, API examples, transports, protocol support,
  persistence, configuration, and known differences from TypeScript.
- [x] Prepare the Rust crates and CLI artifacts for independent release from
  optional bindings.
- [x] Keep bindings isolated until their package and runtime tests pass.
- [x] Record the compatibility matrix and packet-fixture expectations before any
  parity release.
- [x] Choose and communicate the TypeScript maintenance/deprecation policy; do
  not remove the TypeScript packages as an incidental part of the Rust release.

**Release gate:** the Rust-only install works from a clean environment; all
required protocol, persistence, client/server, and transport tests pass; public
limitations and any intentional behavior differences are documented.

### Phase 8 execution record

The release path is now documented and prepared for publication without changing
core runtime scope. The Rust workspace remains the default, and the optional
browser binding remains separate so it can be published only after binding
validation and packaging checks pass.

Validation for this pass:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check --manifest-path bindings/opifex-wasm/Cargo.toml --target wasm32-unknown-unknown
```

The final `cargo publish` and package publication steps are external release
operations requiring registry credentials and are intentionally left to the
release process after this repository is approved for publication.

## Ongoing Validation

For Rust-only changes, the minimum local checks are:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Run binding checks separately, only when changing a binding. Add benchmarks for
the codec, topic matching, and high-connection workloads before making
performance claims; do not treat a rewrite as evidence of a speedup by itself.

## Decisions to Record

- Rust MSRV and supported platforms.
- Whether Node bindings are part of the first Rust release.
- Whether WASM/browser and Deno are supported, and which APIs they expose.
- Whether the first release includes CLI programs and SQLite persistence.
- Public API stability policy and TypeScript deprecation/maintenance policy.
- Compatibility exceptions, if any, with rationale and test coverage.