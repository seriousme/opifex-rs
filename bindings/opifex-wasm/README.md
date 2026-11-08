# opifex-wasm

Optional browser binding for the Rust MQTT client. This package is intentionally
kept separate from the Rust core workspace so the native `opifex` crates remain
usable without an additional WebAssembly toolchain.

## Scope

- Client-only API.
- Browser WebSocket transport.
- No Tokio runtime or native TCP networking in the binding layer.
- No broker implementation; the target is browser-side client connectivity only.

## Example

```js
import init, { BrowserClient } from "./pkg/opifex_wasm.js";

await init();
const client = new BrowserClient("ws://127.0.0.1:1883/mqtt", "browser-client");
client.connect();
client.subscribe("devices/#", 1);
client.publish("devices/room1", new TextEncoder().encode("hello"), 1, false);
```

## Build

```bash
wasm-pack build --target web --out-dir pkg
```
