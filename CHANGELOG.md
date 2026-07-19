# Changelog

## 0.4.0

### Breaking

- **Environment-agnostic transport.** Dropped the `ws` dependency; the client now uses the global WHATWG `WebSocket`. It runs unchanged in Node.js 22+, browsers, and React Native.
- **`engines.node` raised to `>=22`** (built-in global `WebSocket`). Node 16 is no longer supported.
- **Heartbeat model changed.** TCP ping/pong control frames are gone (the global `WebSocket` cannot send them from code). An application-level JSON ping is paired with idle (stale) detection: the socket reconnects when no message arrives within `staleThreshold`.

### Added

- `onClose(context)` callback, fired on every disruption before a reconnect is scheduled, with the new `WebSocketCloseContext` (`closeCode`, `errorMessage`, `consecutiveFailures`, `missedPongCount`, `isPongTimeout`).
- `staleThreshold` (default `60000`) and `staleCheckInterval` (default `5000`) configuration options for idle detection.
- Exported `RawData` and `WebSocketCloseContext` types.

### Changed

- More robust handling of back-to-back `error` + `close` events on a dead socket, so a trailing event can no longer strand the client without a scheduled reconnect.
- `missedPongThreshold` now reflects consecutive stale checks exposed via `isPongTimeout` in the close context.

## 0.3.0

- Refactored the public API around the `ReliableWebSocket` class.
