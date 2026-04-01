# @solncebro/websocket-engine

Reliable WebSocket client for Node.js with automatic reconnection, ping/pong heartbeat, typed messages, optional authentication phase, and request/response pattern.

## Installation

```bash
yarn add @solncebro/websocket-engine
```

```bash
npm install @solncebro/websocket-engine
```

## Features

- **Automatic reconnection** — exponential backoff with jitter; fast reconnect for specific close codes (1001, 1006, 1011–1014)
- **Heartbeat** — TCP ping/pong by default; or application-level JSON ping (e.g. Bybit `{ op: 'ping' }`)
- **Connection timeout** — handshake timeout with retry
- **Auth phase** — optional `onOpen` async callback for authentication before the connection is considered ready
- **Send** — `ws.sendToConnectedSocket(data)` for outbound messages
- **Request/response** — `ws.waitForMessage(predicate, timeout)` to await a specific incoming message by any criteria (e.g. `reqId`)
- **Typed messages** — optional `parseMessage` for type-safe payloads
- **Notifications** — `onNotify` callback for alerts on connection issues and max retries exceeded

## Requirements

- Node.js 16+
- TypeScript 5.x (optional, for types)

## Usage

### Basic (no auth)

```typescript
import { ReliableWebSocket } from "@solncebro/websocket-engine";

interface StreamMessage {
  type: string;
  data: unknown;
}

const ws = new ReliableWebSocket<StreamMessage>({
  url: "wss://stream.example.com",
  label: "market-stream",
  logger: pinoLogger,
  parseMessage: (rawData) => JSON.parse(rawData.toString()) as StreamMessage,
  onMessage: (message) => {
    console.log(message.type, message.data);
  },
  onNotify: async (message) => {
    await sendTelegramAlert(message);
  },
});

ws.close();
```

### With authentication (e.g. Bybit trading WebSocket)

```typescript
import crypto from "crypto";
import {
  ReliableWebSocket,
  WebSocketOpenContext,
} from "@solncebro/websocket-engine";

interface BybitMessage {
  op?: string;
  retCode?: number;
  retMsg?: string;
  reqId?: string;
  data?: unknown;
}

const ws = new ReliableWebSocket<BybitMessage>({
  url: "wss://stream.bybit.com/v5/trade",
  label: "bybit-trade",
  logger: pinoLogger,
  parseMessage: (rawData) => JSON.parse(rawData.toString()) as BybitMessage,

  onOpen: async ({ send, waitForMessage }: WebSocketOpenContext<BybitMessage>) => {
    const expires = Date.now() + 10000;
    const signature = crypto
      .createHmac("sha256", SECRET)
      .update(`GET/realtime${expires}`)
      .digest("hex");

    send({ op: "auth", args: [API_KEY, expires, signature] });

    const response = await waitForMessage((message) => message.op === "auth", 10000);

    if (response.retMsg !== "OK") {
      throw new Error(`Auth failed: ${response.retMsg}`);
    }
  },

  heartbeat: {
    buildPayload: () => ({ op: "ping" }),
    isResponse: (msg) => msg.op === "pong",
  },

  onMessage: (message) => {
    if (message.op === "order.create") {
      // handle order response
    }
  },

  onReconnectSuccess: () => {
    console.log("Reconnected and re-authenticated");
  },

  onNotify: async (message) => {
    await sendTelegramAlert(message);
  },
});

// Send an order and await its specific response by reqId
const sendOrder = async (orderParams: Record<string, unknown>) => {
  const reqId = `req_${Date.now()}`;

  ws.sendToConnectedSocket({
    reqId,
    op: "order.create",
    args: [orderParams],
  });

  return ws.waitForMessage((message) => message.reqId === reqId, 30000);
};
```

## API

### `new ReliableWebSocket<TMessage>(args)`

Creates a `ReliableWebSocket<TMessage>` instance. Connection starts immediately on construction.

#### Arguments

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `url` | `string` | Yes | WebSocket URL |
| `label` | `string` | Yes | Identifier for logs and notifications |
| `logger` | `WebSocketLogger` | Yes | Logger with `debug`, `info`, `warn`, `error`, `fatal` |
| `onMessage` | `(message: TMessage) => void` | Yes | Called for each incoming message (not intercepted by `waitForMessage` or heartbeat) |
| `parseMessage` | `(rawData: RawData) => TMessage` | No | Parse raw data to `TMessage`; default: pass-through |
| `onOpen` | `(context: WebSocketOpenContext<TMessage>) => Promise<void>` | No | Async setup phase after connect (e.g. auth). Connection is not considered ready until this resolves. |
| `onReconnectSuccess` | `() => void` | No | Called after a successful reconnection (not on first connect) |
| `onNotify` | `(message: string) => void \| Promise<void>` | No | Called on connection issues and when max retries exceeded |
| `heartbeat` | `WebSocketHeartbeatOptions<TMessage>` | No | Application-level heartbeat (JSON ping/pong). When provided, TCP ping is disabled. |
| `configuration` | `Partial<WebSocketConfiguration>` | No | Override default timeouts and retry behaviour |

#### Instance Methods

| Method | Description |
|--------|-------------|
| `close()` | Stops reconnection, clears timers, rejects pending waiters, closes the socket |
| `getStatus()` | Returns current `WebSocketStatus` |
| `getUrl()` | Returns the WebSocket URL |
| `sendToConnectedSocket(data)` | Send data; `string` is sent as-is, anything else is `JSON.stringify`-ed. Throws if not connected. |
| `waitForMessage(predicate, timeoutMilliseconds)` | Returns a `Promise<TMessage>` that resolves with the first incoming message matching `predicate`. The message is **not** passed to `onMessage`. Rejects on timeout, connection close, or if `predicate` throws. |

#### WebSocketStatus

- `CONNECTING` — initial connection attempt
- `CONNECTED` — connected (and auth passed if `onOpen` was provided)
- `DISCONNECTED` — disrupted, reconnect scheduled
- `RECONNECTING` — reconnect in progress (includes `onOpen` phase)
- `FAILED` — closed by user or max retries exceeded

#### WebSocketOpenContext

Passed to `onOpen`:

| Property | Description |
|----------|-------------|
| `send` | Send to the open socket (for use during onOpen) |
| `waitForMessage` | Same as instance `waitForMessage` |

#### WebSocketHeartbeatOptions

| Property | Description |
|----------|-------------|
| `buildPayload` | Returns the JSON object to send as a ping |
| `isResponse` | Returns `true` if the message is a pong. Matching messages are **not** passed to `onMessage`. |

### Configuration (`configuration`)

| Option | Default | Description |
|--------|---------|-------------|
| `maxRetryAttempts` | `15` | Max reconnection attempts before entering FAILED status |
| `initialRetryDelay` | `1000` | Initial delay (ms) for exponential backoff |
| `maxRetryDelay` | `30000` | Cap (ms) for backoff delay |
| `retryDelayMultiplier` | `1.8` | Backoff multiplier |
| `connectionTimeout` | `30000` | Handshake timeout (ms) |
| `pingInterval` | `15000` | Ping interval (ms) |
| `pongTimeout` | `10000` | Pong wait timeout (ms) |
| `heartbeatGracePeriod` | `3000` | Delay before first ping (ms) |
| `fastReconnectCodes` | `[1001, 1006, 1011, 1012, 1013, 1014]` | Close codes that use short reconnect delay |
| `missedPongThreshold` | `3` | Missed pongs before terminating connection |

## Behaviour

- On **close** or **error**, reconnection is scheduled with exponential backoff.
- If `onOpen` **throws**, the connection is terminated and reconnect is triggered (with the same backoff logic). Retry counters are only reset after `onOpen` resolves successfully.
- After **maxRetryAttempts** failed attempts, `onNotify` is awaited with a critical message and status becomes `FAILED`. The process is **not** terminated — the caller can check `getStatus()` and decide how to handle the failure.
- `waitForMessage` pending promises are rejected when the connection is disrupted or `close()` is called.
- Heartbeat response messages and `waitForMessage`-intercepted messages are **never** passed to `onMessage`.

## License

ISC
