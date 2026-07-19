/**
 * Reproduces the reconnect-loop deadlock: when a post-attempt socket emits
 * `error` immediately followed by `close`, the next reconnect attempt must still
 * be scheduled. Before the fix the trailing `close` cleared the timer the
 * `error` path had just set and the status guard refused to re-arm it, so the
 * client hung forever. Run with: npx tsx test/reconnectRace.ts
 */

class MockWebSocket {
  public static readonly CONNECTING = 0;
  public static readonly OPEN = 1;
  public static readonly CLOSING = 2;
  public static readonly CLOSED = 3;

  public static instanceList: MockWebSocket[] = [];

  public readyState: number = MockWebSocket.CONNECTING;
  public readonly url: string;
  private readonly listenerMap: Record<string, Array<(event: unknown) => void>> = {};

  public constructor(url: string) {
    this.url = url;
    MockWebSocket.instanceList.push(this);
  }

  public addEventListener(type: string, callback: (event: unknown) => void): void {
    (this.listenerMap[type] ??= []).push(callback);
  }

  public close(): void {
    this.readyState = MockWebSocket.CLOSED;
  }

  public send(): void {
    // no-op for the test
  }

  public dispatch(type: string, event: unknown): void {
    (this.listenerMap[type] ?? []).forEach((callback) => callback(event));
  }
}

(globalThis as unknown as { WebSocket: typeof MockWebSocket }).WebSocket = MockWebSocket;

const silentLogger = {
  debug: (): void => {},
  info: (): void => {},
  warn: (): void => {},
  error: (): void => {},
  fatal: (): void => {},
};

async function main(): Promise<void> {
  const { ReliableWebSocket } = await import("../src/websocket");

  const client = new ReliableWebSocket({
    url: "ws://test",
    label: "race",
    logger: silentLogger,
    onMessage: (): void => {},
    // Large delays so background timers never fire during this synchronous test.
    configuration: { initialRetryDelay: 100000, connectionTimeout: 100000, pingInterval: 100000 },
  });

  const info = (client as unknown as { connectionInfo: { reconnectTimeoutId?: unknown } }).connectionInfo;

  // 1) First socket connects, then drops cleanly with code 1005.
  const first = MockWebSocket.instanceList[0];
  first.readyState = MockWebSocket.OPEN;
  first.dispatch("open", {});
  first.readyState = MockWebSocket.CLOSED;
  first.dispatch("close", { code: 1005 });

  // 2) Drive the scheduled reconnect attempt directly (deterministic, no real timer).
  if (info.reconnectTimeoutId) clearTimeout(info.reconnectTimeoutId as ReturnType<typeof setTimeout>);
  await (client as unknown as { performReconnect: () => Promise<void> }).performReconnect();

  // 3) The freshly opened socket fails: `error` immediately followed by `close`.
  const second = MockWebSocket.instanceList[1];
  if (!second) throw new Error("reconnect did not open a new socket");
  second.dispatch("error", {});
  second.dispatch("close", { code: 1006 });

  // 4) A further reconnect attempt MUST be scheduled, otherwise the loop is dead.
  const reconnectScheduled = info.reconnectTimeoutId !== undefined;

  if (!reconnectScheduled) {
    console.error("FAIL: reconnect loop is dead — no next attempt scheduled after error+close");
    process.exit(1);
  }

  // 5) Regression: the loop still recovers fully — the next attempt connects and
  //    resets the failure counters back to a healthy CONNECTED state.
  if (info.reconnectTimeoutId) clearTimeout(info.reconnectTimeoutId as ReturnType<typeof setTimeout>);
  await (client as unknown as { performReconnect: () => Promise<void> }).performReconnect();
  const third = MockWebSocket.instanceList[2];
  if (!third) throw new Error("second reconnect did not open a new socket");
  third.readyState = MockWebSocket.OPEN;
  third.dispatch("open", {});

  const status = (client as unknown as { getStatus: () => string }).getStatus();
  const fullInfo = info as unknown as { consecutiveFailures: number };
  const recovered = status === "connected" && fullInfo.consecutiveFailures === 0;

  (client as unknown as { close: () => void }).close();

  if (!recovered) {
    console.error(`FAIL: did not recover to a healthy state (status=${status}, failures=${fullInfo.consecutiveFailures})`);
    process.exit(1);
  }

  console.log("PASS: reconnect survives the error+close race and recovers to CONNECTED");
  process.exit(0);
}

void main();
