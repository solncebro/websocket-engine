import { DEFAULT_WEBSOCKET_CONFIGURATION } from "./config";
import type {
  RawData,
  ReliableWebSocketArgs,
  WebSocketCloseContext,
  WebSocketConfiguration,
  WebSocketHeartbeatOptions,
  WebSocketLogger,
  WebSocketOpenContext,
} from "./types";
import { WebSocketStatus } from "./types";
import {
  calcReconnectDelay,
  clearTimers,
  type ConnectionInfo,
  isWebSocketClosable,
} from "./websocket.utils";

interface PendingWaiter<TMessage> {
  predicate: (message: TMessage) => boolean;
  resolve: (message: TMessage) => void;
  reject: (error: Error) => void;
  timeoutId: ReturnType<typeof setTimeout>;
}

class ReliableWebSocket<TMessage = RawData> {
  private readonly label: string;
  private readonly url: string;
  private readonly logger: WebSocketLogger;
  private readonly configuration: WebSocketConfiguration;
  private readonly onMessage: (message: TMessage) => void;
  private readonly parseMessage?: (rawData: RawData) => TMessage;
  private readonly onOpen?: (context: WebSocketOpenContext<TMessage>) => Promise<void>;
  private readonly onReconnectSuccess?: () => void;
  private readonly onClose?: (context: WebSocketCloseContext) => void;
  private readonly onNotify?: (message: string) => void | Promise<void>;
  private readonly heartbeat?: WebSocketHeartbeatOptions<TMessage>;
  private readonly connectionInfo: ConnectionInfo;
  private readonly pendingWaiterList: PendingWaiter<TMessage>[] = [];

  private currentWebSocket: WebSocket | null = null;

  public constructor(args: ReliableWebSocketArgs<TMessage>) {
    this.label = args.label;
    this.url = args.url;
    this.logger = args.logger;
    this.configuration = {
      ...DEFAULT_WEBSOCKET_CONFIGURATION,
      ...args.configuration,
    };
    this.onMessage = args.onMessage;
    this.parseMessage = args.parseMessage;
    this.onOpen = args.onOpen;
    this.onReconnectSuccess = args.onReconnectSuccess;
    this.onClose = args.onClose;
    this.onNotify = args.onNotify;
    this.heartbeat = args.heartbeat;

    this.connectionInfo = {
      retryCount: 0,
      status: WebSocketStatus.CONNECTING,
      consecutiveFailures: 0,
      missedPongCount: 0,
      hasEverConnected: false,
    };

    this.connect();
  }

  public getStatus(): WebSocketStatus {
    return this.connectionInfo.status;
  }

  public getUrl(): string {
    return this.url;
  }

  public waitForMessage(
    predicate: (message: TMessage) => boolean,
    timeoutMilliseconds: number
  ): Promise<TMessage> {
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const index = this.pendingWaiterList.findIndex(
          (waiter) => waiter.timeoutId === timeoutId
        );

        if (index !== -1) {
          this.pendingWaiterList.splice(index, 1);
        }

        reject(
          new Error(
            `[${this.label}] waitForMessage timeout after ${timeoutMilliseconds}ms`
          )
        );
      }, timeoutMilliseconds);

      this.pendingWaiterList.push({
        predicate,
        resolve,
        reject,
        timeoutId,
      });
    });
  }

  public close(): void {
    this.connectionInfo.status = WebSocketStatus.FAILED;
    clearTimers(this.connectionInfo);
    this.rejectPendingWaiters(`[${this.label}] Connection closed`);

    const websocket = this.currentWebSocket;
    this.currentWebSocket = null;

    if (websocket) {
      this.closeSocket(websocket);
    }
  }

  public sendToConnectedSocket(data: unknown): void {
    if (this.connectionInfo.status !== WebSocketStatus.CONNECTED) {
      throw new Error(
        `[${this.label}] Cannot send: status is ${this.connectionInfo.status}`
      );
    }

    this.sendToOpenedSocket(data);
  }

  private sendToOpenedSocket(data: unknown): void {
    if (
      !this.currentWebSocket ||
      this.currentWebSocket.readyState !== WebSocket.OPEN
    ) {
      throw new Error(`[${this.label}] Cannot send: WebSocket is not open`);
    }

    const payload = typeof data === "string" ? data : JSON.stringify(data);
    this.currentWebSocket.send(payload);
  }

  private closeSocket(websocket: WebSocket): void {
    if (!isWebSocketClosable(websocket)) {
      return;
    }

    try {
      websocket.close();
    } catch (error) {
      this.logger.error(`[${this.label}] Error closing socket: ${String(error)}`);
    }
  }

  private notify(message: string): void {
    const result = this.onNotify?.(message);

    if (result instanceof Promise) {
      result.catch((error) => {
        this.logger.error(`[${this.label}] onNotify failed: ${String(error)}`);
      });
    }
  }

  private formatRetryAttemptMessage(prefix: string, suffix?: string): string {
    const attemptPart = `(attempt ${this.connectionInfo.retryCount}/${this.configuration.maxRetryAttempts}`;
    const fullAttempt =
      suffix !== undefined ? `${attemptPart}, ${suffix})` : `${attemptPart})`;

    return `[${this.label}] ${prefix} ${fullAttempt}`;
  }

  private rejectPendingWaiters(reason: string): void {
    const error = new Error(reason);

    this.pendingWaiterList.forEach((waiter) => {
      clearTimeout(waiter.timeoutId);
      waiter.reject(error);
    });

    this.pendingWaiterList.length = 0;
  }

  private resolveMessage(rawData: RawData): TMessage | undefined {
    if (!this.parseMessage) {
      return rawData as TMessage;
    }

    try {
      return this.parseMessage(rawData);
    } catch (error) {
      this.logger.error(`[${this.label}] Failed to parse message: ${String(error)}`);

      return undefined;
    }
  }

  private scheduleReconnect(delay: number): void {
    this.connectionInfo.reconnectTimeoutId = setTimeout(() => {
      this.performReconnect().catch((error) => {
        this.logger.error(`[${this.label}] Unexpected error in reconnect: ${String(error)}`);
      });
    }, delay);
  }

  /**
   * Detach a failed socket before handling its disruption. A dead socket can
   * emit `error` and then `close` back-to-back: without detaching, the trailing
   * event clears the reconnect timer the first event just scheduled and the
   * status guard in handleDisruption refuses to re-arm it, stranding the client.
   * Returns false if the socket was already detached (later events are ignored).
   */
  private retireSocket(websocket: WebSocket): boolean {
    if (websocket !== this.currentWebSocket) {
      return false;
    }

    this.currentWebSocket = null;
    clearTimers(this.connectionInfo);
    this.closeSocket(websocket);

    return true;
  }

  private handleDisruption(closeCode?: number, errorMessage?: string): void {
    if (
      this.connectionInfo.status === WebSocketStatus.FAILED ||
      this.connectionInfo.status === WebSocketStatus.DISCONNECTED
    ) {
      return;
    }

    this.connectionInfo.status = WebSocketStatus.DISCONNECTED;
    this.connectionInfo.consecutiveFailures++;

    this.rejectPendingWaiters(`[${this.label}] Connection disrupted`);

    const failureSuffix = `, consecutive failures: ${this.connectionInfo.consecutiveFailures}`;
    const codeText = closeCode !== undefined ? `code ${closeCode}` : "unknown";
    const message = errorMessage
      ? `[${this.label}] Connection error: ${errorMessage}${failureSuffix}`
      : `[${this.label}] Connection closed (${codeText})${failureSuffix}`;
    const level = errorMessage ? "error" : "warn";

    this.logger[level](message);
    this.notify(message);

    if (this.onClose) {
      try {
        this.onClose({
          closeCode,
          errorMessage,
          consecutiveFailures: this.connectionInfo.consecutiveFailures,
          missedPongCount: this.connectionInfo.missedPongCount,
          isPongTimeout:
            this.connectionInfo.missedPongCount >=
            this.configuration.missedPongThreshold,
        });
      } catch (error) {
        this.logger.warn(`[${this.label}] onClose handler failed: ${String(error)}`);
      }
    }

    const delay = calcReconnectDelay(
      this.connectionInfo.consecutiveFailures,
      closeCode,
      this.configuration
    );
    this.scheduleReconnect(delay);
  }

  private async performReconnect(): Promise<void> {
    if (this.connectionInfo.status === WebSocketStatus.RECONNECTING) {
      this.logger.debug(`[${this.label}] Already reconnecting, skipping`);

      return;
    }

    this.connectionInfo.status = WebSocketStatus.RECONNECTING;
    this.connectionInfo.retryCount++;

    this.logger.info(
      this.formatRetryAttemptMessage(
        "Reconnecting",
        `consecutive failures: ${this.connectionInfo.consecutiveFailures}`
      )
    );

    if (this.connectionInfo.retryCount > this.configuration.maxRetryAttempts) {
      this.connectionInfo.status = WebSocketStatus.FAILED;
      clearTimers(this.connectionInfo);
      this.rejectPendingWaiters(`[${this.label}] Max retries exceeded`);

      if (this.currentWebSocket) {
        this.closeSocket(this.currentWebSocket);
        this.currentWebSocket = null;
      }

      const criticalMessage = `[${this.label}] CRITICAL: max retries (${this.configuration.maxRetryAttempts}) exceeded after ${this.connectionInfo.consecutiveFailures} consecutive failures`;

      this.logger.fatal(criticalMessage);

      try {
        await this.onNotify?.(criticalMessage);
      } catch (error) {
        this.logger.error(`[${this.label}] onNotify failed: ${String(error)}`);
      }

      return;
    }

    try {
      if (this.currentWebSocket) {
        this.closeSocket(this.currentWebSocket);
      }

      this.connect();
    } catch (error) {
      this.logger.error(`[${this.label}] Error creating connection: ${String(error)}`);
      this.connectionInfo.status = WebSocketStatus.DISCONNECTED;
      const delay = calcReconnectDelay(
        this.connectionInfo.consecutiveFailures,
        undefined,
        this.configuration
      );
      this.scheduleReconnect(delay);
    }
  }

  private setupHeartbeat(websocket: WebSocket): void {
    clearInterval(this.connectionInfo.pingIntervalId);
    clearInterval(this.connectionInfo.staleCheckIntervalId);
    this.connectionInfo.pingIntervalId = undefined;
    this.connectionInfo.staleCheckIntervalId = undefined;
    this.connectionInfo.missedPongCount = 0;
    this.connectionInfo.lastMessageAt = Date.now();

    const heartbeat = this.heartbeat;

    if (!heartbeat) {
      return;
    }

    const sendPing = (): void => {
      if (
        websocket !== this.currentWebSocket ||
        websocket.readyState !== WebSocket.OPEN
      ) {
        return;
      }

      try {
        this.sendToOpenedSocket(heartbeat.buildPayload());
      } catch (error) {
        this.logger.error(`[${this.label}] Error sending ping: ${String(error)}`);
      }
    };

    const checkStale = (): void => {
      if (websocket !== this.currentWebSocket) {
        return;
      }

      const lastMessageAt = this.connectionInfo.lastMessageAt ?? Date.now();
      const idleMs = Date.now() - lastMessageAt;

      if (idleMs <= this.configuration.staleThreshold) {
        this.connectionInfo.missedPongCount = 0;

        return;
      }

      this.connectionInfo.missedPongCount++;
      this.logger.warn(
        `[${this.label}] Stale connection: no messages for ${Math.floor(idleMs / 1000)}s, reconnecting`
      );
      this.retireSocket(websocket);
      this.handleDisruption(undefined, `stale ${Math.floor(idleMs / 1000)}s`);
    };

    setTimeout(sendPing, this.configuration.heartbeatGracePeriod);
    this.connectionInfo.pingIntervalId = setInterval(
      sendPing,
      this.configuration.pingInterval
    );
    this.connectionInfo.staleCheckIntervalId = setInterval(
      checkStale,
      this.configuration.staleCheckInterval
    );
  }

  private connect(): void {
    const websocket = new WebSocket(this.url);

    this.currentWebSocket = websocket;

    this.connectionInfo.connectionTimeoutId = setTimeout(() => {
      if (
        websocket === this.currentWebSocket &&
        websocket.readyState === WebSocket.CONNECTING
      ) {
        const message = this.formatRetryAttemptMessage(
          `Connection timeout after ${this.configuration.connectionTimeout}ms`
        );
        this.logger.warn(message);
        this.notify(message);
        this.retireSocket(websocket);
        this.handleDisruption(undefined, "connection timeout");
      }
    }, this.configuration.connectionTimeout);

    websocket.addEventListener("open", () => {
      if (websocket !== this.currentWebSocket) {
        return;
      }

      this.handleOpen(websocket);
    });

    websocket.addEventListener("message", (event) => {
      if (websocket !== this.currentWebSocket) {
        return;
      }

      this.handleIncoming(event.data);
    });

    websocket.addEventListener("close", (event) => {
      if (!this.retireSocket(websocket)) {
        return;
      }

      this.handleDisruption(event.code);
    });

    websocket.addEventListener("error", () => {
      if (!this.retireSocket(websocket)) {
        return;
      }

      this.handleDisruption(undefined, "websocket error");
    });
  }

  private handleOpen(websocket: WebSocket): void {
    clearTimeout(this.connectionInfo.connectionTimeoutId);
    this.connectionInfo.connectionTimeoutId = undefined;

    const afterOpen = (): void => {
      this.connectionInfo.status = WebSocketStatus.CONNECTED;
      this.connectionInfo.connectionStartedAt = Date.now();
      this.connectionInfo.retryCount = 0;
      this.connectionInfo.consecutiveFailures = 0;
      this.connectionInfo.missedPongCount = 0;
      this.connectionInfo.lastMessageAt = Date.now();

      const isFirstConnection = !this.connectionInfo.hasEverConnected;
      this.connectionInfo.hasEverConnected = true;

      this.logger.info(
        `[${this.label}] ${isFirstConnection ? "Connected" : "Reconnected"} successfully`
      );

      this.setupHeartbeat(websocket);

      if (!isFirstConnection) {
        this.onReconnectSuccess?.();
      }
    };

    if (this.onOpen) {
      const openContext: WebSocketOpenContext<TMessage> = {
        send: this.sendToOpenedSocket.bind(this),
        waitForMessage: this.waitForMessage.bind(this),
      };

      this.onOpen(openContext)
        .then(afterOpen)
        .catch((error) => {
          const message = `[${this.label}] onOpen failed: ${String(error)}`;
          this.logger.error(message);
          this.notify(message);
          this.retireSocket(websocket);
          this.handleDisruption(undefined, "onOpen failed");
        });

      return;
    }

    afterOpen();
  }

  private handleIncoming(data: RawData): void {
    this.connectionInfo.lastMessageAt = Date.now();

    const message = this.resolveMessage(data);

    if (message === undefined) {
      return;
    }

    if (this.heartbeat?.isResponse(message)) {
      return;
    }

    let matchedWaiterIndex = -1;
    let predicateError: Error | undefined;

    for (let i = 0; i < this.pendingWaiterList.length; i++) {
      try {
        if (this.pendingWaiterList[i].predicate(message)) {
          matchedWaiterIndex = i;
          break;
        }
      } catch (error) {
        predicateError =
          error instanceof Error ? error : new Error(String(error));
        matchedWaiterIndex = i;
        break;
      }
    }

    if (matchedWaiterIndex !== -1) {
      const waiter = this.pendingWaiterList[matchedWaiterIndex];
      this.pendingWaiterList.splice(matchedWaiterIndex, 1);
      clearTimeout(waiter.timeoutId);

      if (predicateError) {
        waiter.reject(predicateError);
      } else {
        waiter.resolve(message);
      }

      return;
    }

    this.onMessage(message);
  }
}

export { ReliableWebSocket };
