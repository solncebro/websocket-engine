import { WebSocket } from "ws";
import type { RawData } from "ws";

import { DEFAULT_WEBSOCKET_CONFIGURATION } from "./config";
import type {
  ReliableWebSocketArgs,
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
  timeoutId: NodeJS.Timeout;
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

    if (this.currentWebSocket) {
      this.currentWebSocket.removeAllListeners();

      if (isWebSocketClosable(this.currentWebSocket)) {
        this.currentWebSocket.close();
      }
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
        this.currentWebSocket.removeAllListeners();

        if (isWebSocketClosable(this.currentWebSocket)) {
          this.currentWebSocket.terminate();
        }
      }

      const criticalMessage = `[${this.label}] CRITICAL: max retries (${this.configuration.maxRetryAttempts}) exceeded after ${this.connectionInfo.consecutiveFailures} consecutive failures — terminating process`;

      this.logger.fatal(criticalMessage);

      try {
        await this.onNotify?.(criticalMessage);
      } catch (error) {
        this.logger.error(`[${this.label}] onNotify failed: ${String(error)}`);
      }

      process.exit(1);
    }

    try {
      if (this.currentWebSocket) {
        this.currentWebSocket.removeAllListeners();

        if (isWebSocketClosable(this.currentWebSocket)) {
          this.currentWebSocket.terminate();
        }
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
    clearTimeout(this.connectionInfo.pongTimeoutId);
    this.connectionInfo.pingIntervalId = undefined;
    this.connectionInfo.pongTimeoutId = undefined;
    this.connectionInfo.missedPongCount = 0;
    this.connectionInfo.lastPongReceivedAt = Date.now();

    const heartbeat = this.heartbeat;
    const sendApplicationPing = heartbeat
      ? () => this.sendToOpenedSocket(heartbeat.buildPayload())
      : undefined;

    const sendPing = (): void => {
      if (websocket.readyState !== WebSocket.OPEN) {
        return;
      }

      try {
        sendApplicationPing ? sendApplicationPing() : websocket.ping();

        this.connectionInfo.pongTimeoutId = setTimeout(() => {
          this.connectionInfo.missedPongCount++;

          if (
            this.connectionInfo.missedPongCount >=
            this.configuration.missedPongThreshold
          ) {
            this.logger.warn(
              `[${this.label}] Missed ${this.connectionInfo.missedPongCount} pongs, terminating`
            );
            websocket.terminate();
          } else {
            this.logger.warn(
              `[${this.label}] Pong timeout (${this.connectionInfo.missedPongCount}/${this.configuration.missedPongThreshold})`
            );
          }
        }, this.configuration.pongTimeout);
      } catch (error) {
        this.logger.error(`[${this.label}] Error sending ping: ${String(error)}`);
      }
    };

    if (!sendApplicationPing) {
      websocket.removeAllListeners("pong");
      websocket.on("pong", () => {
        this.connectionInfo.missedPongCount = 0;
        this.connectionInfo.lastPongReceivedAt = Date.now();
        clearTimeout(this.connectionInfo.pongTimeoutId);
        this.connectionInfo.pongTimeoutId = undefined;
      });
    }

    setTimeout(sendPing, this.configuration.heartbeatGracePeriod);
    this.connectionInfo.pingIntervalId = setInterval(
      sendPing,
      this.configuration.pingInterval
    );
  }

  private connect(): void {
    const websocket = new WebSocket(this.url, {
      handshakeTimeout: this.configuration.connectionTimeout,
    });

    this.currentWebSocket = websocket;

    this.connectionInfo.connectionTimeoutId = setTimeout(() => {
      if (websocket.readyState === WebSocket.CONNECTING) {
        const message = this.formatRetryAttemptMessage(
          `Connection timeout after ${this.configuration.connectionTimeout}ms`
        );
        this.logger.warn(message);
        this.notify(message);
        websocket.terminate();
      }
    }, this.configuration.connectionTimeout);

    websocket.on("open", () => {
      clearTimeout(this.connectionInfo.connectionTimeoutId);
      this.connectionInfo.connectionTimeoutId = undefined;

      const afterOpen = (): void => {
        this.connectionInfo.status = WebSocketStatus.CONNECTED;
        this.connectionInfo.connectionStartedAt = Date.now();
        this.connectionInfo.retryCount = 0;
        this.connectionInfo.consecutiveFailures = 0;
        this.connectionInfo.missedPongCount = 0;

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
            websocket.terminate();
          });
      } else {
        afterOpen();
      }
    });

    websocket.on("close", (code: number | undefined) => {
      clearTimers(this.connectionInfo);
      this.handleDisruption(code);
    });

    websocket.on("error", (error: unknown) => {
      clearTimers(this.connectionInfo);
      const message =
        error instanceof Error ? error.message : String(error ?? "unknown");
      this.handleDisruption(undefined, message);
    });

    websocket.on("message", (rawData: RawData) => {
      const message = this.resolveMessage(rawData);

      if (message === undefined) {
        return;
      }

      if (this.heartbeat?.isResponse(message)) {
        this.connectionInfo.missedPongCount = 0;
        this.connectionInfo.lastPongReceivedAt = Date.now();
        clearTimeout(this.connectionInfo.pongTimeoutId);
        this.connectionInfo.pongTimeoutId = undefined;

        return;
      }

      const waiterIndex = this.pendingWaiterList.findIndex((waiter) =>
        waiter.predicate(message)
      );

      if (waiterIndex !== -1) {
        const waiter = this.pendingWaiterList[waiterIndex];
        this.pendingWaiterList.splice(waiterIndex, 1);
        clearTimeout(waiter.timeoutId);
        waiter.resolve(message);
        
        return;
      }

      this.onMessage(message);
    });
  }
}

export { ReliableWebSocket };
