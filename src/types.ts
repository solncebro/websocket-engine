import type { RawData } from "ws";

export enum WebSocketStatus {
  CONNECTING = "connecting",
  CONNECTED = "connected",
  DISCONNECTED = "disconnected",
  RECONNECTING = "reconnecting",
  FAILED = "failed",
}

export interface WebSocketConfiguration {
  maxRetryAttempts: number;
  initialRetryDelay: number;
  maxRetryDelay: number;
  retryDelayMultiplier: number;
  connectionTimeout: number;
  pingInterval: number;
  pongTimeout: number;
  heartbeatGracePeriod: number;
  fastReconnectCodes: number[];
  missedPongThreshold: number;
}

export interface WebSocketLogger {
  debug: (message: string, ...args: unknown[]) => void;
  info: (message: string, ...args: unknown[]) => void;
  warn: (message: string, ...args: unknown[]) => void;
  error: (message: string, ...args: unknown[]) => void;
  fatal: (message: string, ...args: unknown[]) => void;
}

export interface WebSocketHeartbeatOptions<TMessage> {
  buildPayload: () => Record<string, unknown>;
  isResponse: (message: TMessage) => boolean;
}

export interface WebSocketOpenContext<TMessage> {
  send: (data: unknown) => void;
  waitForMessage: (
    predicate: (message: TMessage) => boolean,
    timeoutMilliseconds: number
  ) => Promise<TMessage>;
}

export interface ReliableWebSocketArgs<TMessage = RawData> {
  url: string;
  label: string;
  logger: WebSocketLogger;
  onMessage: (message: TMessage) => void;
  parseMessage?: (rawData: RawData) => TMessage;
  onOpen?: (context: WebSocketOpenContext<TMessage>) => Promise<void>;
  onReconnectSuccess?: () => void;
  onNotify?: (message: string) => void | Promise<void>;
  configuration?: Partial<WebSocketConfiguration>;
  heartbeat?: WebSocketHeartbeatOptions<TMessage>;
}
