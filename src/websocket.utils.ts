import { WebSocket } from "ws";

import type { WebSocketConfiguration } from "./types";
import { WebSocketStatus } from "./types";

export interface ConnectionInfo {
  retryCount: number;
  status: WebSocketStatus;
  consecutiveFailures: number;
  missedPongCount: number;
  hasEverConnected: boolean;
  connectionStartedAt?: number;
  pingInterval?: NodeJS.Timeout;
  pongTimeout?: NodeJS.Timeout;
  reconnectTimeout?: NodeJS.Timeout;
  connectionTimeout?: NodeJS.Timeout;
  lastPongReceivedAt?: number;
}

export const isWebSocketClosable = (websocket: WebSocket): boolean =>
  websocket.readyState === WebSocket.OPEN ||
  websocket.readyState === WebSocket.CONNECTING;

export const clearTimers = (connectionInfo: ConnectionInfo): void => {
  if (connectionInfo.pingInterval) {
    clearInterval(connectionInfo.pingInterval);
    connectionInfo.pingInterval = undefined;
  }

  if (connectionInfo.pongTimeout) {
    clearTimeout(connectionInfo.pongTimeout);
    connectionInfo.pongTimeout = undefined;
  }

  if (connectionInfo.reconnectTimeout) {
    clearTimeout(connectionInfo.reconnectTimeout);
    connectionInfo.reconnectTimeout = undefined;
  }

  if (connectionInfo.connectionTimeout) {
    clearTimeout(connectionInfo.connectionTimeout);
    connectionInfo.connectionTimeout = undefined;
  }
};

export const calcExponentialDelay = (
  retryCount: number,
  configuration: WebSocketConfiguration
): number => {
  const base = Math.min(
    configuration.initialRetryDelay *
      Math.pow(configuration.retryDelayMultiplier, retryCount),
    configuration.maxRetryDelay
  );
  const jitter = Math.random() * 0.3 * base;

  return Math.floor(base + jitter);
};

export const calcReconnectDelay = (
  consecutiveFailures: number,
  closeCode: number | undefined,
  configuration: WebSocketConfiguration
): number => {
  const isFastReconnect =
    closeCode !== undefined &&
    configuration.fastReconnectCodes.includes(closeCode);

  if (isFastReconnect && consecutiveFailures <= 3) {
    return 500 + Math.floor(Math.random() * 1000);
  }

  let delay = calcExponentialDelay(
    Math.max(consecutiveFailures - 1, 0),
    configuration
  );

  if (consecutiveFailures > 10) {
    delay = Math.max(delay, configuration.maxRetryDelay);
  } else if (consecutiveFailures <= 2) {
    delay = Math.min(delay, 3000);
  }

  return delay;
};
