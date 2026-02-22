import type { WebSocketConfiguration } from "./types";

const DEFAULT_WEBSOCKET_CONFIGURATION: WebSocketConfiguration = {
  maxRetryAttempts: 15,
  initialRetryDelay: 1000,
  maxRetryDelay: 30000,
  retryDelayMultiplier: 1.8,
  connectionTimeout: 30000,
  pingInterval: 15000,
  pongTimeout: 10000,
  heartbeatGracePeriod: 3000,
  fastReconnectCodes: [1001, 1006, 1011, 1012, 1013, 1014],
  missedPongThreshold: 3,
};

export { DEFAULT_WEBSOCKET_CONFIGURATION };
