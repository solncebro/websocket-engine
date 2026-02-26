import { WebSocketLogger } from "./types";

interface PinoLikeLogger {
  debug: (msg: string, ...args: unknown[]) => void;
  info: (msg: string, ...args: unknown[]) => void;
  warn: (msg: string, ...args: unknown[]) => void;
  error: (msg: string, ...args: unknown[]) => void;
  fatal: (msg: string, ...args: unknown[]) => void;
}

export function createPinoLogger(pinoLogger: PinoLikeLogger): WebSocketLogger {
  return {
    debug: msg => pinoLogger.debug(msg),
    info: msg => pinoLogger.info(msg),
    warn: msg => pinoLogger.warn(msg),
    error: msg => pinoLogger.error(msg),
    fatal: msg => pinoLogger.fatal(msg),
  };
}
