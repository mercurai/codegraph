/** JSON response transport for one Streamable HTTP MCP session (no SSE channel). */
import type { ServerResponse } from 'http';
import {
  ErrorCodes, type JsonRpcNotification, type JsonRpcRequest, type JsonRpcResponse,
  type JsonRpcTransport, type MessageHandler,
} from './transport';

export class HttpTransport implements JsonRpcTransport {
  private handler: MessageHandler | null = null;
  private stopped = false;
  private readonly pending = new Map<string | number, { response: ServerResponse; initialize: boolean; cancellation: AbortController }>();
  private readonly idleWaiters: Array<() => void> = [];

  constructor(private readonly sessionId: string, private readonly maxPending: number) {}

  start(handler: MessageHandler): void { this.handler = handler; }

  hasRequest(id: string | number): boolean { return this.pending.has(id); }

  requestSignal(id: string | number): AbortSignal | undefined { return this.pending.get(id)?.cancellation.signal; }

  whenIdle(): Promise<void> {
    return this.pending.size ? new Promise((resolve) => this.idleWaiters.push(resolve)) : Promise.resolve();
  }

  private cancel(params: unknown): void {
    const id = params && typeof params === 'object' ? (params as { requestId?: unknown }).requestId : undefined;
    if (typeof id !== 'string' && typeof id !== 'number') return;
    const request = this.pending.get(id);
    // initialize is not cancellable. Already completed/unknown requests are no-ops.
    if (!request || request.initialize || request.cancellation.signal.aborted) return;
    if (!request.response.writableEnded && !request.response.destroyed) {
      request.response.writeHead(204);
      request.response.end();
    }
    request.cancellation.abort();
    // The signal removes queued work or retires its owned worker. Keep this
    // reservation until the handler confirms completion/exit; suppress late replies.
  }

  stop(): void {
    this.stopped = true;
    for (const { response } of this.pending.values()) {
      if (!response.writableEnded && !response.destroyed) {
        response.writeHead(503);
        response.end();
      }
    }
    // Jobs retain their reservations until dispatch finishes; disconnect is not cancellation.
  }

  async dispatch(message: JsonRpcRequest | JsonRpcNotification, response: ServerResponse): Promise<void> {
    if (this.stopped || !this.handler) { response.writeHead(503); response.end(); return; }
    if (!('id' in message)) {
      if (message.method === 'notifications/cancelled') this.cancel(message.params);
      await this.handler(message);
      response.writeHead(202);
      response.end();
      return;
    }
    if (this.pending.has(message.id)) {
      response.writeHead(409);
      response.end();
      return;
    }
    if (this.pending.size >= this.maxPending) {
      response.writeHead(429);
      response.end();
      return;
    }
    this.pending.set(message.id, { response, initialize: message.method === 'initialize', cancellation: new AbortController() });
    try {
      await this.handler(message);
      if (!response.writableEnded) this.sendError(message.id, ErrorCodes.InternalError, 'Request did not produce a response');
    } catch {
      this.sendError(message.id, ErrorCodes.InternalError, 'MCP request failed');
    } finally {
      this.pending.delete(message.id);
      if (!this.pending.size) this.idleWaiters.splice(0).forEach((resolve) => resolve());
    }
  }

  send(message: JsonRpcResponse): void {
    if (message.id === null) return;
    const pending = this.pending.get(message.id);
    if (!pending || pending.response.writableEnded || pending.response.destroyed) return;
    const headers: Record<string, string> = { 'content-type': 'application/json', 'cache-control': 'no-store' };
    if (!pending.initialize || !message.error) headers['mcp-session-id'] = this.sessionId;
    for (const [name, value] of Object.entries(headers)) pending.response.setHeader(name, value);
    pending.response.writeHead(200);
    pending.response.end(JSON.stringify(message));
  }

  sendResult(id: string | number, result: unknown): void { this.send({ jsonrpc: '2.0', id, result }); }

  sendError(id: string | number | null, code: number, message: string, data?: unknown): void {
    this.send({ jsonrpc: '2.0', id, error: { code, message, ...(data === undefined ? {} : { data }) } });
  }

  /** This JSON-only transport has no unsolicited server-to-client channel. */
  notify(_method: string, _params?: unknown): void {}

  request(_method: string, _params?: unknown, _timeoutMs?: number): Promise<unknown> {
    return Promise.reject(new Error('Server-initiated requests require an SSE transport'));
  }
}
