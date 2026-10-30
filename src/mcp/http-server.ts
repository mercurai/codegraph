/** Authenticated local Streamable HTTP with one engine and request-local logical identity. */
import { createServer, type IncomingMessage, type ServerResponse } from 'http';
import * as fs from 'fs';
import * as path from 'path';
import { randomUUID } from 'crypto';
import type { Socket, AddressInfo } from 'net';
import type { MCPEngine } from './engine';
import { MCPSession } from './session';
import { ExploreSessionState } from './explore-session-state';
import { HttpTransport } from './http-transport';
import type { JsonRpcRequest, JsonRpcNotification } from './transport';

/** Resolved by trusted launch authentication, never by initialize roots or tool arguments. */
export interface AuthenticatedHttpLaunch {
  readonly launchId: string;
  readonly provider: 'claude' | 'codex';
  readonly providerHome: string;
  readonly workspace: string;
}

/** Native per-call metadata must resolve to a known conversation inside this launch scope. */
export interface HttpCallIdentity {
  readonly conversationId: string;
  readonly workspace: string;
}

export interface HttpMcpAuthority {
  authenticate(request: IncomingMessage): Promise<AuthenticatedHttpLaunch | null>;
  resolveCall(request: JsonRpcRequest, launch: AuthenticatedHttpLaunch): Promise<HttpCallIdentity | null>;
  /** Trusted scope liveness, independent of any incoming credential. Managed services supply this. */
  isActive?(launch: AuthenticatedHttpLaunch): boolean;
}

/** Same launch authority as MCP, with its distinct hook-purpose credential. */
export interface HttpHookAuthority {
  authenticateHook(request: IncomingMessage): unknown | Promise<unknown>;
  registerHook(request: IncomingMessage, payload: unknown): void | Promise<void>;
}

export interface HttpMcpServerOptions {
  engine: MCPEngine;
  authority: HttpMcpAuthority;
  hookAuthority?: HttpHookAuthority;
  host?: string;
  port?: number;
  allowedOrigins?: readonly string[];
  maxSessions?: number;
  maxConversations?: number;
  /** Active tool requests. Bounded ingress headroom keeps control messages reachable. */
  maxRequests?: number;
  maxPendingPerSession?: number;
  maxBodyBytes?: number;
}

export interface HttpMcpServer {
  /** Bound loopback URL, including the /mcp endpoint. */
  readonly url: string;
  address(): AddressInfo;
  health(): { state: 'ready' | 'draining' | 'closed'; transportSessions: number; pendingInitializations: number; inFlightRequests: number };
  /** Stops admission, drains transport work, and awaits the supplied engine's stop(). */
  stop(): Promise<void>;
}

const PROTOCOL = '2025-06-18';

class HttpRefusal extends Error {
  constructor(readonly status: number) { super(`HTTP ${status}`); }
}

function limit(value: number | undefined, fallback: number): number {
  const selected = value ?? fallback;
  if (!Number.isSafeInteger(selected) || selected < 1) throw new Error('HTTP limits must be positive integers');
  return selected;
}

function canonicalWorkspace(workspace: string): string {
  if (!path.isAbsolute(workspace)) throw new HttpRefusal(403);
  try { return fs.realpathSync(workspace); } catch { throw new HttpRefusal(403); }
}

function launchKey(launch: AuthenticatedHttpLaunch): string {
  return JSON.stringify([launch.launchId, launch.provider, launch.providerHome, launch.workspace]);
}

async function authenticate(authority: HttpMcpAuthority, request: IncomingMessage): Promise<AuthenticatedHttpLaunch> {
  const launch = await authority.authenticate(request);
  if (!launch) throw new HttpRefusal(401);
  if (!launch.launchId || !path.isAbsolute(launch.providerHome) || !['claude', 'codex'].includes(launch.provider)) throw new HttpRefusal(403);
  return Object.freeze({ ...launch, providerHome: path.resolve(launch.providerHome), workspace: canonicalWorkspace(launch.workspace) });
}

function readJsonBody(request: IncomingMessage, maxBytes: number): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let bytes = 0;
    const fail = (error: Error): void => { cleanup(); reject(error); };
    const aborted = (): void => fail(new HttpRefusal(400));
    const data = (chunk: Buffer): void => {
      bytes += chunk.length;
      if (bytes > maxBytes) { request.pause(); fail(new HttpRefusal(413)); return; }
      chunks.push(chunk);
    };
    const end = (): void => {
      cleanup();
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown);
      } catch { reject(new HttpRefusal(400)); }
    };
    const timer = setTimeout(() => { request.pause(); fail(new HttpRefusal(408)); }, 10_000);
    timer.unref();
    const cleanup = (): void => {
      clearTimeout(timer);
      request.off('data', data).off('end', end).off('error', fail).off('aborted', aborted);
    };
    request.on('data', data).once('end', end).once('error', fail).once('aborted', aborted);
  });
}

async function readMessage(request: IncomingMessage, maxBytes: number): Promise<JsonRpcRequest | JsonRpcNotification> {
  const message = await readJsonBody(request, maxBytes);
  if (!message || typeof message !== 'object' || Array.isArray(message)) throw new HttpRefusal(400);
  const value = message as Record<string, unknown>;
  if (value.jsonrpc !== '2.0' || typeof value.method !== 'string'
    || ('id' in value && typeof value.id !== 'string' && (typeof value.id !== 'number' || !Number.isFinite(value.id)))) throw new HttpRefusal(400);
  return value as unknown as JsonRpcRequest | JsonRpcNotification;
}

interface SessionEntry { launch: string; scope: AuthenticatedHttpLaunch; session: MCPSession; transport: HttpTransport }
interface ConversationEntry { scope: AuthenticatedHttpLaunch; state: ExploreSessionState }

export async function startHttpMcpServer(options: HttpMcpServerOptions): Promise<HttpMcpServer> {
  if (!options.engine.isExplicitProjectsOnly()) throw new Error('HTTP requires an explicit-project-only engine');
  const host = options.host ?? '127.0.0.1';
  if (host !== '127.0.0.1' && host !== '::1') throw new Error('HTTP MCP must bind a loopback IP');
  const maxSessions = limit(options.maxSessions, 64);
  const maxConversations = limit(options.maxConversations, 256);
  const maxRequests = limit(options.maxRequests, 64);
  const maxPending = limit(options.maxPendingPerSession, 16);
  const maxBody = limit(options.maxBodyBytes, 1024 * 1024);
  const ingressLimit = maxRequests + 8;
  const sessions = new Map<string, SessionEntry>();
  const draining = new Set<SessionEntry>();
  const conversations = new Map<string, ConversationEntry>();
  const active = new Set<Promise<void>>();
  const sockets = new Set<Socket>();
  let activeTools = 0;
  let pendingInitializations = 0;
  let closing = false;
  let closed = false;
  let authorityHost = '';
  const health = (): ReturnType<HttpMcpServer['health']> => ({
    state: closed ? 'closed' : closing ? 'draining' : 'ready', transportSessions: sessions.size,
    pendingInitializations, inFlightRequests: activeTools,
  });

  function createSession(launch: AuthenticatedHttpLaunch): [string, SessionEntry] {
    if (sessions.size + draining.size >= maxSessions) throw new HttpRefusal(429);
    const id = randomUUID();
    const key = launchKey(launch);
    const transport = new HttpTransport(id, maxPending);
    const session = new MCPSession(transport, options.engine, {
      protocolVersion: PROTOCOL,
      trustedScope: {
        workspace: launch.workspace,
        async resolveCall(request) {
          const identity = await options.authority.resolveCall(request, launch);
          if (!identity?.conversationId || canonicalWorkspace(identity.workspace) !== launch.workspace) return null;
          await sweepInactiveLaunches();
          if (closing || options.authority.isActive?.(launch) === false) return null;
          const conversationKey = JSON.stringify([key, identity.conversationId]);
          let conversation = conversations.get(conversationKey);
          if (!conversation) {
            if (conversations.size >= maxConversations) return null;
            conversation = { scope: launch, state: new ExploreSessionState() };
            conversations.set(conversationKey, conversation);
          }
          return { workspace: launch.workspace, exploreSession: conversation.state, signal: transport.requestSignal(request.id) };
        },
      },
    });
    session.start();
    const entry = { launch: key, scope: launch, session, transport };
    sessions.set(id, entry);
    return [id, entry];
  }

  function retire(id: string, entry: SessionEntry): void {
    sessions.delete(id);
    entry.session.stop();
    draining.add(entry);
    void entry.transport.whenIdle().then(() => draining.delete(entry));
  }

  async function sweepInactiveLaunches(): Promise<void> {
    if (!options.authority.isActive) return;
    const liveness = new Map<string, boolean>();
    const inactive = (scope: AuthenticatedHttpLaunch): boolean => {
      const key = launchKey(scope);
      if (!liveness.has(key)) liveness.set(key, options.authority.isActive!(scope));
      return liveness.get(key) === false;
    };
    for (const [id, entry] of sessions) if (inactive(entry.scope)) retire(id, entry);
    for (const [key, entry] of conversations) if (inactive(entry.scope)) conversations.delete(key);
    // Awaiting this sweep lets already-idle retirements release capacity. Busy
    // transports remain in draining until their actual handlers finish; removed
    // conversation state stays owned by those handlers and cannot be reused.
  }

  function validateRequest(request: IncomingMessage): void {
    if (request.headers.host !== authorityHost) throw new HttpRefusal(403);
    const origin = request.headers.origin;
    if (origin && origin !== `http://${authorityHost}` && !options.allowedOrigins?.includes(origin)) throw new HttpRefusal(403);
    if (request.url === '/health') return;
    if (closing) throw new HttpRefusal(503);
    if (request.url !== '/mcp' && !(request.url === '/hooks' && options.hookAuthority)) throw new HttpRefusal(404);
    if (request.url === '/hooks') return;
    const version = request.headers['mcp-protocol-version'];
    if (version !== undefined && version !== PROTOCOL) throw new HttpRefusal(400);
  }

  async function handleHook(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const hookAuthority = options.hookAuthority!;
    if (request.method !== 'POST') { response.setHeader('allow', 'POST'); throw new HttpRefusal(405); }
    const authenticateHook = async (): Promise<void> => {
      try { await hookAuthority.authenticateHook(request); } catch { throw new HttpRefusal(401); }
    };
    await authenticateHook();
    if (request.headers['content-type']?.split(';')[0]?.trim().toLowerCase() !== 'application/json') throw new HttpRefusal(415);
    const payload = await readJsonBody(request, maxBody);
    await authenticateHook();
    if (closing) throw new HttpRefusal(503);
    try { await hookAuthority.registerHook(request, payload); }
    catch { await authenticateHook(); throw new HttpRefusal(400); }
    response.writeHead(202, { 'cache-control': 'no-store' }); response.end();
  }

  async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    validateRequest(request);
    if (request.url === '/health') {
      if (request.method !== 'GET') { response.setHeader('allow', 'GET'); throw new HttpRefusal(405); }
      const status = health();
      response.writeHead(status.state === 'ready' ? 200 : 503, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      response.end(JSON.stringify(status)); return;
    }
    if (request.url === '/hooks') return handleHook(request, response);
    const launch = await authenticate(options.authority, request);
    const sessionId = request.headers['mcp-session-id'];
    if (Array.isArray(sessionId)) throw new HttpRefusal(400);
    let entry = sessionId ? sessions.get(sessionId) : undefined;
    if (sessionId && !entry) throw new HttpRefusal(404);
    if (entry && entry.launch !== launchKey(launch)) throw new HttpRefusal(403);
    if (request.method === 'DELETE') {
      if (!entry || !sessionId) throw new HttpRefusal(400);
      retire(sessionId, entry);
      response.writeHead(204); response.end(); return;
    }
    if (request.method !== 'POST') { response.setHeader('allow', 'POST, DELETE'); throw new HttpRefusal(405); }
    if (request.headers['content-type']?.split(';')[0]?.trim().toLowerCase() !== 'application/json') throw new HttpRefusal(415);
    const accept = request.headers.accept ?? '';
    if (!accept.includes('application/json') || !accept.includes('text/event-stream')) throw new HttpRefusal(406);
    const message = await readMessage(request, maxBody);
    const currentLaunch = await authenticate(options.authority, request);
    if (launchKey(currentLaunch) !== launchKey(launch)) throw new HttpRefusal(403);
    await sweepInactiveLaunches();
    if (options.authority.isActive?.(currentLaunch) === false) throw new HttpRefusal(401);
    if (closing) throw new HttpRefusal(503);
    if (sessionId && sessions.get(sessionId) !== entry) throw new HttpRefusal(404);
    let createdId: string | undefined;
    if (!entry) {
      if (message.method !== 'initialize' || !('id' in message)) throw new HttpRefusal(400);
      [createdId, entry] = createSession(currentLaunch);
      pendingInitializations++;
    } else if (message.method === 'initialize') throw new HttpRefusal(400);
    const tool = message.method === 'tools/call' && 'id' in message;
    // A duplicate must retain its specific refusal even when tool capacity is
    // full. Notifications and DELETE use bounded ingress, not tool reservations.
    const duplicate = 'id' in message && entry.transport.hasRequest(message.id);
    if (tool && !duplicate && activeTools >= maxRequests) throw new HttpRefusal(429);
    if (tool && !duplicate) activeTools++;
    try { await entry.transport.dispatch(message, response); }
    finally {
      if (tool && !duplicate) activeTools--;
      if (createdId) pendingInitializations--;
    }
    // Failed initialize must not leak an unreachable session or disclose its ID.
    if (createdId && !response.hasHeader('mcp-session-id')) {
      retire(createdId, entry);
    }
  }

  const server = createServer((request, response) => {
    if (active.size >= ingressLimit) { response.writeHead(429, { connection: 'close' }); response.end(); return; }
    const task = handle(request, response).catch((error: unknown) => {
      if (!response.writableEnded && !response.destroyed) {
        response.writeHead(error instanceof HttpRefusal ? error.status : 500, { connection: 'close' });
        response.end();
      }
    });
    active.add(task);
    void task.finally(() => active.delete(task));
  });
  server.requestTimeout = 15_000;
  server.headersTimeout = 10_000;
  server.maxConnections = ingressLimit;
  server.on('connection', (socket) => {
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(options.port ?? 0, host, () => { server.off('error', reject); resolve(); });
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('HTTP service failed to bind');
  authorityHost = `${host === '::1' ? '[::1]' : host}:${address.port}`;
  let stopping: Promise<void> | undefined;
  return {
    url: `http://${authorityHost}/mcp`,
    address: () => ({ ...address }),
    health,
    stop() {
      stopping ??= (async () => {
        closing = true;
        const listenerClosed = new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
        // Attach rejection handling now; engine cleanup may fail before we await it.
        void listenerClosed.catch(() => undefined);
        let failure: unknown;
        try {
          for (const [id, entry] of sessions) retire(id, entry);
          // Partial headers/bodies are not idle connections. Closing owned
          // sockets also settles body readers, while admitted jobs still drain.
          for (const socket of sockets) socket.destroy();
          await Promise.allSettled([...active]);
          await options.engine.stop();
        } catch (error) { failure = error; }
        finally {
          sessions.clear(); draining.clear(); conversations.clear();
          for (const socket of sockets) socket.destroy();
          try { await listenerClosed; } catch (error) { failure ??= error; }
          closed = true;
        }
        if (failure !== undefined) throw failure;
      })();
      return stopping;
    },
  };
}
