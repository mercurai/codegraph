/**
 * First HTTP increment: real MCPSession + MCPEngine over loopback HTTP.
 * The authority is a trusted test fixture, not an alternate MCP dispatcher.
 * Only disposable indexes are created. Built modules keep the production lazy
 * CommonJS loaders on the same module graph as CodeGraph and the engine.
 *
 * Next increment: duplicate/in-flight IDs, explicit cancellation, disconnects,
 * bounded transport/conversation admission, and shutdown while jobs are active.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as http from 'http';
import * as os from 'os';
import * as path from 'path';
import { randomUUID } from 'crypto';
import { pathToFileURL } from 'url';
import type { MCPEngine as Engine } from '../src/mcp/engine';
import type { AuthenticatedHttpLaunch, HttpMcpAuthority, HttpMcpServer } from '../src/mcp/http-server';
import type { JsonRpcRequest, JsonRpcResponse } from '../src/mcp/transport';
import type { ToolResult } from '../src/mcp/tools';

const CodeGraph = (require('../dist') as typeof import('../src')).default;
const { MCPEngine } = require('../dist/mcp/engine') as typeof import('../src/mcp/engine');
const { startHttpMcpServer } = require('../dist/mcp/http-server') as typeof import('../src/mcp/http-server');
const PROTOCOL = '2025-06-18';

interface WireResponse {
  status: number;
  headers: http.IncomingHttpHeaders;
  body: JsonRpcResponse | null;
}

function send(url: string, body: unknown, headers: Record<string, string> = {}, method = 'POST'): Promise<WireResponse> {
  return new Promise((resolve, reject) => {
    const request = http.request(url, {
      method,
      headers: {
        'content-type': 'application/json',
        accept: 'application/json, text/event-stream',
        'mcp-protocol-version': PROTOCOL,
        ...headers,
      },
    }, (response) => {
      let text = '';
      response.setEncoding('utf8');
      response.on('data', (chunk: string) => { text += chunk; });
      response.once('error', reject);
      response.once('end', () => {
        let parsed: JsonRpcResponse | null = null;
        try { parsed = text ? JSON.parse(text) as JsonRpcResponse : null; } catch { /* Non-JSON HTTP refusal. */ }
        resolve({ status: response.statusCode ?? 0, headers: response.headers, body: parsed });
      });
    });
    request.once('error', reject);
    request.setTimeout(10_000, () => request.destroy(new Error('HTTP MCP test request timed out')));
    request.end(body === undefined ? undefined : JSON.stringify(body));
  });
}

function initialize(extra: Record<string, unknown> = {}): JsonRpcRequest {
  return {
    jsonrpc: '2.0', id: 0, method: 'initialize',
    params: { protocolVersion: PROTOCOL, capabilities: {}, clientInfo: { name: 'http-fixture', version: '1' }, ...extra },
  };
}

function toolCall(id: number, conversation: string | undefined, args: Record<string, unknown> = {}, name = 'codegraph_search'): JsonRpcRequest {
  return {
    jsonrpc: '2.0', id, method: 'tools/call',
    params: { name, arguments: { query: 'alphaSymbol', ...args },
      ...(conversation === undefined ? {} : { _meta: { 'fixture/conversation': conversation } }) },
  };
}

function resultText(response: WireResponse): string {
  expect(response.status).toBe(200);
  expect(response.body?.error).toBeUndefined();
  const result = response.body?.result as ToolResult;
  expect(result.isError).toBeFalsy();
  const text = result.content.map((item) => item.type === 'text' ? item.text : '').join('\n');
  // A no-results response echoes the query; that must not masquerade as a hit.
  expect(text).not.toMatch(/^No results found/);
  return text;
}

describe('authenticated shared HTTP MCP', { timeout: 30_000 }, () => {
  let root: string;
  let projectA: string;
  let projectB: string;
  let unindexed: string;
  let engine: Engine | undefined;
  let server: HttpMcpServer | undefined;
  let tokenA: string;
  let tokenB: string;
  let grants: Map<string, AuthenticatedHttpLaunch>;

  beforeAll(async () => {
    root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-http-')));
    projectA = path.join(root, 'a');
    projectB = path.join(root, 'b');
    unindexed = path.join(root, 'unindexed');
    fs.mkdirSync(unindexed);
    for (const [directory, symbol] of [[projectA, 'alphaSymbol'], [projectB, 'betaSymbol']]) {
      fs.mkdirSync(directory!, { recursive: true });
      fs.writeFileSync(path.join(directory!, 'sample.ts'), `export function ${symbol}() { return 1; }\n`);
      const graph = await CodeGraph.init(directory!, { config: { include: ['**/*.ts'], exclude: [] } });
      try { await graph.indexAll(); } finally { graph.close(); }
    }
  }, 60_000);

  beforeEach(async () => {
    grants = new Map();
    tokenA = authorize(projectA, 'codex');
    tokenB = authorize(projectB, 'claude');
    const authority: HttpMcpAuthority = {
      async authenticate(request) {
        return grants.get(request.headers.authorization ?? '') ?? null;
      },
      async resolveCall(request, launch) {
        const metadata = (request.params as { _meta?: Record<string, unknown> } | undefined)?._meta;
        const conversationId = metadata?.['fixture/conversation'];
        // Only known fixture conversations are admitted. Caller root hints do
        // not grant workspace access; production uses its native identity registry.
        if (conversationId !== 'one' && conversationId !== 'two') return null;
        return { conversationId, workspace: launch.workspace };
      },
    };
    engine = new MCPEngine({ explicitProjectsOnly: true, watch: false, queryPool: false });
    server = await startHttpMcpServer({ engine, authority, host: '127.0.0.1', port: 0 });
  });

  afterEach(async () => {
    try { await server?.stop(); } finally { await engine?.stop(); }
    server = undefined;
    engine = undefined;
    vi.restoreAllMocks();
  });

  afterAll(() => {
    if (root) fs.rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });

  function authorize(workspace: string, provider: 'claude' | 'codex'): string {
    const token = `Bearer ${randomUUID()}`;
    grants.set(token, { launchId: randomUUID(), provider, providerHome: path.join(root, provider), workspace });
    return token;
  }

  async function connect(token: string): Promise<Record<string, string>> {
    const response = await send(server!.url, initialize(), { authorization: token });
    expect(response.status).toBe(200);
    expect(response.body?.error).toBeUndefined();
    const session = response.headers['mcp-session-id'];
    expect(typeof session).toBe('string');
    return { authorization: token, 'mcp-session-id': session as string };
  }

  it('exposes only bounded aggregate health without allocating an MCP session', async () => {
    await connect(tokenA);
    const response = await send(new URL('/health', server!.url).href, undefined, {}, 'GET');
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ state: 'ready', transportSessions: 1,
      pendingInitializations: 0, inFlightRequests: 0 });
    expect(response.headers['cache-control']).toBe('no-store');
    expect(response.headers['mcp-session-id']).toBeUndefined();
    expect(server!.health().transportSessions).toBe(1);
  });

  it.each<Record<string, string>>([
    { host: 'attacker.invalid' },
    { origin: 'https://attacker.invalid' },
  ])('checks the health Host/Origin boundary before returning aggregates: %j', async (headers) => {
    const response = await send(new URL('/health', server!.url).href, undefined, headers, 'GET');
    expect(response.status).toBe(403);
    expect(response.body ?? {}).not.toHaveProperty('transportSessions');
    expect(server!.health().transportSessions).toBe(0);
  });

  it.each(['POST', 'DELETE', 'PUT'])('refuses health mutation method %s without allocating a session', async (method) => {
    const response = await send(new URL('/health', server!.url).href, undefined, {}, method);
    expect(response.status).toBe(405);
    expect(response.headers.allow).toBe('GET');
    expect(server!.health().transportSessions).toBe(0);
  });

  it('initializes with ID zero and discovers tools without conversation metadata or a default project', async () => {
    const response = await send(server!.url, initialize(), { authorization: tokenA });
    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({ jsonrpc: '2.0', id: 0, result: { protocolVersion: PROTOCOL, serverInfo: { name: 'codegraph' } } });
    const session = response.headers['mcp-session-id'];
    expect(typeof session).toBe('string');
    const list = await send(server!.url, { jsonrpc: '2.0', id: 1, method: 'tools/list' },
      { authorization: tokenA, 'mcp-session-id': session as string });
    const result = list.body?.result as { tools: Array<{ name: string }> };
    expect(list.status).toBe(200);
    // Discovery deliberately advertises explore only; compatibility tools
    // such as search remain callable but unlisted (mcp-tool-allowlist.test.ts).
    expect(result.tools.map((tool) => tool.name)).toContain('codegraph_explore');
    expect(engine!.hasDefaultCodeGraph()).toBe(false);
  });

  it('serves two indexed workspaces through one real engine without first-client routing', async () => {
    const [a, b] = await Promise.all([connect(tokenA), connect(tokenB)]);
    const [alpha, beta] = await Promise.all([
      send(server!.url, toolCall(1, 'one'), a),
      send(server!.url, toolCall(1, 'one', { query: 'betaSymbol' }), b),
    ]);
    expect(resultText(alpha)).toContain('alphaSymbol');
    expect(resultText(alpha)).not.toContain('betaSymbol');
    expect(resultText(beta)).toContain('betaSymbol');
    expect(resultText(beta)).not.toContain('alphaSymbol');
    expect(engine!.hasDefaultCodeGraph()).toBe(false);
  });

  it('requires authentication even on loopback and exposes no MCP session on refusal', async () => {
    const response = await send(server!.url, initialize());
    expect(response.status).toBe(401);
    expect(response.headers['mcp-session-id']).toBeUndefined();
    expect(response.body?.result).toBeUndefined();
  });

  it.each<Record<string, string>>([
    { host: 'attacker.invalid' },
    { origin: 'https://attacker.invalid' },
    { origin: 'http://localhost:49321' },
  ])('rejects untrusted Host/Origin before creating a session: %j', async (headers) => {
    const response = await send(server!.url, initialize(), { authorization: tokenA, ...headers });
    expect(response.status).toBe(403);
    expect(response.headers['mcp-session-id']).toBeUndefined();
  });

  it('revalidates launch ownership for both POST and DELETE', async () => {
    const a = await connect(tokenA);
    // Same provider, home and workspace do not authorize another launch's transport.
    const otherLaunch = authorize(projectA, 'codex');
    const stolen = { ...a, authorization: otherLaunch };
    const post = await send(server!.url, toolCall(2, 'one'), stolen);
    expect(post.status).toBe(403);
    const deletion = await send(server!.url, undefined, stolen, 'DELETE');
    expect(deletion.status).toBe(403);
    expect(resultText(await send(server!.url, toolCall(3, 'one'), a))).toContain('alphaSymbol');
  });

  it('rejects revoked launch credentials on an existing transport', async () => {
    const a = await connect(tokenA);
    grants.delete(tokenA);
    const response = await send(server!.url, toolCall(2, 'one'), a);
    expect(response.status).toBe(401);
    expect(response.body?.result).toBeUndefined();
  });

  it('rejects a caller projectPath outside the trusted workspace', async () => {
    const a = await connect(tokenA);
    const response = await send(server!.url, toolCall(2, 'one', { projectPath: projectB }), a);
    expect(response.body?.error).toMatchObject({ code: -32602 });
    expect(response.body?.result).toBeUndefined();
    expect(resultText(await send(server!.url, toolCall(3, 'one'), a))).toContain('alphaSymbol');
  });

  it.each([
    () => ({ rootUri: pathToFileURL(projectB).href }),
    () => ({ workspaceFolders: [{ uri: pathToFileURL(projectB).href, name: 'forged' }] }),
  ])('refuses initialization roots that conflict with authenticated launch scope', async (roots) => {
    const response = await send(server!.url, initialize(roots()), { authorization: tokenA });
    expect(response.body?.error).toMatchObject({ code: -32602 });
    expect(response.body?.result).toBeUndefined();
    expect(response.headers['mcp-session-id']).toBeUndefined();
    expect(engine!.hasDefaultCodeGraph()).toBe(false);
  });

  it('refuses missing per-call identity instead of reusing the most recent conversation', async () => {
    const a = await connect(tokenA);
    expect(resultText(await send(server!.url, toolCall(1, 'one'), a))).toContain('alphaSymbol');
    const response = await send(server!.url, toolCall(2, undefined), a);
    expect(response.body?.error).toMatchObject({ code: -32602 });
    expect(response.body?.result).toBeUndefined();
  });

  it('isolates two logical conversations multiplexed over one MCP transport', async () => {
    const a = await connect(tokenA);
    const execute = vi.spyOn(engine!.getToolHandler(), 'execute');
    for (const [id, conversation] of [[1, 'one'], [2, 'two'], [3, 'one']] as const) {
      const response = await send(server!.url, toolCall(id, conversation, { query: 'alphaSymbol' }, 'codegraph_explore'), a);
      expect(resultText(response)).toContain('sample.ts');
    }
    expect(execute).toHaveBeenCalledTimes(3);
    const states = execute.mock.calls.map((call) => call[2]);
    expect(states[0]).toBeDefined();
    expect(states[0]).toBe(states[2]);
    expect(states[0]).not.toBe(states[1]);
    expect(states[0]!.callCount(projectA)).toBe(2);
    expect(states[1]!.callCount(projectA)).toBe(1);
  });

  it('preserves logical conversation history across transport replacement without sharing it with another launch', async () => {
    const execute = vi.spyOn(engine!.getToolHandler(), 'execute');
    const a = await connect(tokenA);
    resultText(await send(server!.url, toolCall(1, 'one', { query: 'alphaSymbol' }, 'codegraph_explore'), a));
    expect((await send(server!.url, undefined, a, 'DELETE')).status).toBe(204);
    const replacement = await connect(tokenA);
    resultText(await send(server!.url, toolCall(2, 'one', { query: 'alphaSymbol' }, 'codegraph_explore'), replacement));
    const other = await connect(authorize(projectA, 'codex'));
    resultText(await send(server!.url, toolCall(3, 'one', { query: 'alphaSymbol' }, 'codegraph_explore'), other));
    const states = execute.mock.calls.map((call) => call[2]);
    expect(states[0]).toBe(states[1]);
    expect(states[0]).not.toBe(states[2]);
    expect(states[0]!.callCount(projectA)).toBe(2);
    expect(states[2]!.callCount(projectA)).toBe(1);
    expect((await send(server!.url, toolCall(4, 'one'), a)).status).toBe(404);
  });

  it('keeps discovery available for an unindexed workspace and returns guidance without adopting a neighbor', async () => {
    const session = await connect(authorize(unindexed, 'codex'));
    const list = await send(server!.url, { jsonrpc: '2.0', id: 1, method: 'tools/list' }, session);
    expect(list.body?.error).toBeUndefined();
    expect((list.body?.result as { tools: unknown[] }).tools.length).toBeGreaterThan(0);
    const text = resultText(await send(server!.url, toolCall(2, 'one'), session));
    expect(text).toMatch(/isn't indexed|not indexed/i);
    expect(text).not.toContain('alphaSymbol');
    expect(text).not.toContain('betaSymbol');
    expect(engine!.hasDefaultCodeGraph()).toBe(false);
  });
});
