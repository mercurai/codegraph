/** Real packed context-mode public exports consumed by CodeGraph's compiled CJS.
 * Issuance uses test-only IPC, never an HTTP issuance route. Capabilities stay
 * in memory and are never written to fixtures, logs, or assertion snapshots.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as http from 'http';
import { execFileSync, spawn, type ChildProcess } from 'child_process';
import { randomUUID } from 'crypto';
import { pathToFileURL } from 'url';
import type { ManagedLaunchGrant, ManagedLaunchMetadata } from '../src/mcp/managed-security';
import type { JsonRpcResponse } from '../src/mcp/transport';
import type { ToolResult } from '../src/mcp/tools';

const CodeGraph = (require('../dist') as typeof import('../src')).default;
const protocolVersion = '2025-06-18';
const ttlMs = 60_000;
interface Reply { status: number; body: JsonRpcResponse | null; headers: http.IncomingHttpHeaders }
interface Observation { state: number; project: string; tool: string }
interface ChildReply {
  id?: number; ready?: boolean; ok?: boolean; error?: string; value?: unknown;
  url?: string; hookUrl?: string; engineStarts?: number; listens?: number; barrier?: string; hasRenew?: boolean;
}

const childScript = `
const path = require('path');
const net = require('net');
const http = require('http');
let engineStarts = 0, listens = 0, now = 100000;
const settings = JSON.parse(process.argv[2]);
const Graph = require(path.join(process.argv[1], 'index.js')).default;
const sync = Graph.prototype.sync;
let syncGate;
Graph.prototype.sync = async function (...args) {
  if (syncGate && !syncGate.entered && this.getProjectRoot() === syncGate.workspace) {
    const gate = syncGate; gate.entered = true;
    process.send({ barrier: gate.barrier });
    await gate.held;
  }
  return sync.apply(this, args);
};
const listen = net.Server.prototype.listen;
net.Server.prototype.listen = function (...args) { listens++; return listen.apply(this, args); };
const emit = http.Server.prototype.emit;
http.Server.prototype.emit = function (event, ...args) {
  const result = emit.call(this, event, ...args);
  if (event === 'request' && args[0].url === '/hooks' && args[0].headers['x-fixture-barrier']) {
    const barrier = args[0].headers['x-fixture-barrier'];
    setImmediate(() => process.send({ barrier }));
  }
  return result;
};
const engineModule = require(path.join(process.argv[1], 'mcp/engine.js'));
const OriginalEngine = engineModule.MCPEngine;
const observations = [], states = new WeakMap();
let nextState = 0;
engineModule.MCPEngine = class extends OriginalEngine {
  constructor(...args) {
    super(...args); engineStarts++;
    const handler = this.getToolHandler(), execute = handler.execute.bind(handler);
    handler.execute = async (...call) => {
      const state = call[2];
      const gate = syncGate;
      if (state && !states.has(state)) states.set(state, ++nextState);
      observations.push({ state: state ? states.get(state) : 0, project: call[1].projectPath, tool: call[0] });
      try { return await execute(...call); }
      finally { if (gate && gate.workspace === call[1].projectPath) process.send({ barrier: gate.barrier + ':drained' }); }
    };
  }
};
let service;
process.on('message', async (message) => {
  if (message.op === 'exit') { process.exit(0); return; }
  try {
    let value;
    if (message.op === 'issue') value = service.issue(message.value);
    else if (message.op === 'revoke') value = service.revoke(message.value);
    else if (message.op === 'renew') value = service.renew(message.value);
    else if (message.op === 'advance') { now += message.value; value = now; }
    else if (message.op === 'stop') value = await service.stop();
    else if (message.op === 'observations') value = observations;
    else if (message.op === 'inspect') { await service.listen(); value = { address: service.address(), health: service.health(), listens }; }
    else if (message.op === 'health') value = service.health();
    else if (message.op === 'holdSync') {
      let release; const held = new Promise(resolve => { release = resolve; });
      syncGate = { ...message.value, held, release, entered: false };
    }
    else if (message.op === 'releaseSync') { syncGate?.release(); syncGate = undefined; }
    else throw new Error('Unknown fixture command');
    process.send({ id: message.id, ok: true, value });
  } catch { process.send({ id: message.id, ok: false }); }
});
(async () => {
  try {
    if (settings.legacyAuthority) {
      const securityModule = require(path.join(process.argv[1], 'mcp/managed-security.js'));
      const load = securityModule.loadManagedSecurity;
      securityModule.loadManagedSecurity = async () => {
        const security = await load();
        return { ...security, createSharedLaunchAuthority(options) {
          const authority = security.createSharedLaunchAuthority(options);
          if (typeof authority.renew !== 'function') throw new Error('Renewable fixture package required');
          const { renew, ...legacy } = authority;
          return legacy;
        } };
      };
    }
    const { startManagedCodeGraphService } = require(path.join(process.argv[1], 'mcp/managed-service.js'));
    service = await startManagedCodeGraphService({ now: () => new Date(now), ttlMs: ${ttlMs},
      maxIdentities: 64, maxLaunches: 8, mcpServerName: 'codegraph',
      http: { host: '127.0.0.1', port: 0, allowedOrigins: [], maxBodyBytes: 4096, ...settings.http },
      engine: { watch: false, queryPool: false, maxOpenProjects: 2, ...settings.engine } });
    process.send({ ready: true, url: service.url, hookUrl: service.hookUrl, engineStarts, listens, hasRenew: typeof service.renew === 'function' });
  } catch (error) {
    process.send({ ready: false, error: error instanceof Error ? error.message : 'Startup failed', engineStarts, listens });
  }
})();
`;

describe('managed CodeGraph composition with packed shared security', { timeout: 30_000 }, () => {
  let fixture: string;
  let consumer: string;
  let dependency: string;
  let dependencyMetadata: string;
  let identitySource: string;
  let loader: string;
  let projectA: string;
  let projectB: string;
  let home: string;
  let child: ChildProcess | undefined;
  let url: string;
  let hookUrl: string;
  let nextId = 0;
  const pending = new Map<number, (reply: ChildReply) => void>();
  const barriers = new Map<string, () => void>();
  const sockets = new Set<http.ClientRequest>();

  beforeAll(async () => {
    const artifact = process.env.CONTEXT_MODE_PACKAGE_TARBALL;
    if (!artifact || !fs.existsSync(artifact)) throw new Error('Root must supply the qualified CONTEXT_MODE_PACKAGE_TARBALL');
    fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-managed-')));
    consumer = path.join(fixture, 'consumer');
    fs.mkdirSync(consumer);
    fs.cpSync(path.resolve(__dirname, '../dist'), path.join(consumer, 'dist'), { recursive: true });
    fs.writeFileSync(path.join(consumer, 'package.json'), JSON.stringify({ type: 'commonjs', private: true }));
    dependency = path.join(consumer, 'node_modules', 'context-mode');
    fs.mkdirSync(dependency, { recursive: true });
    execFileSync('tar', ['-xzf', path.resolve(artifact), '--strip-components=1', '-C', dependency], { stdio: 'pipe', windowsHide: true });
    dependencyMetadata = fs.readFileSync(path.join(dependency, 'package.json'), 'utf8');
    identitySource = fs.readFileSync(path.join(dependency, 'build/shared/identity.js'), 'utf8');
    loader = path.join(fixture, 'public-boundary.mjs');
    fs.writeFileSync(loader, `
export async function resolve(specifier, context, nextResolve) {
  const result = await nextResolve(specifier, context);
  if (result.url.includes('/node_modules/context-mode/') &&
      !/\\/build\\/shared\\/(identity|launch-authority)\\.js$/.test(result.url)) {
    throw new Error('Managed integration loaded a context execution module');
  }
  return result;
}
`);
    home = path.join(fixture, 'home'); fs.mkdirSync(home);
    for (const provider of ['codex', 'claude-code']) fs.mkdirSync(path.join(home, provider));
    projectA = path.join(fixture, 'a'); projectB = path.join(fixture, 'b');
    for (const [directory, symbol] of [[projectA, 'managedAlpha'], [projectB, 'managedBeta']]) {
      fs.mkdirSync(directory!);
      fs.writeFileSync(path.join(directory!, 'sample.ts'), `export function ${symbol}() { return 1; }\n`);
      const graph = await CodeGraph.init(directory!, { config: { include: ['**/*.ts'], exclude: [] } });
      try { await graph.indexAll(); } finally { graph.close(); }
    }
  }, 60_000);

  afterEach(async () => {
    for (const request of sockets) request.destroy();
    sockets.clear();
    if (child && child.exitCode === null && child.signalCode === null) {
      await control('releaseSync');
      await control('stop'); // Exercise the real owner cleanup before terminating the IPC fixture.
      const owned = child;
      const exited = new Promise<void>((resolve) => owned.once('exit', () => resolve()));
      owned.kill('SIGTERM');
      await exited;
    }
    child = undefined; pending.clear(); barriers.clear();
    fs.writeFileSync(path.join(dependency, 'package.json'), dependencyMetadata);
    fs.writeFileSync(path.join(dependency, 'build/shared/identity.js'), identitySource);
  });

  afterAll(() => { if (fixture) fs.rmSync(fixture, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });

  async function start(expectReady = true, settings: {
    legacyAuthority?: boolean;
    http?: { maxSessions?: number; maxConversations?: number };
    engine?: { watch?: boolean; maxOpenProjects?: number };
  } = {}): Promise<ChildReply> {
    const processReady = new Promise<ChildReply>((resolve, reject) => {
      child = spawn(process.execPath, ['--no-warnings', '--loader', pathToFileURL(loader).href, '-e', childScript, path.join(consumer, 'dist'), JSON.stringify(settings)], {
        cwd: consumer, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
        // Only CodeGraph dependencies use NODE_PATH. The real context artifact
        // resolves first from this consumer's own node_modules/public exports.
        env: { ...process.env, NODE_PATH: path.resolve(__dirname, '../node_modules'), CODEGRAPH_TELEMETRY: '0', CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS: '0' },
      });
      child.stdout?.resume(); child.stderr?.resume();
      child.once('error', reject);
      child.once('exit', () => reject(new Error('Managed fixture exited before startup')));
      child.on('message', (message: ChildReply) => {
        if (message.ready !== undefined) resolve(message);
        else if (message.barrier) { barriers.get(message.barrier)?.(); barriers.delete(message.barrier); }
        else if (message.id !== undefined) { pending.get(message.id)?.(message); pending.delete(message.id); }
      });
    });
    const ready = await processReady;
    if (expectReady && !ready.ready) throw new Error(ready.error ?? 'Managed fixture did not start');
    if (ready.ready) {
      url = ready.url!; hookUrl = ready.hookUrl!;
      expect(ready.engineStarts).toBe(1);
      expect(ready.listens).toBe(1);
    }
    return ready;
  }

  function control(op: string, value?: unknown): Promise<ChildReply> {
    const id = ++nextId;
    return new Promise((resolve, reject) => {
      pending.set(id, resolve);
      child!.send({ id, op, value }, (error) => { if (error) { pending.delete(id); reject(error); } });
    });
  }

  function send(target: string, body: unknown, credential?: string, session?: string, method = 'POST') {
    let request!: http.ClientRequest;
    const reply = new Promise<Reply>((resolve, reject) => {
      request = http.request(target, { method, headers: {
        'content-type': 'application/json', accept: 'application/json, text/event-stream', 'mcp-protocol-version': protocolVersion,
        ...(credential ? { authorization: `Bearer ${credential}` } : {}), ...(session ? { 'mcp-session-id': session } : {}),
      } }, (response) => {
        let text = ''; response.setEncoding('utf8');
        response.on('data', (chunk: string) => { text += chunk; });
        response.once('error', reject);
        response.once('end', () => {
          let parsed: JsonRpcResponse | null = null;
          try { parsed = text ? JSON.parse(text) as JsonRpcResponse : null; } catch { /* Generic HTTP refusal. */ }
          resolve({ status: response.statusCode ?? 0, headers: response.headers, body: parsed });
        });
      });
      request.once('error', reject); request.once('close', () => sockets.delete(request));
      request.setTimeout(10_000, () => request.destroy(new Error('Managed HTTP fixture timed out')));
      sockets.add(request); request.end(JSON.stringify(body));
    });
    return reply;
  }

  async function issue(provider: ManagedLaunchMetadata['provider'] = 'codex', workspace = projectA): Promise<ManagedLaunchGrant> {
    const reply = await control('issue', { provider, workspace, home: path.join(home, provider), userHome: home });
    expect(reply.ok).toBe(true);
    return reply.value as ManagedLaunchGrant;
  }

  async function connect(grant: ManagedLaunchGrant) {
    const initialized = await send(url, { jsonrpc: '2.0', id: 0, method: 'initialize', params: {
      protocolVersion, capabilities: {}, clientInfo: { name: 'managed-fixture', version: '1' },
    } }, grant.mcpCapability);
    expect(initialized.status).toBe(200);
    expect(initialized.body?.error).toBeUndefined();
    const session = initialized.headers['mcp-session-id'] as string;
    const listed = await send(url, { jsonrpc: '2.0', id: 1, method: 'tools/list' }, grant.mcpCapability, session);
    const catalog = (listed.body?.result as { tools: Array<{ name: string }> }).tools;
    expect(catalog.length).toBeGreaterThan(0);
    return { grant, session, tool: catalog[0]!.name };
  }

  async function hook(grant: ManagedLaunchGrant, payload: Record<string, unknown>) {
    return send(hookUrl, payload, grant.hookCapability);
  }

  function nativeCall(connection: Awaited<ReturnType<typeof connect>>, meta: unknown, query = 'managedAlpha', extra: Record<string, unknown> = {}) {
    return send(url, { jsonrpc: '2.0', id: ++nextId, method: 'tools/call', params: {
      name: connection.tool, _meta: meta, arguments: { query, ...extra },
    } }, connection.grant.mcpCapability, connection.session);
  }

  function resultText(reply: Reply): string {
    expect(reply.status).toBe(200); expect(reply.body?.error).toBeUndefined();
    const result = reply.body?.result as ToolResult;
    expect(result.isError).toBeFalsy();
    return result.content.map((item) => item.text).join('\n');
  }

  it.each(['missing', 'malformed'] as const)('rejects a %s optional security package before creating engine or listener', async (kind) => {
    const metadata = JSON.parse(dependencyMetadata) as { exports: Record<string, unknown> };
    if (kind === 'missing') delete metadata.exports['./shared/identity'];
    else {
      const malformed = path.join(dependency, 'build/shared/identity.js');
      fs.writeFileSync(malformed, 'export const SharedIdentityRegistry = 7; export const createSharedLaunchAuthority = null;\n');
    }
    fs.writeFileSync(path.join(dependency, 'package.json'), JSON.stringify(metadata));
    const reply = await start(false);
    expect(reply.ready).toBe(false);
    expect(reply.error).toMatch(/context-mode.*(unavailable|incompatible|required|export)/i);
    expect(reply.engineStarts).toBe(0); expect(reply.listens).toBe(0);
  });

  it('loads the real security-only ESM exports from CJS and serves Codex root and child conversations on one transport', async () => {
    await start(); const grant = await issue(); const connection = await connect(grant);
    const inspected = await control('inspect');
    expect(inspected.ok).toBe(true);
    expect(inspected.value).toMatchObject({ address: { port: Number(new URL(url).port) }, listens: 1,
      health: { state: 'ready', transportSessions: 1, pendingInitializations: 0, inFlightRequests: 0 } });
    expect((await hook(grant, { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA })).status).toBe(202);
    expect((await hook(grant, { hook_event_name: 'SubagentStart', session_id: 'root', agent_id: 'child', cwd: projectA })).status).toBe(202);
    expect(resultText(await nativeCall(connection, { threadId: 'root', sessionId: 'root' }))).toContain('managedAlpha');
    expect(resultText(await nativeCall(connection, { threadId: 'child', sessionId: 'root' }))).toContain('managedAlpha');
    await nativeCall(connection, { threadId: 'root', sessionId: 'root' });
    const observations = (await control('observations')).value as Observation[];
    expect(observations.map((entry) => entry.project)).toEqual([projectA, projectA, projectA]);
    expect(observations[0]!.state).not.toBe(observations[1]!.state);
    expect(observations[0]!.state).toBe(observations[2]!.state);
  });

  it.each(['codex', 'claude-code'] as const)('renews %s capabilities and native identity beyond their original expiry', async (provider) => {
    expect((await start()).hasRenew).toBe(true);
    const grant = await issue(provider); const connection = await connect(grant);
    const untouched = await issue();
    const meta = provider === 'codex'
      ? { threadId: 'root', sessionId: 'root' }
      : { 'claudecode/toolUseId': 'use-renewed' };
    const registration = provider === 'codex'
      ? { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA }
      : { hook_event_name: 'PreToolUse', session_id: 'root', tool_use_id: 'use-renewed',
          cwd: projectA, tool_name: 'mcp__codegraph__' + connection.tool };
    expect((await hook(grant, registration)).status).toBe(202);
    expect(resultText(await nativeCall(connection, meta))).toContain('managedAlpha');
    expect((await control('advance', ttlMs / 2)).ok).toBe(true);
    expect((await control('renew', grant.scope)).ok).toBe(true);
    expect((await control('advance', ttlMs / 2 + 1)).ok).toBe(true);
    expect(resultText(await nativeCall(connection, meta))).toContain('managedAlpha');
    expect((await hook(grant, registration)).status).toBe(202);
    expect((await hook(untouched, { hook_event_name: 'SessionStart', session_id: 'expired', cwd: projectA })).status).toBe(401);
    const state = (await control('observations')).value as Observation[];
    expect(state).toHaveLength(2);
    expect(state[0]!.state).toBe(state[1]!.state);
    expect((await control('inspect')).value).toMatchObject({ listens: 1, health: { transportSessions: 1 } });
  });

  it('starts without exposing renewal when the real authority lacks that optional method', async () => {
    expect((await start(true, { legacyAuthority: true })).hasRenew).toBe(false);
    const grant = await issue(); const connection = await connect(grant);
    expect((await hook(grant, { hook_event_name: 'SessionStart', session_id: 'legacy', cwd: projectA })).status).toBe(202);
    expect(resultText(await nativeCall(connection, { threadId: 'legacy', sessionId: 'legacy' }))).toContain('managedAlpha');
  });

  it('registers Claude native tool-use identity using the real discovered tool catalog', async () => {
    await start(); const grant = await issue('claude-code'); const connection = await connect(grant);
    expect((await hook(grant, { hook_event_name: 'PreToolUse', session_id: 'claude-root', tool_use_id: 'use-1',
      cwd: projectA, tool_name: `mcp__codegraph__${connection.tool}` })).status).toBe(202);
    expect(resultText(await nativeCall(connection, { 'claudecode/toolUseId': 'use-1' }))).toContain('managedAlpha');
  });

  it('keeps collision-prone Claude session/agent tuples separate', async () => {
    await start(); const grant = await issue('claude-code'); const connection = await connect(grant);
    for (const [session, agent, toolUse] of [['left:right', 'tail', 'one'], ['left', 'right:tail', 'two']]) {
      expect((await hook(grant, { hook_event_name: 'PreToolUse', session_id: session, agent_id: agent, tool_use_id: toolUse,
        cwd: projectA, tool_name: `mcp__codegraph__${connection.tool}` })).status).toBe(202);
      expect(resultText(await nativeCall(connection, { 'claudecode/toolUseId': toolUse }))).toContain('managedAlpha');
    }
    const observations = (await control('observations')).value as Observation[];
    expect(observations[0]!.state).not.toBe(observations[1]!.state);
  });

  it('routes two providers and workspaces through the one composed engine', async () => {
    await start(); const a = await connect(await issue()); const b = await connect(await issue('claude-code', projectB));
    await hook(a.grant, { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA });
    await hook(b.grant, { hook_event_name: 'PreToolUse', session_id: 'root', tool_use_id: 'use', cwd: projectB, tool_name: `mcp__codegraph__${b.tool}` });
    const [alpha, beta] = await Promise.all([nativeCall(a, { threadId: 'root', sessionId: 'root' }), nativeCall(b, { 'claudecode/toolUseId': 'use' }, 'managedBeta')]);
    expect(resultText(alpha)).toContain('managedAlpha'); expect(resultText(alpha)).not.toContain('managedBeta');
    expect(resultText(beta)).toContain('managedBeta'); expect(resultText(beta)).not.toContain('managedAlpha');
  });

  it.each(['missing-meta', 'wrong-lineage', 'unregistered-child', 'forged-arguments'] as const)('refuses %s without dispatching a tool', async (kind) => {
    await start(); const connection = await connect(await issue());
    await hook(connection.grant, { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA });
    const meta = kind === 'wrong-lineage' ? { threadId: 'root', sessionId: 'other' }
      : kind === 'unregistered-child' ? { threadId: 'child', sessionId: 'root' } : undefined;
    const reply = await nativeCall(connection, meta, 'managedAlpha', kind === 'forged-arguments' ? { threadId: 'root', sessionId: 'root', workspace: projectA } : {});
    expect(reply.body?.result).toBeUndefined(); expect(reply.body?.error).toBeDefined();
    expect(((await control('observations')).value as unknown[]).length).toBe(0);
  });

  it.each(['wrong-workspace', 'unknown-parent', 'wrong-tool', 'conflicting-native-id'] as const)('rejects %s hook registration', async (kind) => {
    await start(); const grant = await issue(kind === 'wrong-tool' ? 'claude-code' : 'codex');
    await connect(grant);
    let payload: Record<string, unknown> = { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectB };
    if (kind === 'unknown-parent') payload = { hook_event_name: 'SubagentStart', session_id: 'unknown', agent_id: 'child', cwd: projectA };
    if (kind === 'wrong-tool') payload = { hook_event_name: 'PreToolUse', session_id: 'root', tool_use_id: 'use', cwd: projectA, tool_name: 'mcp__other_service__other_tool' };
    if (kind === 'conflicting-native-id') {
      await hook(grant, { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA });
      await hook(grant, { hook_event_name: 'SessionStart', session_id: 'other-root', cwd: projectA });
      payload = { hook_event_name: 'SubagentStart', session_id: 'other-root', agent_id: 'root', cwd: projectA };
    }
    expect((await hook(grant, payload)).status).toBe(400);
  });

  it('keeps hook and MCP capabilities purpose-specific', async () => {
    await start(); const grant = await issue();
    expect((await send(hookUrl, { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA }, grant.mcpCapability)).status).toBe(401);
    expect((await send(url, { jsonrpc: '2.0', id: 0, method: 'initialize', params: { protocolVersion, capabilities: {} } }, grant.hookCapability)).status).toBe(401);
  });

  it('rejects another launch borrowing a valid MCP transport session', async () => {
    await start(); const a = await connect(await issue()); const b = await issue();
    const response = await send(url, { jsonrpc: '2.0', id: 2, method: 'ping' }, b.mcpCapability, a.session);
    expect(response.status).toBe(403);
  });

  it.each(['revoke', 'expire'] as const)('invalidates both capabilities after %s', async (action) => {
    await start(); const grant = await issue(); const connection = await connect(grant);
    await hook(grant, { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA });
    expect(resultText(await nativeCall(connection, { threadId: 'root', sessionId: 'root' }))).toContain('managedAlpha');
    expect((await control(action === 'revoke' ? 'revoke' : 'advance', action === 'revoke' ? grant.scope : ttlMs)).ok).toBe(true);
    expect((await nativeCall(connection, { threadId: 'root', sessionId: 'root' })).status).toBe(401);
    expect((await hook(grant, { hook_event_name: 'SessionStart', session_id: 'later', cwd: projectA })).status).toBe(401);
  });

  it.each(['revoke', 'expire'] as const)('reclaims transport and conversation capacity after %s without a DELETE from the dead launch', async (action) => {
    await start(true, { http: { maxSessions: 1, maxConversations: 1 } });
    const old = await connect(await issue());
    await hook(old.grant, { hook_event_name: 'SessionStart', session_id: 'old', cwd: projectA });
    expect(resultText(await nativeCall(old, { threadId: 'old', sessionId: 'old' }))).toContain('managedAlpha');
    expect((await control(action === 'revoke' ? 'revoke' : 'advance', action === 'revoke' ? old.grant.scope : ttlMs)).ok).toBe(true);
    const replacement = await connect(await issue());
    expect((await hook(replacement.grant, { hook_event_name: 'SessionStart', session_id: 'replacement', cwd: projectA })).status).toBe(202);
    expect(resultText(await nativeCall(replacement, { threadId: 'replacement', sessionId: 'replacement' }))).toContain('managedAlpha');
    const observations = (await control('observations')).value as Observation[];
    expect(observations[0]!.state).not.toBe(observations[1]!.state);
    expect((await control('health')).value).toMatchObject({ transportSessions: 1, inFlightRequests: 0 });
    expect((await send(url, undefined, old.grant.mcpCapability, old.session, 'DELETE')).status).toBe(401);
  });

  it('retires revoked transports on the next admission while retaining request and project reservations until real sync finishes', async () => {
    await start(true, { http: { maxSessions: 1, maxConversations: 1 }, engine: { watch: true, maxOpenProjects: 1 } });
    const old = await connect(await issue());
    await hook(old.grant, { hook_event_name: 'SessionStart', session_id: 'old', cwd: projectA });
    const barrier = randomUUID();
    const entered = new Promise<void>((resolve) => barriers.set(barrier, resolve));
    const drained = new Promise<void>((resolve) => barriers.set(`${barrier}:drained`, resolve));
    expect((await control('holdSync', { workspace: projectA, barrier })).ok).toBe(true);
    const active = nativeCall(old, { threadId: 'old', sessionId: 'old' });
    void active.catch(() => undefined);
    try {
      await entered;
      const writerPath = path.join(projectA, '.codegraph', 'writer.pid');
      const writer = fs.readFileSync(writerPath, 'utf8');
      expect((await control('revoke', old.grant.scope)).ok).toBe(true);
      const replacement = await issue('codex', projectB);
      const pendingInitialization = await send(url, { jsonrpc: '2.0', id: 0, method: 'initialize', params: { protocolVersion, capabilities: {} } }, replacement.mcpCapability);
      expect(pendingInitialization.status).toBe(429);
      expect((await control('health')).value).toMatchObject({ transportSessions: 0, inFlightRequests: 1 });
      expect(fs.readFileSync(writerPath, 'utf8')).toBe(writer);
      expect((await control('releaseSync')).ok).toBe(true);
      await drained;
      expect((await active).status).toBe(503);
      const connection = await connect(replacement);
      await hook(replacement, { hook_event_name: 'SessionStart', session_id: 'new', cwd: projectB });
      expect(resultText(await nativeCall(connection, { threadId: 'new', sessionId: 'new' }, 'managedBeta'))).toContain('managedBeta');
    } finally { await control('releaseSync'); await Promise.allSettled([active]); }
  });

  it('does not let an invalid credential or another launch retire a valid transport', async () => {
    await start(true, { http: { maxSessions: 1 } }); const live = await connect(await issue());
    await hook(live.grant, { hook_event_name: 'SessionStart', session_id: 'live', cwd: projectA });
    const other = await issue();
    expect((await send(url, undefined, randomUUID(), live.session, 'DELETE')).status).toBe(401);
    expect((await send(url, undefined, other.mcpCapability, live.session, 'DELETE')).status).toBe(403);
    expect((await control('health')).value).toMatchObject({ transportSessions: 1 });
    expect(resultText(await nativeCall(live, { threadId: 'live', sessionId: 'live' }))).toContain('managedAlpha');
  });

  it('authenticates hooks before waiting for a body and again after a partial body', async () => {
    await start(); const grant = await issue();
    async function partial(credential: string, revoke: boolean): Promise<number> {
      const barrier = randomUUID();
      let seen!: () => void;
      const received = new Promise<void>((resolve) => { seen = resolve; });
      barriers.set(barrier, seen);
      const payload = JSON.stringify({ hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA });
      let request!: http.ClientRequest;
      const reply = new Promise<number>((resolve, reject) => {
        request = http.request(hookUrl, { method: 'POST', headers: {
          authorization: `Bearer ${credential}`, 'content-type': 'application/json', 'x-fixture-barrier': barrier,
        } }, (response) => { response.resume(); response.once('end', () => resolve(response.statusCode ?? 0)); });
        request.once('error', reject); request.setTimeout(10_000, () => request.destroy(new Error('Hook pre-body authentication timed out')));
        sockets.add(request); request.once('close', () => sockets.delete(request));
        if (revoke) request.write(payload.slice(0, 10)); else request.flushHeaders();
      });
      if (revoke) {
        await received;
        expect((await control('revoke', grant.scope)).ok).toBe(true);
        request.end(payload.slice(10));
      }
      const status = await reply;
      request.destroy();
      return status;
    }
    expect(await partial(grant.mcpCapability, false)).toBe(401);
    expect(await partial(grant.hookCapability, true)).toBe(401);
  });

  it('bounds hook bodies and exposes no HTTP issuance route', async () => {
    await start(); const grant = await issue();
    expect((await hook(grant, { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA, ignored: 'x'.repeat(5000) })).status).toBe(413);
    for (const route of ['/issue', '/launches', '/register']) {
      expect((await send(new URL(route, url).href, {}, grant.mcpCapability)).status).toBe(404);
    }
    expect((await hook(grant, { hook_event_name: 'SessionStart', session_id: 'root', cwd: projectA })).status).toBe(202);
  });

  it('closes admission, listener, and authority together without allowing issuance after stop', async () => {
    await start(); const grant = await issue(); await connect(grant);
    expect((await control('stop')).ok).toBe(true);
    expect((await control('health')).value).toMatchObject({ state: 'closed', transportSessions: 0, pendingInitializations: 0, inFlightRequests: 0 });
    expect((await control('issue', { provider: 'codex', workspace: projectA, home: path.join(home, 'codex'), userHome: home })).ok).toBe(false);
    await expect(send(url, {}, grant.mcpCapability)).rejects.toMatchObject({ code: 'ECONNREFUSED' });
    expect((await control('stop')).ok).toBe(true);
  });
});
