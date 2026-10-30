/** Real compiled CJS entry + packed public ESM control, over private pipes.
 * Fixture IPC observes real engine ownership and controls deterministic gates;
 * service commands and grants travel only through the production framed pipes.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as http from 'http';
import { execFileSync, spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { pathToFileURL } from 'url';
import { createRequire } from 'module';
import type { JsonRpcResponse } from '../src/mcp/transport';
import type { ToolResult } from '../src/mcp/tools';

const CodeGraph = (require('../dist') as typeof import('../src')).default;
const { WASM_RUNTIME_FLAGS } = require('../dist/extraction/wasm-runtime-flags') as typeof import('../src/extraction/wasm-runtime-flags');
type RecordValue = Record<string, unknown>;
interface Frame { type: string; value: RecordValue }
interface Grant { handle: string; mcpCapability: string; hookCapability: string }
interface Ready { generation: string; mcpUrl: string; hooksUrl: string; healthUrl: string; renew?: true }
interface Reply { status: number; headers: http.IncomingHttpHeaders; body: JsonRpcResponse | null }

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

function frame(type: string, value: unknown): Buffer {
  const bytes = Buffer.from(JSON.stringify(value));
  const header = Buffer.alloc(5); header[0] = type.charCodeAt(0); header.writeUInt32BE(bytes.length, 1);
  return Buffer.concat([header, bytes]);
}

const childScript = `
const path = require('path');
const childProcess = require('child_process');
const net = require('net');
const root = process.argv[1], mode = process.argv[2];
let engineStarts = 0, listens = 0, spawnAttempts = 0, stopFailure = false, gate, now = 100000;
const report = value => process.send?.(value);
if (mode === 'import' || mode === 'missing-flags') {
  for (const name of ['spawn', 'spawnSync', 'exec', 'execSync', 'execFile', 'execFileSync', 'fork']) {
    childProcess[name] = () => { spawnAttempts++; throw new Error('Managed entry attempted a subprocess'); };
  }
}
const listen = net.Server.prototype.listen;
net.Server.prototype.listen = function (...args) { listens++; return listen.apply(this, args); };
async function run() {
  if (mode === 'import') {
    const before = [process.stdin.listenerCount('data'), process.listenerCount('SIGTERM')];
    const entry = require(path.join(root, 'mcp/service-main.js'));
    report({ event: 'import', callable: typeof entry.main === 'function', listens, spawnAttempts,
      unchangedListeners: JSON.stringify(before) === JSON.stringify([process.stdin.listenerCount('data'), process.listenerCount('SIGTERM')]),
      loadedEngine: Boolean(require.cache[path.join(root, 'mcp/engine.js')]) });
    process.disconnect(); return;
  }
  const Graph = require(path.join(root, 'index.js')).default;
  const sync = Graph.prototype.sync;
  Graph.prototype.sync = async function (...args) {
    if (gate && !gate.entered && this.getProjectRoot() === gate.workspace) {
      gate.entered = true; report({ event: 'sync-entered' }); await gate.held;
    }
    return sync.apply(this, args);
  };
  const engineModule = require(path.join(root, 'mcp/engine.js'));
  const Original = engineModule.MCPEngine;
  engineModule.MCPEngine = class extends Original {
    constructor(...args) { super(...args); engineStarts++; }
    async stop() {
      await super.stop(); report({ event: 'engine-stopped' });
      if (stopFailure) throw new Error('Synthetic cleanup failure');
    }
  };
  process.on('message', message => {
    if (message.op === 'hold') {
      let release; const held = new Promise(resolve => { release = resolve; });
      gate = { workspace: message.workspace, held, release, entered: false };
    } else if (message.op === 'release') { gate?.release(); gate = undefined; }
    else if (message.op === 'stop-failure') stopFailure = true;
    else if (message.op === 'advance') now += message.advanceMs;
    setImmediate(() => report({ event: 'fixture-ack', id: message.id, engineStarts, listens }));
  });
  try {
    if (mode === 'legacy-authority') {
      const securityModule = require(path.join(root, 'mcp/managed-security.js'));
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
    await require(path.join(root, 'mcp/service-main.js')).main(process.stdin, process.stdout, () => new Date(now));
    report({ event: 'done', failed: false, engineStarts, listens, spawnAttempts });
  } catch (error) {
    report({ event: 'done', failed: true, errorCode: error?.code ?? null, engineStarts, listens, spawnAttempts });
    process.exitCode = 1;
  } finally { process.disconnect(); }
}
void run().catch(() => { report({ event: 'fixture-error' }); process.exitCode = 1; process.disconnect(); });
`;

describe('managed CodeGraph private control entry', { timeout: 30_000 }, () => {
  let fixture: string;
  let consumer: string;
  let loader: string;
  let home: string;
  let projectA: string;
  let projectB: string;
  let metadataPath: string;
  let packageMetadata: string;
  let sequence = 0;
  const children: ReturnType<typeof start>[] = [];
  const requests = new Set<http.ClientRequest>();

  beforeAll(async () => {
    const artifact = process.env.CONTEXT_MODE_PACKAGE_TARBALL;
    if (!artifact || !fs.existsSync(artifact)) throw new Error('Root must supply the real CONTEXT_MODE_PACKAGE_TARBALL');
    fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-control-')));
    consumer = path.join(fixture, 'consumer'); fs.mkdirSync(consumer);
    fs.cpSync(path.resolve(__dirname, '../dist'), path.join(consumer, 'dist'), { recursive: true });
    fs.writeFileSync(path.join(consumer, 'package.json'), JSON.stringify({ type: 'commonjs', private: true }));
    const dependency = path.join(consumer, 'node_modules/context-mode'); fs.mkdirSync(dependency, { recursive: true });
    execFileSync('tar', ['-xzf', path.resolve(artifact), '--strip-components=1', '-C', dependency], { stdio: 'pipe', windowsHide: true });
    metadataPath = path.join(dependency, 'package.json'); packageMetadata = fs.readFileSync(metadataPath, 'utf8');
    loader = path.join(fixture, 'public-control-boundary.mjs');
    fs.writeFileSync(loader, `
export async function resolve(specifier, context, nextResolve) {
  const result = await nextResolve(specifier, context);
  if (result.url.includes('/node_modules/context-mode/') &&
      !/\\/build\\/shared\\/(identity|launch-authority|control|service-entry)\\.js$/.test(result.url)) {
    throw new Error('CodeGraph control imported a context service or execution module');
  }
  return result;
}
`);
    home = path.join(fixture, 'home'); fs.mkdirSync(home);
    fs.mkdirSync(path.join(home, 'codex'));
    projectA = path.join(fixture, 'a'); projectB = path.join(fixture, 'b');
    for (const [directory, symbol] of [[projectA, 'controlAlpha'], [projectB, 'controlBeta']]) {
      fs.mkdirSync(directory!);
      fs.writeFileSync(path.join(directory!, 'sample.ts'), `export function ${symbol}() { return 7; }\n`);
      const graph = await CodeGraph.init(directory!, { config: { include: ['**/*.ts'], exclude: [] } });
      try { await graph.indexAll(); } finally { graph.close(); }
    }
  }, 60_000);

  afterEach(async () => {
    for (const request of requests) request.destroy();
    requests.clear();
    let cleanupFailed = false;
    for (const channel of children.splice(0)) {
      let timer: NodeJS.Timeout | undefined;
      try {
        if (channel.child.connected) channel.child.send({ op: 'release', id: -1 }, () => undefined);
        channel.child.stdin.end();
        const completed = await Promise.race([channel.exited.then(() => true), new Promise<false>((resolve) => {
          timer = setTimeout(() => resolve(false), 10_000);
        })]);
        if (completed) continue;
        cleanupFailed = true;
        if (channel.child.exitCode === null && channel.child.signalCode === null) channel.child.kill('SIGKILL');
        await channel.exited;
      } catch { cleanupFailed = true; }
      finally { clearTimeout(timer); }
    }
    if (metadataPath) fs.writeFileSync(metadataPath, packageMetadata);
    if (cleanupFailed) throw new Error('Owned control fixture did not drain after EOF');
  });
  afterAll(() => { if (fixture) fs.rmSync(fixture, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });

  function start(mode = 'service', flags: readonly string[] = WASM_RUNTIME_FLAGS, cwd = consumer) {
    const child = spawn(process.execPath, [...flags, '--no-warnings', '--loader', pathToFileURL(loader).href,
      '-e', childScript, path.join(consumer, 'dist'), mode], {
      cwd, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
      env: { ...process.env, NODE_PATH: path.resolve(__dirname, '../node_modules'), CODEGRAPH_TELEMETRY: '0',
        CODEGRAPH_QUERY_POOL_SIZE: '0', CODEGRAPH_PARSE_WORKERS: '1', CODEGRAPH_RESOLVE_WORKERS: '1',
        CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS: '0' },
    }) as ChildProcessWithoutNullStreams;
    const frames: Frame[] = [], events: RecordValue[] = [];
    let buffered = Buffer.alloc(0), available = deferred<void>(), stderr = '', framingError = false;
    const wake = () => { available.resolve(); available = deferred<void>(); };
    child.stdout.on('data', (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 5) {
        const length = buffered.readUInt32BE(1);
        if (!length || length > 65_536) { framingError = true; buffered = Buffer.alloc(0); wake(); return; }
        if (buffered.length < length + 5) return;
        try { frames.push({ type: String.fromCharCode(buffered[0]!), value: JSON.parse(buffered.subarray(5, length + 5).toString('utf8')) }); }
        catch { framingError = true; }
        buffered = buffered.subarray(length + 5); wake();
      }
    });
    child.stderr.on('data', (chunk: Buffer) => { stderr = (stderr + chunk.toString('utf8')).slice(-65_536); });
    child.stdin.on('error', () => undefined); // Early protocol rejection can close stdin before the fixture write.
    child.on('message', (message: RecordValue) => { events.push(message); wake(); });
    const exited = new Promise<number | null>((resolve, reject) => {
      child.once('error', reject); child.once('close', (code) => resolve(code));
    });
    void exited.catch(() => undefined);
    const wait = async () => {
      await Promise.race([available.promise, exited.then(() => { throw new Error('Control entry ended before expected evidence'); })]);
      if (framingError) throw new Error('Control entry emitted invalid framing');
    };
    const channel = { child, frames, events, exited,
      async next(): Promise<Frame> { while (!frames.length) await wait(); return frames.shift()!; },
      async event(name: string, id?: number): Promise<RecordValue> {
        for (;;) {
          const index = events.findIndex((event) => event.event === name && (id === undefined || event.id === id));
          if (index !== -1) return events.splice(index, 1)[0]!;
          await wait();
        }
      },
      async fixture(op: string, workspace?: string, advanceMs?: number): Promise<RecordValue> {
        const id = ++sequence;
        child.send({ op, id, workspace, advanceMs });
        return channel.event('fixture-ack', id);
      },
      write: (type: string, value: unknown) => child.stdin.write(frame(type, value)),
      cleanOutput: () => !framingError && buffered.length === 0,
      leaked: (value: string) => stderr.includes(value),
    };
    children.push(channel); return channel;
  }

  function options(watch = false) {
    return { ttlMs: 60_000, maxIdentities: 32, maxLaunches: 4, mcpServerName: 'codegraph',
      http: { host: '127.0.0.1', port: 0, allowedOrigins: [], maxSessions: 4, maxConversations: 8,
        maxRequests: 4, maxPendingPerSession: 4, maxBodyBytes: 4096 },
      engine: { watch, queryPool: false, queryPoolDefaultMax: 1, maxOpenProjects: 2, maxQueuedJobs: 4 } };
  }

  async function bootstrap(channel: ReturnType<typeof start>, watch = false): Promise<Ready> {
    channel.write('B', { v: 1, kind: 'codegraph', options: options(watch) });
    const ready = await channel.next(); expect(ready.type).toBe('Y');
    expect(ready.value.v).toBe(1); expect(ready.value.kind).toBe('codegraph');
    expect(typeof ready.value.generation).toBe('string');
    return ready.value as unknown as Ready;
  }

  async function rpc(channel: ReturnType<typeof start>, ready: Ready, op: string, extra: RecordValue = {}) {
    const id = String(++sequence); channel.write('R', { v: 1, id, generation: ready.generation, op, ...extra });
    const result = await channel.next(); expect(result.type).toBe('S');
    expect(result.value.id).toBe(id); expect(result.value.generation).toBe(ready.generation);
    return result.value;
  }

  async function issue(channel: ReturnType<typeof start>, ready: Ready, workspace = projectA): Promise<Grant> {
    const result = await rpc(channel, ready, 'issue', { metadata: {
      provider: 'codex', home: path.join(home, 'codex'), userHome: home, workspace,
    } });
    expect(result.ok).toBe(true);
    const grant = result.result as Grant;
    expect(Object.keys(grant).sort()).toEqual(['handle', 'hookCapability', 'mcpCapability']);
    for (const value of Object.values(grant)) expect(typeof value).toBe('string');
    return grant;
  }

  function send(url: string, body: unknown, capability?: string, session?: string, method = 'POST'): Promise<Reply> {
    return new Promise((resolve, reject) => {
      const request = http.request(url, { method, headers: {
        'content-type': 'application/json', accept: 'application/json, text/event-stream',
        'mcp-protocol-version': '2025-06-18', ...(capability ? { authorization: `Bearer ${capability}` } : {}),
        ...(session ? { 'mcp-session-id': session } : {}),
      } }, (response) => {
        let text = ''; response.setEncoding('utf8');
        response.on('data', (chunk: string) => { text += chunk; }); response.once('error', reject);
        response.once('end', () => {
          let body: JsonRpcResponse | null = null;
          try { body = text ? JSON.parse(text) as JsonRpcResponse : null; } catch { /* Bounded HTTP refusal. */ }
          resolve({ status: response.statusCode ?? 0, headers: response.headers, body });
        });
      });
      requests.add(request); request.once('close', () => requests.delete(request)); request.once('error', reject);
      request.setTimeout(10_000, () => request.destroy(new Error('Owned HTTP probe timed out')));
      request.end(body === undefined ? undefined : JSON.stringify(body));
    });
  }

  async function connect(ready: Ready, grant: Grant, workspace: string, native = 'root') {
    const hooked = await send(ready.hooksUrl, { hook_event_name: 'SessionStart', session_id: native, cwd: workspace }, grant.hookCapability);
    expect(hooked.status).toBe(202);
    const initialized = await send(ready.mcpUrl, { jsonrpc: '2.0', id: 0, method: 'initialize', params: {
      protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'private-control-fixture', version: '1' },
    } }, grant.mcpCapability);
    expect(initialized.status).toBe(200); expect(initialized.body?.error).toBeUndefined();
    expect(typeof initialized.headers['mcp-session-id']).toBe('string');
    return { session: initialized.headers['mcp-session-id'] as string, native };
  }

  function query(ready: Ready, grant: Grant, connection: Awaited<ReturnType<typeof connect>>, symbol: string) {
    return send(ready.mcpUrl, { jsonrpc: '2.0', id: ++sequence, method: 'tools/call', params: {
      name: 'codegraph_explore', _meta: { threadId: connection.native, sessionId: connection.native }, arguments: { query: symbol },
    } }, grant.mcpCapability, connection.session);
  }

  function textOf(reply: Reply): string {
    expect(reply.status).toBe(200); expect(reply.body?.error).toBeUndefined();
    const result = reply.body?.result as ToolResult; expect(result.isError).toBeFalsy();
    return result.content.map((part) => part.text ?? '').join('\n');
  }

  it('imports without reading stdin, starting an engine/listener or spawning a process', async () => {
    const channel = start('import');
    expect(await channel.event('import')).toEqual({ event: 'import', callable: true, listens: 0,
      spawnAttempts: 0, unchangedListeners: true, loadedEngine: false });
    expect(await channel.exited).toBe(0); expect(channel.frames).toEqual([]); expect(channel.cleanOutput()).toBe(true);
  });

  it('rejects missing runtime flags before bootstrap without a re-exec parent', async () => {
    const channel = start('missing-flags', []);
    const done = await channel.event('done');
    expect(done).toMatchObject({ failed: true, errorCode: 'MANAGED_CODEGRAPH_RUNTIME_FLAGS_REQUIRED',
      engineStarts: 0, listens: 0, spawnAttempts: 0 });
    expect(await channel.exited).toBe(1); expect(channel.frames).toEqual([]);
  });

  it('boots the packed control API and announces reachable aggregate health', async () => {
    const channel = start(); const ready = await bootstrap(channel);
    expect(new URL(ready.mcpUrl).pathname).toBe('/mcp'); expect(new URL(ready.hooksUrl).pathname).toBe('/hooks');
    const health = await send(ready.healthUrl, undefined, undefined, undefined, 'GET');
    expect(health.status).toBe(200);
    expect(health.body).toEqual({ state: 'ready', transportSessions: 0, pendingInitializations: 0, inFlightRequests: 0 });
    const status = await rpc(channel, ready, 'status'); expect(status.ok).toBe(true); expect(status.result).toEqual(health.body);
    expect(await channel.fixture('checkpoint')).toMatchObject({ engineStarts: 1, listens: 1 });
    expect((await rpc(channel, ready, 'shutdown')).result).toEqual({ closed: true });
    expect(await channel.exited).toBe(0); expect(channel.cleanOutput()).toBe(true);
  });

  it('advertises renewal and keeps the same private grant usable beyond its original expiry', async () => {
    const channel = start(); const ready = await bootstrap(channel);
    expect(ready.renew).toBe(true);
    const grant = await issue(channel, ready); const connection = await connect(ready, grant, projectA);
    expect(textOf(await query(ready, grant, connection, 'controlAlpha'))).toContain('export function controlAlpha');
    await channel.fixture('advance', undefined, 30_000);
    expect((await rpc(channel, ready, 'renew', { handle: grant.handle })).result).toEqual({ renewed: true });
    await channel.fixture('advance', undefined, 30_001);
    expect((await rpc(channel, ready, 'status')).ok).toBe(true);
    expect(textOf(await query(ready, grant, connection, 'controlAlpha'))).toContain('export function controlAlpha');
    expect((await send(ready.hooksUrl, { hook_event_name: 'SessionStart', session_id: 'late', cwd: projectA }, grant.hookCapability)).status).toBe(202);
    expect((await rpc(channel, ready, 'renew', { handle: grant.handle })).result).toEqual({ renewed: true });
    expect(await channel.fixture('checkpoint')).toMatchObject({ engineStarts: 1, listens: 1 });
    expect(channel.leaked(grant.mcpCapability) || channel.leaked(grant.hookCapability)).toBe(false);
  });

  it('omits the Ready renewal flag and serves requests with a legacy authority', async () => {
    const channel = start('legacy-authority'); const ready = await bootstrap(channel);
    expect(Object.hasOwn(ready, 'renew')).toBe(false);
    const grant = await issue(channel, ready); const connection = await connect(ready, grant, projectA);
    expect(textOf(await query(ready, grant, connection, 'controlAlpha'))).toContain('export function controlAlpha');
    expect((await rpc(channel, ready, 'shutdown')).result).toEqual({ closed: true });
    expect(await channel.exited).toBe(0);
  });

  it.each(['extra', 'invalid-limit', 'invalid-watch', 'invalid-host', 'wrong-kind'])('rejects bootstrap %s before an engine or listener exists', async (variant) => {
    const channel = start(); const bootstrap: RecordValue = { v: 1, kind: 'codegraph', options: options() };
    const config = bootstrap.options as RecordValue;
    if (variant === 'extra') config.secret = 'synthetic-unexpected-field';
    if (variant === 'invalid-limit') config.maxLaunches = 0;
    if (variant === 'invalid-watch') (config.engine as RecordValue).watch = 'false';
    if (variant === 'invalid-host') (config.http as RecordValue).host = '0.0.0.0';
    if (variant === 'wrong-kind') bootstrap.kind = 'context-mode';
    channel.write('B', bootstrap);
    expect(await channel.event('done')).toMatchObject({ failed: true, engineStarts: 0, listens: 0 });
    expect(await channel.exited).toBe(1); expect(channel.frames).toEqual([]);
    expect(channel.leaked('synthetic-unexpected-field')).toBe(false);
  });

  it('requires the packed public control export rather than a private file fallback', async () => {
    const metadata = JSON.parse(packageMetadata) as { exports: RecordValue };
    delete metadata.exports['./shared/control']; fs.writeFileSync(metadataPath, JSON.stringify(metadata));
    const channel = start(); channel.write('B', { v: 1, kind: 'codegraph', options: options() });
    expect(await channel.event('done')).toMatchObject({ failed: true, engineStarts: 0, listens: 0 });
    expect(await channel.exited).toBe(1); expect(channel.frames).toEqual([]);
  });

  it('loads control from the installation when cwd contains a resolvable shadow package', async () => {
    const workspace = path.join(fixture, 'shadow-workspace');
    const shadow = path.join(workspace, 'node_modules/context-mode');
    const entry = path.join(shadow, 'build/shared/control.js');
    const marker = path.join(workspace, 'shadow-loaded.marker');
    fs.mkdirSync(path.dirname(entry), { recursive: true });
    fs.writeFileSync(path.join(shadow, 'package.json'), JSON.stringify({ name: 'context-mode', type: 'module',
      exports: { './shared/control': './build/shared/control.js' } }));
    fs.writeFileSync(entry, `import { writeFileSync } from 'node:fs';
writeFileSync(${JSON.stringify(marker)}, 'shadow module evaluated');
throw new Error('Hostile cwd control package loaded');
export function runServiceControl() { throw new Error('Shadow control called'); }
`);
    // Establish that a cwd-anchored resolver would select this trap, without evaluating it.
    expect(createRequire(path.join(workspace, 'consumer.cjs')).resolve('context-mode/shared/control')).toBe(entry);
    expect(fs.existsSync(marker)).toBe(false);
    const channel = start('service', WASM_RUNTIME_FLAGS, workspace);
    const ready = await bootstrap(channel); const grant = await issue(channel, ready);
    const connection = await connect(ready, grant, projectA);
    expect(textOf(await query(ready, grant, connection, 'controlAlpha'))).toContain('export function controlAlpha');
    expect(await channel.fixture('checkpoint')).toMatchObject({ engineStarts: 1, listens: 1 });
    expect(fs.existsSync(marker)).toBe(false);
  });

  it('serves separate workspace grants through one engine/listener with no cross-project result', async () => {
    const channel = start(); const ready = await bootstrap(channel);
    const a = await issue(channel, ready, projectA), b = await issue(channel, ready, projectB);
    const one = await connect(ready, a, projectA), two = await connect(ready, b, projectB);
    const [alpha, beta] = await Promise.all([query(ready, a, one, 'controlAlpha'), query(ready, b, two, 'controlBeta')]);
    expect(textOf(alpha)).toContain('export function controlAlpha'); expect(textOf(alpha)).not.toContain('controlBeta');
    expect(textOf(beta)).toContain('export function controlBeta'); expect(textOf(beta)).not.toContain('controlAlpha');
    expect(await channel.fixture('checkpoint')).toMatchObject({ engineStarts: 1, listens: 1 });
    expect(channel.leaked(a.mcpCapability) || channel.leaked(a.hookCapability)).toBe(false);
  });

  it('revokes the opaque launch handle across MCP and hooks without an HTTP issuance route', async () => {
    const channel = start(); const ready = await bootstrap(channel); const grant = await issue(channel, ready);
    const connection = await connect(ready, grant, projectA);
    expect((await send(new URL('/issue', ready.mcpUrl).href, {})).status).toBe(404);
    const wrongPurpose = await send(ready.hooksUrl, {}, grant.mcpCapability); expect(wrongPurpose.status).toBe(401);
    expect((await rpc(channel, ready, 'revoke', { handle: grant.handle })).result).toEqual({ revoked: true });
    expect((await query(ready, grant, connection, 'controlAlpha')).status).toBe(401);
    expect((await send(ready.hooksUrl, {}, grant.hookCapability)).status).toBe(401);
  });

  it('retires the service on a wrong-generation request without echoing caller fields', async () => {
    const channel = start(); const ready = await bootstrap(channel);
    channel.write('R', { v: 1, id: 'wrong-generation', generation: 'old-generation', op: 'status' });
    expect(await channel.event('done')).toMatchObject({ failed: true, engineStarts: 1, listens: 1 });
    expect(await channel.exited).toBe(1); expect(channel.frames).toEqual([]);
    await expect(send(ready.healthUrl, undefined, undefined, undefined, 'GET')).rejects.toThrow();
  });

  it.each(['shutdown', 'eof'])('%s retains the admitted project until its real catch-up sync drains', async (operation) => {
    const channel = start(); const ready = await bootstrap(channel, true); const grant = await issue(channel, ready);
    const connection = await connect(ready, grant, projectA);
    await channel.fixture('hold', projectA);
    const tool = query(ready, grant, connection, 'controlAlpha').catch(() => undefined);
    await channel.event('sync-entered');
    if (operation === 'shutdown') channel.write('R', { v: 1, id: 'drain', generation: ready.generation, op: 'shutdown' });
    else channel.child.stdin.end();
    await channel.fixture('checkpoint');
    expect(channel.events.some((event) => event.event === 'done')).toBe(false);
    expect(channel.frames.some((message) => message.type === 'S')).toBe(false);
    await channel.fixture('release'); await tool;
    if (operation === 'shutdown') {
      const response = await channel.next(); expect(response.value).toMatchObject({ id: 'drain', ok: true, result: { closed: true } });
    }
    expect(await channel.event('done')).toMatchObject({ failed: false });
    expect(await channel.exited).toBe(0); expect(channel.cleanOutput()).toBe(true);
    const { readWriterLock } = require('../dist/mcp/writer-lock') as typeof import('../src/mcp/writer-lock');
    expect(readWriterLock(projectA)).toBeNull();
  });

  it('reports cleanup failure without a successful closed receipt', async () => {
    const channel = start(); const ready = await bootstrap(channel);
    await channel.fixture('stop-failure');
    const result = await rpc(channel, ready, 'shutdown');
    expect(result.ok).toBe(false); expect(result.error).toBe('cleanup-failed'); expect(result.result).toBeUndefined();
    expect(await channel.exited).toBe(1); expect(channel.cleanOutput()).toBe(true);
  });

  it('restarts with a new generation and refuses prior-generation grants', async () => {
    const first = start(); const previous = await bootstrap(first); const grant = await issue(first, previous);
    expect((await rpc(first, previous, 'shutdown')).ok).toBe(true); expect(await first.exited).toBe(0);
    const second = start(); const current = await bootstrap(second);
    expect(current.generation === previous.generation).toBe(false);
    expect((await send(current.hooksUrl, {}, grant.hookCapability)).status).toBe(401);
    expect((await send(current.mcpUrl, { jsonrpc: '2.0', id: 0, method: 'initialize', params: {} }, grant.mcpCapability)).status).toBe(401);
    expect((await rpc(second, current, 'revoke', { handle: grant.handle })).result).toEqual({ revoked: false });
  });
});
