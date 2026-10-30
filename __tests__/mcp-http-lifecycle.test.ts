/**
 * Shared HTTP lifecycle acceptance. All dispatch uses the built production
 * HTTP server, MCPSession, MCPEngine and disposable indexes. Deferred sync
 * barriers hold real work; they do not replace the protocol or tool handler.
 *
 * Capacity contract: deleting a transport removes its lookup immediately,
 * but its active/draining reservation survives until its jobs finish. A socket
 * disconnect is not cancellation. Cancellation names a request on the same
 * authenticated transport; it needs no invented conversation metadata.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as http from 'http';
import * as net from 'net';
import * as os from 'os';
import * as path from 'path';
import { randomUUID } from 'crypto';
import { execFileSync } from 'child_process';
import { EventEmitter } from 'events';
import { Worker } from 'worker_threads';
import type { QueryPool as Pool, PoolWorker } from '../src/mcp/query-pool';
import type { MCPEngineOptions, MCPEngine as Engine } from '../src/mcp/engine';
import type { AuthenticatedHttpLaunch, HttpMcpServer, HttpMcpServerOptions } from '../src/mcp/http-server';
import type { JsonRpcResponse } from '../src/mcp/transport';
import type { ToolResult } from '../src/mcp/tools';

const CodeGraph = (require('../dist') as typeof import('../src')).default;
const { MCPEngine } = require('../dist/mcp/engine') as typeof import('../src/mcp/engine');
const { startHttpMcpServer } = require('../dist/mcp/http-server') as typeof import('../src/mcp/http-server');
const { QueryPool } = require('../dist/mcp/query-pool') as typeof import('../src/mcp/query-pool');
const protocolVersion = '2025-06-18';
const initialize = { jsonrpc: '2.0', id: 0, method: 'initialize', params: { protocolVersion, capabilities: {} } };

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

function call(id: number, query = 'alphaSymbol', conversation = 'one') {
  return { jsonrpc: '2.0', id, method: 'tools/call', params: {
    name: 'codegraph_search', arguments: { query }, _meta: { 'fixture/conversation': conversation },
  } };
}

function cancel(requestId: number) {
  return { jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId } };
}

interface Reply { status: number; headers: http.IncomingHttpHeaders; body: JsonRpcResponse | null }
type Headers = Record<string, string>;

describe('shared HTTP request and resource lifetime', { timeout: 30_000 }, () => {
  let root: string;
  let projectA: string;
  let projectB: string;
  let engine: Engine | undefined;
  let server: HttpMcpServer | undefined;
  let tokenA: string;
  let tokenB: string;
  const grants = new Map<string, AuthenticatedHttpLaunch>();
  const requests = new Set<http.ClientRequest>();
  const releases: Array<() => void> = [];
  const pools: Pool[] = [];
  const previousGate = process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS;

  beforeAll(async () => {
    root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-http-life-')));
    projectA = path.join(root, 'a'); projectB = path.join(root, 'b');
    for (const [directory, symbol] of [[projectA, 'alphaSymbol'], [projectB, 'betaSymbol']]) {
      fs.mkdirSync(directory!);
      fs.writeFileSync(path.join(directory!, 'sample.ts'), `export function ${symbol}() { return 1; }\n`);
      const graph = await CodeGraph.init(directory!, { config: { include: ['**/*.ts'], exclude: [] } });
      try { await graph.indexAll(); } finally { graph.close(); }
    }
  }, 60_000);

  beforeEach(() => {
    process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS = '0';
    for (const [workspace, provider] of [[projectA, 'codex'], [projectB, 'claude']] as const) {
      const token = `Bearer ${randomUUID()}`;
      grants.set(token, { workspace, provider, launchId: randomUUID(), providerHome: path.join(root, provider) });
      if (workspace === projectA) tokenA = token; else tokenB = token;
    }
  });

  afterEach(async () => {
    releases.splice(0).forEach((release) => release());
    try { await server?.stop(); } finally {
      await Promise.all(pools.splice(0).map((pool) => pool.destroy()));
      await engine?.stop();
      for (const request of requests) request.destroy();
      requests.clear(); grants.clear();
      server = undefined; engine = undefined;
      vi.restoreAllMocks();
      if (previousGate === undefined) delete process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS;
      else process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS = previousGate;
    }
  });

  afterAll(() => {
    if (root) fs.rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });

  async function boot(limits: Partial<HttpMcpServerOptions> = {}, extra: MCPEngineOptions & { maxOpenProjects?: number } = {}) {
    const engineOptions = { explicitProjectsOnly: true, watch: true, queryPool: false, ...extra };
    engine = new MCPEngine(engineOptions);
    server = await startHttpMcpServer({
      ...limits, engine, host: '127.0.0.1', port: 0,
      authority: {
        async authenticate(request) { return grants.get(request.headers.authorization ?? '') ?? null; },
        async resolveCall(request, launch) {
          const id = (request.params as { _meta?: Record<string, unknown> } | undefined)?._meta?.['fixture/conversation'];
          return id === 'one' || id === 'two' ? { workspace: launch.workspace, conversationId: id } : null;
        },
      },
    });
  }

  function send(body: unknown, headers: Headers, method = 'POST') {
    let request!: http.ClientRequest;
    const reply = new Promise<Reply>((resolve, reject) => {
      request = http.request(server!.url, { method, headers: {
        'content-type': 'application/json', accept: 'application/json, text/event-stream',
        'mcp-protocol-version': protocolVersion, ...headers,
      } }, (response) => {
        let text = '';
        response.setEncoding('utf8');
        response.on('data', (chunk: string) => { text += chunk; });
        response.once('error', reject);
        response.once('end', () => resolve({ status: response.statusCode ?? 0, headers: response.headers,
          body: text ? JSON.parse(text) as JsonRpcResponse : null }));
      });
      request.once('error', reject);
      request.once('close', () => requests.delete(request));
      request.setTimeout(10_000, () => request.destroy(new Error('HTTP lifecycle request timed out')));
      request.end(body === undefined ? undefined : JSON.stringify(body));
      requests.add(request);
    });
    // Disconnect tests observe rejection later, without an unhandled rejection race.
    void reply.catch(() => undefined);
    return { request, reply };
  }

  async function connect(token = tokenA): Promise<Headers> {
    const response = await send(initialize, { authorization: token }).reply;
    expect(response.status).toBe(200);
    expect(response.body?.error).toBeUndefined();
    expect(typeof response.headers['mcp-session-id']).toBe('string');
    return { authorization: token, 'mcp-session-id': response.headers['mcp-session-id'] as string };
  }

  function text(response: Reply): string {
    expect(response.status).toBe(200);
    expect(response.body?.error).toBeUndefined();
    const result = response.body?.result as ToolResult;
    expect(result.isError).toBeFalsy();
    return result.content.map((item) => item.text).join('\n');
  }

  function holdSync() {
    const entered = deferred(); const release = deferred(); const completed = deferred();
    releases.push(release.resolve);
    const sync = CodeGraph.prototype.sync;
    let held = false;
    vi.spyOn(CodeGraph.prototype, 'sync').mockImplementation(async function (this: InstanceType<typeof CodeGraph>, ...args) {
      if (!held && this.getProjectRoot() === projectA) {
        held = true; entered.resolve(); await release.promise;
      }
      return sync.apply(this, args);
    });
    const execute = engine!.getToolHandler().execute.bind(engine!.getToolHandler());
    vi.spyOn(engine!.getToolHandler(), 'execute').mockImplementation(async (...args) => {
      try { return await execute(...args); }
      finally { if (args[1].projectPath === projectA) completed.resolve(); }
    });
    return { entered: entered.promise, release: release.resolve, completed: completed.promise };
  }

  async function workerPool(holdFirst = true, failReaderClose = false) {
    const barrier = new SharedArrayBuffer(4);
    const entered = deferred(); const ready = deferred(); const closeFailed = deferred();
    const events: Array<{ type: string; root?: string; count?: number; pid?: number }> = [];
    const script = `
      const { parentPort, workerData } = require('worker_threads');
      const path = require('path');
      const CodeGraph = require(path.join(workerData.modules, 'index.js')).default;
      const { ToolHandler } = require(path.join(workerData.modules, 'mcp/tools.js'));
      const openSync = CodeGraph.openSync;
      let handles = 0;
      CodeGraph.openSync = function (...args) {
        const graph = openSync.apply(this, args);
        handles++;
        parentPort.postMessage({ type: 'fixture-open', root: graph.getProjectRoot(), count: handles });
        const close = graph.close.bind(graph);
        let closed = false;
        graph.close = function () {
          if (workerData.failReaderClose) {
            parentPort.postMessage({ type: 'fixture-close-failed', root: graph.getProjectRoot(), count: handles });
            throw new Error('fixture reader close failed');
          }
          const result = close();
          if (!closed) { closed = true; handles--; parentPort.postMessage({ type: 'fixture-close', root: graph.getProjectRoot(), count: handles }); }
          return result;
        };
        return graph;
      };
      const execute = ToolHandler.prototype.executeReadTool;
      let first = true;
      ToolHandler.prototype.executeReadTool = async function (...args) {
        if (first && workerData.holdFirst) {
          first = false;
          parentPort.postMessage({ type: 'fixture-entered', pid: process.pid });
          Atomics.wait(new Int32Array(workerData.barrier), 0, 0);
        }
        return execute.apply(this, args);
      };
      require(path.join(workerData.modules, 'mcp/query-worker.js'));
    `;
    const workers: Worker[] = [];
    function createWorker(): Worker {
      const first = workers.length === 0;
      const created = new Worker(script, { eval: true, workerData: {
        root: null, modules: path.resolve(__dirname, '../dist'), barrier,
        holdFirst: first && holdFirst, failReaderClose: first && failReaderClose,
      } });
      workers.push(created);
      return created;
    }
    const worker = createWorker();
    const exited = new Promise<void>((resolve) => worker.once('exit', () => resolve()));
    worker.on('message', (message: { type: string; ok?: boolean; root?: string; count?: number; pid?: number }) => {
      if (message.type === 'ready' && message.ok) ready.resolve();
      if (message.type.startsWith('fixture-')) events.push(message);
      if (message.type === 'fixture-entered') entered.resolve();
      if (message.type === 'fixture-close-failed') closeFailed.resolve();
    });
    const release = () => { Atomics.store(new Int32Array(barrier), 0, 1); Atomics.notify(new Int32Array(barrier), 0); };
    releases.push(release);
    let suppliedFirst = false;
    const pool = new QueryPool({ root: null, size: 1, maxQueuedJobs: 1, waitForCompletion: true,
      createWorker: () => { if (!suppliedFirst) { suppliedFirst = true; return worker; } return createWorker(); },
    });
    pools.push(pool);
    engine!.getToolHandler().setQueryPool(pool);
    await ready.promise;
    return { pool, worker, workers, entered: entered.promise, exited, release, events, closeFailed: closeFailed.promise };
  }

  function holdTermination(worker: Worker) {
    const requested = deferred(); const allowed = deferred();
    releases.push(allowed.resolve);
    const terminate = worker.terminate.bind(worker);
    const spy = vi.spyOn(worker, 'terminate').mockImplementation(async () => {
      requested.resolve();
      await allowed.promise;
      return terminate(); // Actual owned Worker termination and confirmed exit, never a process-name kill.
    });
    return { requested: requested.promise, allow: allowed.resolve, spy };
  }

  function observePoolCalls(pool: Pool, count: number) {
    const admitted = deferred(); const completed = deferred();
    const run = pool.run.bind(pool);
    const thresholds = new Map<number, ReturnType<typeof deferred>>();
    let calls = 0;
    vi.spyOn(pool, 'run').mockImplementation((...args) => {
      const result = run(...args);
      if (++calls === count) {
        admitted.resolve();
        void result.then(completed.resolve);
      }
      thresholds.get(calls)?.resolve();
      return result;
    });
    return { admitted: admitted.promise, completed: completed.promise,
      waitForCalls(target: number) {
        if (calls >= target) return Promise.resolve();
        const gate = deferred(); thresholds.set(target, gate); return gate.promise;
      },
    };
  }

  it('drains HTTP handlers after the last usable worker exhausts its crash budget', async () => {
    await boot(); const session = await connect();
    expect(text(await send(call(90), session).reply)).toContain('alphaSymbol');
    class FailedWorker extends EventEmitter implements PoolWorker {
      postMessage(_message: unknown): void {}
      terminate(): Promise<number> { return Promise.resolve(0); }
    }
    const workers: FailedWorker[] = [];
    const pool = new QueryPool({ root: null, size: 1, maxRetries: 0, waitForCompletion: true,
      createWorker: () => { const worker = new FailedWorker(); workers.push(worker); return worker; },
    });
    pools.push(pool); workers[0]!.emit('message', { type: 'ready', ok: true });
    engine!.getToolHandler().setQueryPool(pool);
    const observed = observePoolCalls(pool, 2);
    const first = send(call(1), session).reply;
    const second = send(call(2), session).reply;
    await observed.admitted;
    const shutdown = server!.stop();
    try {
      workers[0]!.emit('error', new Error('fixture exhausted worker'));
      for (let index = 1; index < 32 && pool.healthy; index++) {
        workers[index]!.emit('message', { type: 'ready', ok: false });
      }
      expect(pool.healthy).toBe(false);
      expect(pool.liveWorkers).toBe(0);
      const settled = await Promise.race([
        observed.completed.then(() => true), new Promise<false>((resolve) => setImmediate(() => resolve(false))),
      ]);
      expect(settled, 'HTTP stop cannot wait on an unserviceable worker queue').toBe(true);
      await shutdown;
    } finally {
      await pool.destroy(); // Releases a broken queue so the RED fixture itself cannot hang.
      await shutdown;
      await Promise.allSettled([first, second]);
    }
  });

  it('removes canceled queued work before it reaches an actual worker and immediately reuses the queue slot', async () => {
    await boot(); const session = await connect(); const worker = await workerPool();
    const observed = observePoolCalls(worker.pool, 2);
    const dispatch = vi.spyOn(worker.worker, 'postMessage');
    const first = send(call(1), session).reply;
    await worker.entered;
    const queued = send(call(2, 'alphaSymbol', 'two'), session).reply;
    await observed.admitted;
    expect((await send(cancel(2), session).reply).status).toBe(202);
    expect((await queued).status).toBe(204);
    const third = send(call(3), session).reply;
    await observed.waitForCalls(3);
    worker.release();
    expect(text(await first)).toContain('alphaSymbol');
    const recovered = text(await third);
    expect(recovered).toContain('alphaSymbol');
    expect(recovered).not.toMatch(/busy|capacity/i);
    const sent = dispatch.mock.calls.map(([message]) => message as { type?: string; id?: number });
    expect(sent.filter((message) => message.type === 'call')).toHaveLength(2);
  });

  it('does not publish canceled exploration history after its owned running worker is terminated', async () => {
    await boot({ maxPendingPerSession: 1 }); const session = await connect(); const worker = await workerPool();
    const observed = observePoolCalls(worker.pool, 1);
    const execute = vi.spyOn(engine!.getToolHandler(), 'execute');
    const message = call(1); message.params.name = 'codegraph_explore';
    const original = send(message, session).reply;
    await worker.entered;
    const retirement = holdTermination(worker.worker);
    const state = execute.mock.calls[0]![2]!;
    expect(state).toBeDefined();
    expect((await send(cancel(1), session).reply).status).toBe(202);
    expect((await original).status).toBe(204);
    expect(retirement.spy).toHaveBeenCalledTimes(1);
    expect(state.callCount(projectA)).toBe(0);
    expect(worker.events.find((event) => event.type === 'fixture-entered')?.pid).toBe(process.pid);
    const writer = fs.readFileSync(path.join(projectA, '.codegraph', 'writer.pid'), 'utf8');
    expect((await send(call(2), session).reply).status).toBe(429);
    expect(fs.readFileSync(path.join(projectA, '.codegraph', 'writer.pid'), 'utf8')).toBe(writer);
    retirement.allow(); await worker.exited; await observed.completed;
    // A fresh request creates an HTTP/event-loop barrier after the original
    // result has passed through the real ToolHandler's emission accounting.
    expect(text(await send(call(3), session).reply)).toContain('alphaSymbol');
    expect(state.callCount(projectA)).toBe(0);
    const fresh = call(4); fresh.params.name = 'codegraph_explore';
    expect(text(await send(fresh, session).reply)).toContain('sample.ts');
    expect(state.callCount(projectA)).toBe(1);
    await worker.pool.destroy(); await worker.exited;
    expect(worker.worker.threadId).toBe(-1);
  });

  it('keeps the whole engine reader footprint within one project across actual worker calls', async () => {
    await boot({}, { maxOpenProjects: 1 });
    const a = await connect(); const b = await connect(tokenB); const worker = await workerPool(false);
    expect(text(await send(call(1), a).reply)).toContain('alphaSymbol');
    expect(text(await send(call(2, 'betaSymbol'), b).reply)).toContain('betaSymbol');
    const opens = worker.events.filter((event) => event.type === 'fixture-open');
    expect(opens.map((event) => event.root)).toEqual([projectA, projectB]);
    expect(Math.max(...opens.map((event) => event.count!))).toBeLessThanOrEqual(1);
    const closedA = worker.events.findIndex((event) => event.type === 'fixture-close' && event.root === projectA);
    expect(closedA).toBeGreaterThanOrEqual(0);
    expect(closedA).toBeLessThan(worker.events.findIndex((event) => event.type === 'fixture-open' && event.root === projectB));
    await worker.pool.destroy(); await worker.exited;
    expect(worker.worker.threadId).toBe(-1);
  });

  it('retains the running worker reservation and writer lease through canceled shutdown until confirmed termination', async () => {
    await boot(); const session = await connect(); const worker = await workerPool();
    const original = send(call(1), session).reply;
    await worker.entered;
    const retirement = holdTermination(worker.worker);
    const originalThread = worker.worker.threadId;
    const writerPath = path.join(projectA, '.codegraph', 'writer.pid');
    const writer = fs.readFileSync(writerPath, 'utf8');
    expect((await send(cancel(1), session).reply).status).toBe(202);
    expect((await original).status).toBe(204);
    expect(retirement.spy).toHaveBeenCalledTimes(1);
    const shutdown = server!.stop();
    try {
      const stopped = await Promise.race([
        shutdown.then(() => true), new Promise<false>((resolve) => setImmediate(() => resolve(false))),
      ]);
      expect(stopped).toBe(false);
      expect(worker.worker.threadId).toBe(originalThread);
      expect(originalThread).toBeGreaterThan(0);
      expect(fs.readFileSync(writerPath, 'utf8')).toBe(writer);
      retirement.allow(); await worker.exited;
      await shutdown;
      expect(fs.existsSync(writerPath)).toBe(false);
      await worker.pool.destroy(); await worker.exited;
      expect(worker.worker.threadId).toBe(-1);
    } finally { retirement.allow(); worker.release(); await worker.pool.destroy(); await shutdown; }
  });

  it('terminates only the canceled request worker, holds project admission until exit, and replaces it safely', async () => {
    await boot({}, { maxOpenProjects: 1 }); const a = await connect(); const b = await connect(tokenB);
    const owned = await workerPool();
    const original = send(call(1), a).reply;
    await owned.entered;
    const retirement = holdTermination(owned.worker);
    const unrelated = await workerPool(false);
    const unrelatedTerminate = vi.spyOn(unrelated.worker, 'terminate');
    const unrelatedThread = unrelated.worker.threadId;
    const writerPath = path.join(projectA, '.codegraph', 'writer.pid');
    const writer = fs.readFileSync(writerPath, 'utf8');
    expect((await send(cancel(1), a).reply).status).toBe(202);
    expect((await original).status).toBe(204);
    expect(retirement.spy).toHaveBeenCalledTimes(1);
    expect(unrelatedTerminate).not.toHaveBeenCalled();
    expect((await send(call(1), a).reply).status).toBe(409);
    expect(text(await send(call(2, 'betaSymbol'), b).reply)).toMatch(/busy|capacity|retry/i);
    expect(fs.readFileSync(writerPath, 'utf8')).toBe(writer);
    retirement.allow(); await owned.exited;
    expect(owned.worker.threadId).toBe(-1);
    expect(unrelated.worker.threadId).toBe(unrelatedThread);
    expect(unrelatedTerminate).not.toHaveBeenCalled();
    engine!.getToolHandler().setQueryPool(owned.pool);
    expect(text(await send(call(3, 'betaSymbol'), b).reply)).toContain('betaSymbol');
    expect(owned.workers).toHaveLength(2);
    expect(owned.workers[1]).not.toBe(owned.worker);
    expect(owned.workers[1]!.threadId).toBeGreaterThan(0);
  });

  it('retires a worker whose reader cleanup fails before settling its result or releasing project admission', async () => {
    await boot({}, { maxOpenProjects: 1 }); const a = await connect(); const b = await connect(tokenB);
    const owned = await workerPool(false, true);
    const retirement = holdTermination(owned.worker);
    const original = send(call(1), a).reply;
    await owned.closeFailed;
    const retiredBeforeReply = await Promise.race([
      retirement.requested.then(() => true), original.then(() => false),
    ]);
    expect(retiredBeforeReply, 'a cleanup error must not release an open reader through normal completion').toBe(true);
    expect(retirement.spy).toHaveBeenCalledTimes(1);
    expect(owned.worker.threadId).toBeGreaterThan(0);
    expect((await send(call(1), a).reply).status).toBe(409);
    expect(text(await send(call(2, 'betaSymbol'), b).reply)).toMatch(/busy|capacity|retry/i);
    retirement.allow(); await owned.exited;
    const failure = await original;
    expect(failure.status).toBe(200);
    expect((failure.body?.result as ToolResult).isError).toBe(true);
    expect(JSON.stringify(failure.body)).toMatch(/reader|cleanup|worker/i);
    expect(owned.worker.threadId).toBe(-1);
    expect(text(await send(call(3, 'betaSymbol'), b).reply)).toContain('betaSymbol');
    expect(owned.workers).toHaveLength(2);
  });

  it('rejects duplicate in-flight ID zero without stealing its response', async () => {
    await boot(); const session = await connect(); const gate = holdSync();
    const original = send(call(0), session).reply;
    await gate.entered;
    expect((await send(call(0), session).reply).status).toBe(409);
    gate.release();
    const result = await original;
    expect(result.body?.id).toBe(0);
    expect(text(result)).toContain('alphaSymbol');
  });

  it.each([undefined, 1])('cancels ID zero even at tool capacity and retains its reservation until actual completion (maxRequests=%s)', async (maxRequests) => {
    await boot({ maxPendingPerSession: 1, maxRequests }); const session = await connect(); const gate = holdSync();
    const original = send(call(0), session).reply;
    await gate.entered;
    expect((await send(cancel(0), session).reply).status).toBe(202);
    const canceled = await original;
    expect(canceled.status).toBe(204);
    expect(canceled.body).toBeNull();
    expect((await send(call(0), session).reply).status).toBe(409);
    expect((await send(call(1), session).reply).status).toBe(429);
    gate.release(); await gate.completed;
    expect(text(await send(call(2), session).reply)).toContain('alphaSymbol');
  });

  it('cannot cancel another transport request or another authenticated launch', async () => {
    await boot(); const a = await connect(); const b = await connect(); const gate = holdSync();
    const original = send(call(0), a).reply;
    await gate.entered;
    expect((await send(cancel(0), { ...a, authorization: tokenB }).reply).status).toBe(403);
    // Same launch, different transport: request ID zero is unknown here.
    expect((await send(cancel(0), b).reply).status).toBe(202);
    expect((await send(cancel(999), a).reply).status).toBe(202);
    gate.release();
    expect(text(await original)).toContain('alphaSymbol');
  });

  it('does not treat socket disconnect as cancellation or release occupied request capacity', async () => {
    await boot({ maxPendingPerSession: 1 }); const session = await connect(); const gate = holdSync();
    const original = send(call(0), session);
    await gate.entered;
    original.request.destroy(new Error('fixture disconnect'));
    await expect(original.reply).rejects.toThrow('fixture disconnect');
    expect((await send(call(0), session).reply).status).toBe(409);
    expect((await send(call(1), session).reply).status).toBe(429);
    gate.release(); await gate.completed;
    expect(text(await send(call(2), session).reply)).toContain('alphaSymbol');
  });

  it('counts a deleted transport as draining until its admitted job finishes', async () => {
    await boot({ maxSessions: 1 }); const session = await connect(); const gate = holdSync();
    const original = send(call(0), session).reply;
    await gate.entered;
    expect((await send(undefined, session, 'DELETE').reply).status).toBe(204);
    expect((await original).status).toBe(503);
    expect((await send(call(1), session).reply).status).toBe(404);
    expect((await send(initialize, { authorization: tokenA }).reply).status).toBe(429);
    gate.release(); await gate.completed;
    const replacement = await connect();
    expect(text(await send(call(2), replacement).reply)).toContain('alphaSymbol');
  });

  it('bounds sessions and recovers capacity after deleting an idle transport', async () => {
    await boot({ maxSessions: 1 }); const session = await connect();
    expect((await send(initialize, { authorization: tokenA }).reply).status).toBe(429);
    expect((await send(undefined, session, 'DELETE').reply).status).toBe(204);
    const replacement = await connect();
    expect(replacement['mcp-session-id']).not.toBe(session['mcp-session-id']);
  });

  it('rejects oversized bodies without retiring a valid session', async () => {
    await boot({ maxBodyBytes: 256 }); const session = await connect();
    const response = await send(call(1, 'x'.repeat(512)), session).reply;
    expect(response.status).toBe(413);
    expect(response.body?.result).toBeUndefined();
    expect(text(await send(call(2), session).reply)).toContain('alphaSymbol');
  });

  it('bounds logical conversations without replacing an existing conversation identity', async () => {
    await boot({ maxConversations: 1 }); const session = await connect();
    const execute = vi.spyOn(engine!.getToolHandler(), 'execute');
    expect(text(await send(call(1), session).reply)).toContain('alphaSymbol');
    const full = await send(call(2, 'alphaSymbol', 'two'), session).reply;
    expect(full.body?.result).toBeUndefined();
    expect(full.body?.error).toBeDefined();
    expect(text(await send(call(3), session).reply)).toContain('alphaSymbol');
    expect(execute).toHaveBeenCalledTimes(2);
    expect(execute.mock.calls[0]![2]).toBeDefined();
    expect(execute.mock.calls[0]![2]).toBe(execute.mock.calls[1]![2]);
  });

  it('refuses another project before opening its database when the project limit is occupied', async () => {
    await boot({}, { maxOpenProjects: 1 });
    const a = await connect(); const b = await connect(tokenB); const gate = holdSync();
    const opens = vi.spyOn(CodeGraph, 'openSync');
    const first = send(call(0), a).reply;
    await gate.entered;
    const full = await send(call(1, 'betaSymbol'), b).reply;
    // Admission is expected overload guidance, not a tool failure or an index error.
    expect(text(full)).toMatch(/busy|capacity|retry/i);
    expect(opens.mock.calls.some(([directory]) => fs.realpathSync(directory) === projectB)).toBe(false);
    gate.release(); expect(text(await first)).toContain('alphaSymbol');
    // An idle first project may now be evicted; the next call must recover.
    const retry = await send(call(2, 'betaSymbol'), b).reply;
    expect(text(retry)).toContain('betaSymbol');
    expect(opens.mock.calls.some(([directory]) => fs.realpathSync(directory) === projectB)).toBe(true);
  });

  async function nestedWorktree() {
    const parent = path.join(root, `nested-parent-${randomUUID()}`);
    const linked = path.join(parent, 'child');
    fs.mkdirSync(parent);
    const git = (...args: string[]) => execFileSync('git', args, {
      cwd: parent, encoding: 'utf8', windowsHide: true, timeout: 10_000, stdio: ['ignore', 'pipe', 'pipe'],
    });
    git('init', '-q');
    fs.writeFileSync(path.join(parent, '.gitignore'), '.codegraph/\nchild/\n');
    fs.writeFileSync(path.join(parent, 'private.ts'), 'export function ancestorPrivateSymbol() { return 7; }\n');
    git('add', '.gitignore', 'private.ts');
    git('-c', 'user.name=CodeGraph Test', '-c', 'user.email=test@codegraph.invalid',
      '-c', 'commit.gpgsign=false', 'commit', '-q', '-m', 'nested HTTP fixture');
    git('worktree', 'add', '--detach', linked, 'HEAD');
    expect(fs.statSync(path.join(linked, '.git')).isFile()).toBe(true);
    const actualRoot = execFileSync('git', ['rev-parse', '--show-toplevel'], { cwd: linked, encoding: 'utf8', windowsHide: true }).trim();
    expect(fs.realpathSync(actualRoot)).toBe(fs.realpathSync(linked));
    const graph = await CodeGraph.init(parent, { config: { include: ['**/*.ts'], exclude: ['child/**'] } });
    try { await graph.indexAll(); } finally { graph.close(); }
    expect(fs.existsSync(path.join(linked, '.codegraph'))).toBe(false);
    return linked;
  }

  it('refuses an unindexed nested linked worktree without relying on the strict-worktree environment flag', async () => {
    const linked = await nestedWorktree();
    const previousStrict = process.env.CODEGRAPH_STRICT_WORKTREE;
    delete process.env.CODEGRAPH_STRICT_WORKTREE;
    try {
      await boot();
      const token = `Bearer ${randomUUID()}`;
      grants.set(token, { workspace: linked, provider: 'codex', launchId: randomUUID(), providerHome: path.join(root, 'codex') });
      const session = await connect(token);
      const result = await send(call(1, 'ancestorPrivateSymbol'), session).reply;
      const body = result.body?.result as ToolResult;
      expect(result.status).toBe(200);
      expect(result.body?.error).toBeUndefined();
      const message = body.content.map((item) => item.text).join('\n');
      expect(message).toMatch(/isn't indexed|not indexed|worktree.*index|index.*worktree/i);
      expect(message).not.toContain('ancestorPrivateSymbol');
      expect(engine!.hasDefaultCodeGraph()).toBe(false);
      expect(process.env.CODEGRAPH_STRICT_WORKTREE).toBeUndefined();
    } finally {
      if (previousStrict === undefined) delete process.env.CODEGRAPH_STRICT_WORKTREE;
      else process.env.CODEGRAPH_STRICT_WORKTREE = previousStrict;
    }
  });

  // POSIX permits renaming an open SQLite directory; Windows may reject it.
  // This qualification is explicit because the test exercises that filesystem
  // transition, while the non-racing nested-worktree refusal above is portable.
  it.skipIf(process.platform === 'win32')('retains strict worktree routing in the actual worker when its own index disappears after main-thread admission (POSIX)', async () => {
    const linked = await nestedWorktree();
    const graph = await CodeGraph.init(linked, { config: { include: ['**/*.ts'], exclude: [] } });
    try { await graph.indexAll(); } finally { graph.close(); }
    const previousStrict = process.env.CODEGRAPH_STRICT_WORKTREE;
    delete process.env.CODEGRAPH_STRICT_WORKTREE;
    let release: (() => void) | undefined;
    try {
      await boot();
      const token = `Bearer ${randomUUID()}`;
      grants.set(token, { workspace: linked, provider: 'codex', launchId: randomUUID(), providerHome: path.join(root, 'codex') });
      const session = await connect(token);
      const worker = await workerPool(); release = worker.release;
      const pending = send(call(1, 'ancestorPrivateSymbol'), session).reply;
      await worker.entered;
      fs.renameSync(path.join(linked, '.codegraph'), path.join(linked, '.codegraph-retired-fixture'));
      worker.release();
      const response = await pending;
      expect(response.status).toBe(200);
      expect(response.body?.error).toBeUndefined();
      const message = (response.body?.result as ToolResult).content.map((item) => item.text).join('\n');
      expect(message).toMatch(/isn't indexed|not indexed|worktree.*index|index.*worktree/i);
      expect(message).not.toContain('ancestorPrivateSymbol');
      expect(process.env.CODEGRAPH_STRICT_WORKTREE).toBeUndefined();
    } finally {
      release?.();
      if (previousStrict === undefined) delete process.env.CODEGRAPH_STRICT_WORKTREE;
      else process.env.CODEGRAPH_STRICT_WORKTREE = previousStrict;
    }
  });

  it('closes its listener and partial-header sockets even when engine.stop rejects, while surfacing the error', async () => {
    await boot(); const session = await connect();
    expect(text(await send(call(1), session).reply)).toContain('alphaSymbol');
    const endpoint = new URL(server!.url);
    const partial = net.createConnection({ host: endpoint.hostname, port: Number(endpoint.port) });
    const socketClosed = new Promise<void>((resolve) => partial.once('close', () => resolve()));
    // Peer errors during forced server cleanup are expected; the close event is the evidence.
    partial.on('error', () => undefined);
    await new Promise<void>((resolve, reject) => {
      partial.once('connect', resolve); partial.once('error', reject);
    });
    await new Promise<void>((resolve, reject) => {
      partial.write(`POST /mcp HTTP/1.1\r\nHost: ${endpoint.host}\r\n`, (error) => error ? reject(error) : resolve());
    });
    const clientPort = partial.localPort;
    const destroy = net.Socket.prototype.destroy;
    let destroyedOwnedSocket = false;
    vi.spyOn(net.Socket.prototype, 'destroy').mockImplementation(function (this: net.Socket, error?: Error) {
      if (this.localPort === Number(endpoint.port) && this.remotePort === clientPort) destroyedOwnedSocket = true;
      return destroy.call(this, error);
    });
    const failure = new Error('fixture engine shutdown failure');
    const stop = vi.spyOn(engine!, 'stop').mockRejectedValue(failure);
    try {
      await expect(server!.stop()).rejects.toBe(failure);
      // Observe actual owned-socket destruction at the public Socket boundary;
      // do not wait for Node's header timeout to hide incomplete shutdown.
      expect(destroyedOwnedSocket).toBe(true);
      await socketClosed;
      await expect(send(initialize, { authorization: tokenA }).reply).rejects.toMatchObject({ code: 'ECONNREFUSED' });
    } finally {
      partial.destroy();
      await socketClosed;
      stop.mockRestore();
      await engine!.stop(); // Never leave real graph/watcher cleanup replaced by the failure fixture.
      server = undefined; // Its stop promise intentionally retains the original surfaced failure.
    }
  });
});
