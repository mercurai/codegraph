/**
 * Opt-in exact-worktree discovery, before daemon selection or graph opening.
 *
 * Real Git worktrees and initialized SQLite fixtures exercise existing public
 * discovery APIs. Only the startup transport boundaries are stubbed: no daemon
 * is launched, contacted, replaced, or stopped by this suite.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { execFileSync, spawn } from 'child_process';
import { createInterface } from 'readline';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { pathToFileURL } from 'url';
import CodeGraph from '../src/index';
import { findNearestCodeGraphRoot, resolveServerRoot } from '../src/directory';
import { gitWorktreeRoot } from '../src/sync/worktree';
import type { MCPEngine } from '../src/mcp/engine';
import { MCPServer } from '../src/mcp/index';
import { ToolHandler } from '../src/mcp/tools';

const STRICT_ENV = 'CODEGRAPH_STRICT_WORKTREE';

function git(cwd: string, ...args: string[]): void {
  execFileSync('git', args, {
    cwd,
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
    timeout: 10_000,
  });
}

function initializeRepository(root: string): void {
  fs.mkdirSync(root, { recursive: true });
  git(root, 'init', '-q');
  git(root, '-c', 'user.name=CodeGraph Test', '-c', 'user.email=test@codegraph.invalid',
    '-c', 'commit.gpgsign=false', 'commit', '--allow-empty', '-q', '-m', 'fixture');
}

function initializeIndex(root: string): void {
  const graph = CodeGraph.initSync(root);
  graph.close();
}

/** Count actual Git proofs in built modules, isolated from the test runner. */
function probeProofLifetime(cwd: string, body: string): unknown {
  const script = `
    const cp = require('child_process');
    const fs = require('fs');
    const path = require('path');
    const original = cp.execFileSync;
    let gitCalls = 0;
    cp.execFileSync = function(file, ...args) {
      if (path.basename(file).replace(/\\.exe$/i, '') === 'git') gitCalls++;
      return original.call(this, file, ...args);
    };
    const { withWorktreeProofScope } = require(${JSON.stringify(path.resolve(__dirname, '../dist/sync/worktree.js'))});
    const { resolveServerRoot } = require(${JSON.stringify(path.resolve(__dirname, '../dist/directory.js'))});
    (async () => { ${body} })().then(result => process.stdout.write(JSON.stringify(result)))
      .catch(error => { console.error(error); process.exitCode = 1; });
  `;
  return JSON.parse(execFileSync(process.execPath, ['-e', script], {
    cwd, env: process.env, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true, timeout: 10_000,
  }));
}

/** Startup boundaries only; discovery and MCPServer.start remain real. */
function observeStartup(server: MCPServer) {
  const boundaries = server as unknown as {
    runProxyWithLocalHandshake(root: string): Promise<void>;
    startDirect(reason: string): Promise<void>;
  };
  return {
    proxy: vi.spyOn(boundaries, 'runProxyWithLocalHandshake').mockResolvedValue(undefined),
    direct: vi.spyOn(boundaries, 'startDirect').mockResolvedValue(undefined),
  };
}

/** Exercise the real stdio proxy without opening a socket or spawning a daemon. */
async function callAttachedProxy(
  root: string,
  projectPath?: string,
  initializeParams: Record<string, unknown> = {},
): Promise<{
  forwarded: number;
  forwardedProjectPath?: string;
  response: { result?: { content?: Array<{ text?: string }> }; error?: { message: string } };
}> {
  const modulePath = path.resolve(__dirname, '../dist/mcp/proxy.js');
  const child = spawn(process.execPath, ['-e', `
    const { EventEmitter } = require('events');
    const { writeSync } = require('fs');
    const { runLocalHandshakeProxy } = require(${JSON.stringify(modulePath)});
    const say = value => writeSync(1, JSON.stringify(value) + '\\n');
    const socket = new EventEmitter();
    socket.destroyed = false;
    socket.destroy = () => { socket.destroyed = true; };
    socket.setEncoding = () => {
      // attachDaemon finishes registering its listeners before this runs.
      setImmediate(() => say({ proxyAttached: true }));
      return socket;
    };
    socket.write = line => {
      const request = JSON.parse(line);
      if (request.method === 'tools/call') say({
        observedDaemonCall: true, projectPath: request.params?.arguments?.projectPath,
      });
      socket.emit('data', JSON.stringify({
        jsonrpc: '2.0', id: request.id,
        result: { content: [{ type: 'text', text: 'permissive daemon result' }] },
      }) + '\\n');
      return true;
    };
    void runLocalHandshakeProxy({
      root: ${JSON.stringify(root)},
      getDaemonSocket: async () => socket,
      makeEngine: () => { throw new Error('Unexpected fallback engine'); },
    });
  `], {
    cwd: root,
    env: {
      ...process.env,
      CODEGRAPH_NO_WATCHDOG: '1', CODEGRAPH_PPID_POLL_MS: '0',
      CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0', CODEGRAPH_DAEMON_RETRY_MS: '0',
    },
    stdio: ['pipe', 'pipe', 'pipe'],
    windowsHide: true,
  });
  let forwarded = 0;
  let forwardedProjectPath: string | undefined;
  let response: Awaited<ReturnType<typeof callAttachedProxy>>['response'] | undefined;
  let stderr = '';
  child.stderr.on('data', (chunk) => { stderr = (stderr + chunk).slice(-4_000); });
  child.stdin.on('error', () => { /* The exit assertion reports a premature close. */ });
  const lines = createInterface({ input: child.stdout });
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        child.kill('SIGKILL'); // Only the fixture process created above.
      }, 10_000);
      child.once('error', (error) => { clearTimeout(timer); reject(error); });
      child.once('close', (code) => {
        clearTimeout(timer);
        if (code === 0 && response) resolve();
        else reject(new Error(`Proxy fixture exited ${code} without a response: ${stderr}`));
      });
      lines.on('line', (line) => {
        const message = JSON.parse(line);
        if (message.proxyAttached) {
          child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: initializeParams }) + '\n');
          child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'tools/call',
            params: { name: 'codegraph_status', arguments: { projectPath } } }) + '\n');
        } else if (message.observedDaemonCall) {
          forwarded++;
          forwardedProjectPath = message.projectPath;
        } else if (message.id === 2) {
          response = message;
          child.stdin.end();
        }
      });
    });
    return { forwarded, forwardedProjectPath, response: response! };
  } finally {
    lines.close();
    child.stdin.destroy();
  }
}

describe('opt-in strict worktree routing', () => {
  let fixture: string;
  let mainRepo: string;
  let linkedWorktree: string;
  const engines: MCPEngine[] = [];

  beforeEach(() => {
    vi.stubEnv(STRICT_ENV, '1');
    vi.stubEnv('CODEGRAPH_NO_DAEMON', '0');
    vi.stubEnv('CODEGRAPH_DAEMON_INTERNAL', '0');
    vi.stubEnv('CODEGRAPH_NO_UPDATE_CHECK', '1');
    vi.stubEnv('CODEGRAPH_TELEMETRY', '0');
    fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-strict-worktree-')));
    mainRepo = path.join(fixture, 'main');
    initializeRepository(mainRepo);
    initializeIndex(mainRepo);
    linkedWorktree = path.join(mainRepo, 'nested-worktree');
    git(mainRepo, 'worktree', 'add', '--detach', '-q', linkedWorktree);
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    await Promise.all(engines.splice(0).map((engine) => engine.stop()));
    fs.rmSync(fixture, { recursive: true, force: true });
  });

  it('resolves a subdirectory to its own indexed linked worktree', () => {
    initializeIndex(linkedWorktree);
    const source = path.join(linkedWorktree, 'src');
    fs.mkdirSync(source);

    expect(findNearestCodeGraphRoot(source)).toBe(linkedWorktree);
    expect(resolveServerRoot(source).root).toBe(linkedWorktree);
  });

  it('refuses a parent index when the nested linked worktree has no index', () => {
    expect(gitWorktreeRoot(linkedWorktree)).toBe(linkedWorktree);
    expect(findNearestCodeGraphRoot(linkedWorktree)).toBeNull();
    expect(resolveServerRoot(linkedWorktree).root).toBeNull();
  });

  it.each([undefined, '0'])('retains upstream discovery when strict mode is %s', (setting) => {
    vi.stubEnv(STRICT_ENV, setting);

    expect(findNearestCodeGraphRoot(linkedWorktree)).toBe(mainRepo);
    expect(resolveServerRoot(linkedWorktree).root).toBe(mainRepo);
  });

  it('does not mistake an independent nested repository for a linked worktree', () => {
    const independent = path.join(mainRepo, 'independent-repository');
    initializeRepository(independent);

    // Existing parent indexes can cover nested independent repos/submodules.
    expect(findNearestCodeGraphRoot(independent)).toBe(mainRepo);
    expect(resolveServerRoot(independent).root).toBe(mainRepo);
  });

  it.each(['explicit', 'workspace'])('preserves an excluded nested repository requested via %s through proxy forwarding', async (route) => {
    const independent = path.join(mainRepo, 'excluded-repository');
    initializeRepository(independent);
    fs.writeFileSync(path.join(mainRepo, '.gitignore'), '/excluded-repository/\n');
    fs.writeFileSync(path.join(independent, 'nested.ts'), 'export const nestedOnly = true;\n');
    const graph = CodeGraph.openSync(mainRepo, { readOnly: true });
    const handler = new ToolHandler(graph);
    try {
      expect(graph.hasFilesUnder('excluded-repository')).toBe(false);
      // Different repositories may legitimately use a parent index, so the
      // proxy must retain this path for the handler's indexed-coverage guard.
      expect(findNearestCodeGraphRoot(independent)).toBe(mainRepo);
      const direct = await handler.execute('codegraph_status', { projectPath: independent });
      expect(direct.content.map((item) => item.text ?? '').join('\n')).toMatch(/holds none of its files/);

      const routed = route === 'explicit'
        ? await callAttachedProxy(mainRepo, independent)
        : await callAttachedProxy(mainRepo, undefined, { rootUri: pathToFileURL(independent).href });
      expect(routed.forwarded).toBe(1);
      expect(typeof routed.forwardedProjectPath).toBe('string');
      // Feed the actual observed wire argument into the real handler: rewriting
      // it to the parent would return that parent's successful status instead.
      const result = await handler.execute('codegraph_status', { projectPath: routed.forwardedProjectPath });
      const text = result.content.map((item) => item.text ?? '').join('\n');
      expect(text).toMatch(/holds none of its files/);
      expect(text).not.toContain('CodeGraph Status');
      expect(routed.forwardedProjectPath).toBe(independent);
    } finally {
      await handler.closeAll();
      graph.close();
    }
  }, 15_000);

  it('fails closed when Git cannot establish the linked worktree identity', () => {
    const emptyPath = path.join(fixture, 'no-executables');
    fs.mkdirSync(emptyPath);
    vi.stubEnv('PATH', emptyPath);

    expect(gitWorktreeRoot(linkedWorktree)).toBeNull();
    expect(findNearestCodeGraphRoot(linkedWorktree)).toBeNull();
    expect(resolveServerRoot(linkedWorktree).root).toBeNull();
  });

  it('does not let inherited Git directory overrides disguise a borrowed worktree index', () => {
    vi.stubEnv('GIT_DIR', path.join(mainRepo, '.git'));
    vi.stubEnv('GIT_WORK_TREE', linkedWorktree);
    // Prove the contaminating environment affects ordinary Git discovery:
    // both paths now report the linked worktree despite their physical roots.
    expect(gitWorktreeRoot(mainRepo)).toBe(linkedWorktree);
    expect(gitWorktreeRoot(linkedWorktree)).toBe(linkedWorktree);

    expect(findNearestCodeGraphRoot(linkedWorktree)).toBeNull();
    expect(resolveServerRoot(linkedWorktree).root).toBeNull();
  });

  it('proves the same Git root once per resolution without caching across requests', () => {
    const modulePath = path.resolve(__dirname, '../dist/directory.js');
    // Count real Git subprocesses, not elapsed time. Instrument only this
    // fixture child; every proof still executes Git against the real repo.
    const output = execFileSync(process.execPath, ['-e', `
      const cp = require('child_process');
      const original = cp.execFileSync;
      let gitCalls = 0;
      cp.execFileSync = function(file, ...args) {
        if (require('path').basename(file).replace(/\\.exe$/i, '') === 'git') gitCalls++;
        return original.call(this, file, ...args);
      };
      const { resolveServerRoot } = require(${JSON.stringify(modulePath)});
      const resolutions = [];
      for (let request = 0; request < 2; request++) {
        gitCalls = 0;
        const result = resolveServerRoot(${JSON.stringify(mainRepo)});
        resolutions.push({ root: result.root, gitCalls });
      }
      process.stdout.write(JSON.stringify(resolutions));
    `], {
      cwd: mainRepo,
      env: process.env,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
      timeout: 10_000,
    });

    expect(JSON.parse(output)).toEqual([
      { root: mainRepo, gitCalls: 1 },
      { root: mainRepo, gitCalls: 1 },
    ]);
  });

  it.each([false, true])('keeps one fresh physical Git proof per complete ToolHandler request (concurrent=%s)', (concurrent) => {
    initializeIndex(linkedWorktree);
    const graphModule = path.resolve(__dirname, '../dist/index.js');
    const toolsModule = path.resolve(__dirname, '../dist/mcp/tools.js');
    const output = execFileSync(process.execPath, ['-e', `
      const cp = require('child_process');
      const original = cp.execFileSync;
      const { AsyncLocalStorage } = require('async_hooks');
      const labels = new AsyncLocalStorage();
      const counts = new Map();
      cp.execFileSync = function(file, ...args) {
        const label = labels.getStore();
        if (label !== undefined && require('path').basename(file).replace(/\\.exe$/i, '') === 'git') {
          counts.set(label, (counts.get(label) || 0) + 1);
        }
        return original.call(this, file, ...args);
      };
      const CodeGraph = require(${JSON.stringify(graphModule)}).default;
      const { ToolHandler } = require(${JSON.stringify(toolsModule)});
      const graph = CodeGraph.openSync(${JSON.stringify(mainRepo)}, { readOnly: true });
      const handler = new ToolHandler(graph);
      (async () => {
        let observations;
        try {
          const projects = ${JSON.stringify([mainRepo, mainRepo, linkedWorktree])};
          const call = request => labels.run(request, async () => {
            const result = await handler.execute('codegraph_status', { projectPath: projects[request] });
            const text = result.content.map(item => item.text || '').join('\\n');
            return { gitCalls: counts.get(request), validStatus: text.includes('CodeGraph Status') };
          });
          if (${concurrent}) {
            observations = await Promise.all(projects.map((_, request) => call(request)));
          } else {
            observations = [];
            for (let request = 0; request < projects.length; request++) observations.push(await call(request));
          }
        } finally {
          await handler.closeAll();
          graph.close();
        }
        process.stdout.write(JSON.stringify(observations));
      })().catch(error => { console.error(error); process.exitCode = 1; });
    `], {
      cwd: mainRepo,
      env: process.env,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
      timeout: 10_000,
    });

    expect(JSON.parse(output)).toEqual([
      { gitCalls: 1, validStatus: true },
      { gitCalls: 1, validStatus: true },
      { gitCalls: 1, validStatus: true },
    ]);
  });

  it.each([false, true])('discards proofs inherited by deferred work after scope completion (rejected=%s)', (rejected) => {
    const observation = probeProofLifetime(mainRepo, `
      const root = ${JSON.stringify(mainRepo)};
      let release;
      const gate = new Promise(resolve => { release = resolve; });
      let deferred;
      let didReject = false;
      try {
        await withWorktreeProofScope(async () => {
          resolveServerRoot(root);
          deferred = gate.then(() => [resolveServerRoot(root).root, resolveServerRoot(root).root]);
          if (${rejected}) throw new Error('intentional request rejection');
        });
      } catch { didReject = true; }
      const beforeDeferred = gitCalls;
      release();
      const roots = await deferred;
      return { didReject, beforeDeferred, afterDeferred: gitCalls, roots };
    `);
    expect(observation).toEqual({
      didReject: rejected, beforeDeferred: 1, afterDeferred: 3, roots: [mainRepo, mainRepo],
    });
  });

  it('rechecks physical identity after a worktree Git link changes between requests', () => {
    initializeIndex(linkedWorktree);
    const observation = probeProofLifetime(linkedWorktree, `
      const root = ${JSON.stringify(linkedWorktree)};
      const gitFile = path.join(root, '.git');
      const original = fs.readFileSync(gitFile);
      const first = await withWorktreeProofScope(async () => resolveServerRoot(root).root);
      let second;
      try {
        fs.writeFileSync(gitFile, 'gitdir: ' + path.join(root, 'missing-git-directory') + '\\n');
        second = await withWorktreeProofScope(async () => resolveServerRoot(root).root);
      } finally { fs.writeFileSync(gitFile, original); }
      return { first, second, gitCalls };
    `);
    expect(observation).toEqual({ first: linkedWorktree, second: null, gitCalls: 2 });
  });

  it('does not replace a refused parent route with an indexed child', () => {
    const child = path.join(linkedWorktree, 'indexed-child');
    fs.mkdirSync(child);
    initializeIndex(child);

    const resolution = resolveServerRoot(linkedWorktree);
    expect(resolution.root).toBeNull();
    expect(resolution.viaSubScan).toBe(false);
  });

  it.each(['1', '0'])('applies strict mode %s when the only indexed child is another linked worktree', (setting) => {
    vi.stubEnv(STRICT_ENV, setting);
    // Neither this worktree nor any ancestor has an index. The only index a
    // workspace down-scan can see belongs to a different branch/worktree.
    const unindexed = path.join(fixture, 'unindexed-worktree');
    git(mainRepo, 'worktree', 'add', '--detach', '-q', unindexed);
    const indexedChild = path.join(unindexed, 'another-worktree');
    git(mainRepo, 'worktree', 'add', '--detach', '-q', indexedChild);
    initializeIndex(indexedChild);
    expect(findNearestCodeGraphRoot(unindexed)).toBeNull();
    expect(gitWorktreeRoot(unindexed)).toBe(unindexed);
    expect(gitWorktreeRoot(indexedChild)).toBe(indexedChild);

    const resolution = resolveServerRoot(unindexed);
    expect(resolution.root).toBe(setting === '1' ? null : indexedChild);
    expect(resolution.viaSubScan).toBe(setting !== '1');
  });

  it('allows an indexed child project within the same linked worktree', async () => {
    const unindexed = path.join(fixture, 'unindexed-worktree');
    git(mainRepo, 'worktree', 'add', '--detach', '-q', unindexed);
    const indexedChild = path.join(unindexed, 'package');
    fs.mkdirSync(indexedChild);
    initializeIndex(indexedChild);
    expect(findNearestCodeGraphRoot(unindexed)).toBeNull();
    expect(gitWorktreeRoot(indexedChild)).toBe(unindexed);

    const resolution = resolveServerRoot(unindexed);
    expect(resolution.root).toBe(indexedChild);
    expect(resolution.viaSubScan).toBe(true);
    const routed = await callAttachedProxy(mainRepo, undefined, { rootUri: pathToFileURL(unindexed).href });
    expect(routed.forwarded).toBe(1);
    expect(routed.forwardedProjectPath).toBe(indexedChild);
  }, 15_000);

  it('keeps direct-engine fallback from opening the rejected parent graph', async () => {
    // The engine lazy-loads ../index with require(). Use the built module just
    // as the production proxy does; source-module loading under Vitest cannot
    // resolve that require and would make an opening failure look like safety.
    const { MCPEngine: BuiltEngine } = require('../dist/mcp/engine') as typeof import('../src/mcp/engine');
    const indexedWorktree = path.join(mainRepo, 'indexed-worktree');
    git(mainRepo, 'worktree', 'add', '--detach', '-q', indexedWorktree);
    initializeIndex(indexedWorktree);
    const valid = new BuiltEngine({ readOnly: true, watch: false, queryPool: false });
    const engine = new BuiltEngine({ readOnly: true, watch: false, queryPool: false });
    engines.push(valid, engine);

    await valid.ensureInitialized(indexedWorktree);
    valid.retryInitializeSync(indexedWorktree);
    expect(valid.hasDefaultCodeGraph()).toBe(true);

    await engine.ensureInitialized(linkedWorktree);
    engine.retryInitializeSync(linkedWorktree);

    expect(engine.hasDefaultCodeGraph()).toBe(false);
  });

  it('refuses explicit projectPath lookups even when a parent graph is already cached', async () => {
    const graph = CodeGraph.openSync(mainRepo, { readOnly: true });
    const handler = new ToolHandler(graph);
    try {
      const result = await handler.execute('codegraph_status', { projectPath: linkedWorktree });
      const text = result.content.map((item) => item.text ?? '').join('\n');

      expect(text).not.toContain('CodeGraph Status');
      expect(text).toMatch(/worktree|not indexed|isn't indexed|No CodeGraph project/i);
    } finally {
      await handler.closeAll();
      graph.close();
    }
  });

  it('refuses the route before a permissive existing daemon can be contacted', async () => {
    const server = new MCPServer(linkedWorktree);
    const startup = observeStartup(server);

    await server.start();

    // A daemon's version/policy is irrelevant: the rejected root never reaches
    // connect-or-spawn, which could attach to or replace an existing daemon.
    expect(startup.proxy).not.toHaveBeenCalled();
    expect(startup.direct).toHaveBeenCalledOnce();
  });

  it('still attaches to the correctly indexed worktree in strict mode', async () => {
    initializeIndex(linkedWorktree);
    const server = new MCPServer(linkedWorktree);
    const startup = observeStartup(server);

    await server.start();

    expect(startup.proxy).toHaveBeenCalledTimes(1);
    expect(startup.proxy).toHaveBeenCalledWith(linkedWorktree);
    expect(startup.direct).not.toHaveBeenCalled();
  });

  it.each(['1', '0'])('enforces strict mode %s on explicit paths after attaching to a permissive daemon', async (setting) => {
    vi.stubEnv(STRICT_ENV, setting);
    // Startup is valid in both modes; only this later tool argument crosses
    // the nested linked-worktree boundary. The daemon intentionally accepts it.
    expect(resolveServerRoot(mainRepo).root).toBe(mainRepo);
    const { forwarded, response } = await callAttachedProxy(mainRepo, linkedWorktree);
    const text = response.result?.content?.map((item) => item.text ?? '').join('\n')
      ?? response.error?.message ?? '';

    if (setting === '1') {
      expect(forwarded).toBe(0);
      expect(text).toMatch(/worktree|not indexed|isn't indexed|No CodeGraph project/i);
      expect(text).not.toContain('permissive daemon result');
    } else {
      expect(forwarded).toBe(1);
      expect(text).toBe('permissive daemon result');
    }
  }, 15_000);

  it.each([
    { setting: '1', metadata: 'rootUri' },
    { setting: '1', metadata: 'workspaceFolders' },
    { setting: '0', metadata: 'rootUri' },
    { setting: '0', metadata: 'workspaceFolders' },
  ])('enforces strict mode $setting for argument-less calls after initialize.$metadata selects another worktree', async ({ setting, metadata }) => {
    vi.stubEnv(STRICT_ENV, setting);
    expect(resolveServerRoot(mainRepo).root).toBe(mainRepo);
    const uri = pathToFileURL(linkedWorktree).href;
    const initializeParams = metadata === 'rootUri'
      ? { rootUri: uri }
      : { workspaceFolders: [{ uri, name: 'linked-worktree' }] };
    // A native workspace signal overrides the valid launch root. A tool call
    // without projectPath must not silently fall back to the main-tree daemon.
    const { forwarded, response } = await callAttachedProxy(mainRepo, undefined, initializeParams);
    const text = response.result?.content?.map((item) => item.text ?? '').join('\n')
      ?? response.error?.message ?? '';

    if (setting === '1') {
      expect(forwarded).toBe(0);
      expect(text).toMatch(/worktree|not indexed|isn't indexed|No CodeGraph project/i);
      expect(text).not.toContain('permissive daemon result');
    } else {
      expect(forwarded).toBe(1);
      expect(text).toBe('permissive daemon result');
    }
  }, 15_000);

  it('retains parent-daemon attachment when strict mode is disabled', async () => {
    vi.stubEnv(STRICT_ENV, undefined);
    const server = new MCPServer(linkedWorktree);
    const startup = observeStartup(server);

    await server.start();

    expect(startup.proxy).toHaveBeenCalledTimes(1);
    expect(startup.proxy).toHaveBeenCalledWith(mainRepo);
    expect(startup.direct).not.toHaveBeenCalled();
  });
});
