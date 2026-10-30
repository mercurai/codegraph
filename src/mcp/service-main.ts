import type { Readable, Writable } from 'stream';
import { processHasWasmRuntimeFlags } from '../extraction/wasm-runtime-flags';
import { loadManagedControl } from './managed-security';
import type { ManagedCodeGraphOptions } from './managed-service';

type BootstrapOptions = Omit<ManagedCodeGraphOptions, 'now'>;

function invalid(): Error { return new Error('Invalid managed CodeGraph bootstrap options'); }

function exact(value: unknown, fields: readonly string[]): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw invalid();
  const input = value as Record<string, unknown>;
  const keys = Object.keys(input);
  if (keys.length !== fields.length || keys.some((key) => !fields.includes(key))) throw invalid();
  return input;
}

function positive(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1) throw invalid();
  return value;
}

function boolean(value: unknown): boolean {
  if (typeof value !== 'boolean') throw invalid();
  return value;
}

function parseHttp(value: unknown): BootstrapOptions['http'] {
  const input = exact(value, ['host', 'port', 'allowedOrigins', 'maxSessions', 'maxConversations',
    'maxRequests', 'maxPendingPerSession', 'maxBodyBytes']);
  if (input.host !== '127.0.0.1' && input.host !== '::1') throw invalid();
  if (typeof input.port !== 'number' || !Number.isInteger(input.port) || input.port < 0 || input.port > 65535) throw invalid();
  if (!Array.isArray(input.allowedOrigins) || input.allowedOrigins.some((origin: unknown) =>
    typeof origin !== 'string' || !origin || origin.length > 4096 || /[\x00-\x1f\x7f]/.test(origin))) throw invalid();
  return { host: input.host, port: input.port, allowedOrigins: [...input.allowedOrigins] as string[],
    maxSessions: positive(input.maxSessions), maxConversations: positive(input.maxConversations),
    maxRequests: positive(input.maxRequests), maxPendingPerSession: positive(input.maxPendingPerSession),
    maxBodyBytes: positive(input.maxBodyBytes) };
}

function parseOptions(value: unknown): BootstrapOptions {
  const input = exact(value, ['ttlMs', 'maxIdentities', 'maxLaunches', 'mcpServerName', 'http', 'engine']);
  const engine = exact(input.engine, ['watch', 'queryPool', 'queryPoolDefaultMax', 'maxOpenProjects', 'maxQueuedJobs']);
  if (typeof input.mcpServerName !== 'string') throw invalid();
  // The shared authority remains responsible for server-name and identity rules.
  return { ttlMs: positive(input.ttlMs), maxIdentities: positive(input.maxIdentities),
    maxLaunches: positive(input.maxLaunches), mcpServerName: input.mcpServerName,
    http: parseHttp(input.http), engine: { watch: boolean(engine.watch), queryPool: boolean(engine.queryPool),
      queryPoolDefaultMax: positive(engine.queryPoolDefaultMax), maxOpenProjects: positive(engine.maxOpenProjects),
      maxQueuedJobs: positive(engine.maxQueuedJobs) } };
}

/** Explicit managed-service entry; importing this module never starts work. */
export async function main(
  input: Readable = process.stdin,
  output: Writable = process.stdout,
  now: () => Date = () => new Date(),
): Promise<void> {
  // Re-exec would retain a second Node parent and split supervisor ownership.
  if (!processHasWasmRuntimeFlags()) throw Object.assign(new Error('Managed CodeGraph requires explicit Node runtime flags'),
    { code: 'MANAGED_CODEGRAPH_RUNTIME_FLAGS_REQUIRED' });
  try {
    const { runServiceControl } = await loadManagedControl();
    await runServiceControl({ kind: 'codegraph', input, output, now, parseOptions,
      async createService(options, clock) {
        const { startManagedCodeGraphService } = require('./managed-service') as typeof import('./managed-service');
        return startManagedCodeGraphService({ ...options, now: clock });
      },
    });
  } finally {
    // The runner has retired its service; release the entry's stdin read handle.
    input.pause();
  }
}

if (require.main === module) {
  void loadManagedControl().then(({ runServiceEntry }) =>
    runServiceEntry((input, output) => main(input, output)),
  ).catch(() => {
    process.exitCode = 1;
  });
}
