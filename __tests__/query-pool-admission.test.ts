/** Bounded queue behavior through QueryPool's existing controlled-worker seam. */
import { EventEmitter } from 'events';
import { describe, expect, it } from 'vitest';
import { QueryPool, type PoolWorker } from '../src/mcp/query-pool';
import type { ToolResult } from '../src/mcp/tools';

interface Call { id: number; args: Record<string, unknown> }

class HeldWorker extends EventEmitter implements PoolWorker {
  readonly calls: Call[] = [];
  postMessage(message: unknown): void { this.calls.push(message as Call); }
  terminate(): Promise<number> { return Promise.resolve(0); }
  complete(index: number): void {
    const call = this.calls[index]!;
    const result: ToolResult = { content: [{ type: 'text', text: String(call.args.label) }] };
    this.emit('message', { type: 'result', id: call.id, result });
  }
}

describe('QueryPool admission', () => {
  it.each(['failed-start', 'factory-throw'] as const)('settles queued work when the crash budget is exhausted (%s)', async (failure) => {
    const workers: HeldWorker[] = [];
    const pool = new QueryPool({ root: null, size: 1, maxRetries: 0, waitForCompletion: true,
      createWorker: () => {
        if (failure === 'factory-throw' && workers.length) throw new Error('fixture worker launch failed');
        const worker = new HeldWorker(); workers.push(worker); return worker;
      },
    });
    workers[0]!.emit('message', { type: 'ready', ok: true });
    try {
      const running = pool.run('codegraph_search', { label: 'running' });
      const queued = pool.run('codegraph_search', { label: 'queued' });
      workers[0]!.emit('error', new Error('fixture running worker failed'));
      for (let index = 1; index < 32 && pool.healthy; index++) {
        // Replacement workers fail before ready, so the queued job cannot run.
        workers[index]!.emit('message', { type: 'ready', ok: false });
      }
      expect(pool.healthy).toBe(false);
      expect(pool.liveWorkers).toBe(0);
      expect((await running).isError).toBe(true);
      const result = await Promise.race([queued, new Promise<null>((resolve) => setImmediate(() => resolve(null)))]);
      expect(result, 'accepted queued work must settle without a timer or destroy()').not.toBeNull();
      expect(result?.content[0]?.text).toMatch(/crash|unavailable|retry|busy/i);
      const later = await Promise.race([
        pool.run('codegraph_search', { label: 'after-exhaustion' }),
        new Promise<null>((resolve) => setImmediate(() => resolve(null))),
      ]);
      expect(later, 'an unhealthy pool must not accept an unserviceable queue').not.toBeNull();
      expect(workers.flatMap((worker) => worker.calls).map((call) => call.args.label)).toEqual(['running']);
    } finally { await pool.destroy(); }
  });

  it('rejects overflow before retaining it and preserves accepted FIFO work', async () => {
    const worker = new HeldWorker();
    // Structural options keep this test compilable for RED before the option exists.
    const options = { root: null, size: 1, maxQueuedJobs: 1, createWorker: () => worker };
    const pool = new QueryPool(options);
    worker.emit('message', { type: 'ready', ok: true });
    try {
      const first = pool.run('codegraph_search', { label: 'first' });
      expect(worker.calls.map((call) => call.args.label)).toEqual(['first']);
      const queued = pool.run('codegraph_search', { label: 'queued' });
      const overflow = pool.run('codegraph_search', { label: 'overflow' });
      // A macrotask barrier distinguishes immediate refusal from an unbounded
      // queue without sleeping or waiting for its 45-second backstop.
      const result = await Promise.race([
        overflow,
        new Promise<null>((resolve) => setImmediate(() => resolve(null))),
      ]);
      expect(result, 'queue overflow must settle without a worker completing').not.toBeNull();
      expect(result?.isError).toBeFalsy();
      expect(result?.content[0]?.text).toMatch(/busy|capacity|retry/i);
      worker.complete(0);
      expect((await first).content[0]?.text).toBe('first');
      expect(worker.calls.map((call) => call.args.label)).toEqual(['first', 'queued']);
      worker.complete(1);
      expect((await queued).content[0]?.text).toBe('queued');
      const recovered = pool.run('codegraph_search', { label: 'recovered' });
      expect(worker.calls.map((call) => call.args.label)).toEqual(['first', 'queued', 'recovered']);
      worker.complete(2);
      expect((await recovered).content[0]?.text).toBe('recovered');
      expect(pool.liveWorkers).toBe(1);
    } finally { await pool.destroy(); }
  });
});
