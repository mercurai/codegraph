/** Consumed public API shapes for an optional context-mode security dependency.
 * No security rules are implemented here; the qualified package owns them.
 */
import type { IncomingMessage } from 'http';
import type { Readable, Writable } from 'stream';
import type { AddressInfo } from 'net';
import { pathToFileURL } from 'url';

export interface ManagedScope {
  provider: 'codex' | 'claude-code';
  home: string;
  launchId: string;
}

export interface ManagedLaunchMetadata {
  provider: ManagedScope['provider'];
  home: string;
  userHome: string;
  workspace: string;
}

export interface ManagedLaunchGrant {
  readonly scope: Readonly<ManagedScope>;
  readonly mcpCapability: string;
  readonly hookCapability: string;
}

export interface ManagedResolvedIdentity extends ManagedScope {
  sessionId: string;
  workspace: string;
  threadId?: string;
  agentId?: string;
}

export interface ManagedIdentityRegistry {
  resolve(scope: Readonly<ManagedScope>, call: { name: string; _meta?: unknown; arguments?: unknown }): ManagedResolvedIdentity;
}

export interface ManagedLaunchAuthority {
  issue(metadata: ManagedLaunchMetadata): ManagedLaunchGrant;
  authenticate(request: IncomingMessage): Readonly<ManagedScope>;
  authenticateHook(request: IncomingMessage): Readonly<ManagedScope>;
  registerHook(request: IncomingMessage, payload: unknown): void;
  metadata(scope: Readonly<ManagedScope>): Readonly<ManagedLaunchMetadata>;
  isActive(scope: Readonly<ManagedScope>): boolean;
  revoke(scope: Readonly<ManagedScope>): void;
  close(): void;
}

export interface ManagedSecurityModules {
  SharedIdentityRegistry: new (options: { now: () => Date; ttlMs: number; maxEntries: number }) => ManagedIdentityRegistry;
  createSharedLaunchAuthority(options: {
    identityRegistry: ManagedIdentityRegistry;
    now: () => Date;
    ttlMs: number;
    maxLaunches: number;
    mcpServerName: string;
    toolNames: readonly string[];
  }): ManagedLaunchAuthority;
}

/** Structural boundary only; the public context-mode module owns the protocol. */
export interface ManagedControlledService {
  issue(metadata: ManagedLaunchMetadata): ManagedLaunchGrant;
  revoke(scope: Readonly<ManagedScope>): void;
  listen(): Promise<void>;
  address(): AddressInfo;
  health(): { state: 'starting' | 'ready' | 'draining' | 'closed'; transportSessions: number;
    pendingInitializations: number; inFlightRequests: number };
  close(): Promise<void>;
}

export interface ManagedControlModule {
  runServiceEntry(run: (input: Readable, output: Writable, errors: Writable) => Promise<void>): Promise<void>;
  runServiceControl<T extends { ttlMs: number; maxLaunches: number }>(options: {
    kind: 'codegraph'; input: Readable; output: Writable; now: () => Date;
    parseOptions(value: unknown): T;
    createService(options: T, now: () => Date): Promise<ManagedControlledService>;
  }): Promise<void>;
}

// Match the CLI's existing native import boundary: CJS tsc rewrites ordinary
// import() into require(), which cannot load these ESM-only public exports.
const importESM = new Function('url', 'return import(url)') as (url: string) => Promise<Record<string, unknown>>;

/** Resolve from this installed package, never from a caller's workspace. */
export async function loadManagedSecurity(): Promise<ManagedSecurityModules> {
  try {
    const [identity, authority] = await Promise.all([
      importESM(pathToFileURL(require.resolve('context-mode/shared/identity')).href),
      importESM(pathToFileURL(require.resolve('context-mode/shared/launch-authority')).href),
    ]);
    if (typeof identity.SharedIdentityRegistry !== 'function' || typeof authority.createSharedLaunchAuthority !== 'function') {
      throw new Error('Invalid shared security exports');
    }
    return {
      SharedIdentityRegistry: identity.SharedIdentityRegistry as ManagedSecurityModules['SharedIdentityRegistry'],
      createSharedLaunchAuthority: authority.createSharedLaunchAuthority as ManagedSecurityModules['createSharedLaunchAuthority'],
    };
  } catch {
    // No package path, loader stack, or user-controlled payload reaches callers.
    throw new Error('context-mode shared security exports are unavailable or incompatible');
  }
}

/** Optional control stays separate from security-only consumers and imports. */
export async function loadManagedControl(): Promise<ManagedControlModule> {
  try {
    const control = await importESM(pathToFileURL(require.resolve('context-mode/shared/control')).href);
    if (typeof control.runServiceControl !== 'function' || typeof control.runServiceEntry !== 'function') {
      throw new Error('Invalid shared control export');
    }
    return {
      runServiceControl: control.runServiceControl as ManagedControlModule['runServiceControl'],
      runServiceEntry: control.runServiceEntry as ManagedControlModule['runServiceEntry'],
    };
  } catch {
    throw new Error('context-mode shared control export is unavailable or incompatible');
  }
}
