import { MCPEngine, type MCPEngineOptions } from './engine';
import { startHttpMcpServer, type HttpMcpServer, type HttpMcpServerOptions, type AuthenticatedHttpLaunch } from './http-server';
import { getStaticTools } from './tools';
import { loadManagedSecurity, type ManagedLaunchMetadata, type ManagedLaunchGrant, type ManagedScope,
  type ManagedLaunchAuthority, type ManagedIdentityRegistry } from './managed-security';

export interface ManagedCodeGraphOptions {
  now: () => Date;
  ttlMs: number;
  maxIdentities: number;
  maxLaunches: number;
  mcpServerName: string;
  http: Omit<HttpMcpServerOptions, 'engine' | 'authority' | 'hookAuthority'>;
  engine?: Pick<MCPEngineOptions, 'watch' | 'queryPool' | 'queryPoolDefaultMax' | 'maxOpenProjects' | 'maxQueuedJobs'>;
}

export interface ManagedCodeGraphService {
  readonly url: string;
  readonly hookUrl: string;
  /** Trusted in-process launcher only; never exposed as HTTP/MCP issuance. */
  issue(metadata: ManagedLaunchMetadata): ManagedLaunchGrant;
  revoke(scope: Readonly<ManagedScope>): void;
  stop(): Promise<void>;
  /** The start factory has already bound its listener; repeat listen is idempotent. */
  listen(): Promise<void>;
  address: HttpMcpServer['address'];
  health: HttpMcpServer['health'];
  close(): Promise<void>;
}

function scopeOf(launch: AuthenticatedHttpLaunch): ManagedScope {
  return { provider: launch.provider === 'claude' ? 'claude-code' : 'codex', home: launch.providerHome, launchId: launch.launchId };
}

/** Protocol-shape adaptation only; the shared authority and registry own all identity rules. */
function adaptAuthority(authority: ManagedLaunchAuthority, registry: ManagedIdentityRegistry) {
  return {
    isActive: (launch: AuthenticatedHttpLaunch): boolean => authority.isActive(scopeOf(launch)),
    async authenticate(request: Parameters<ManagedLaunchAuthority['authenticate']>[0]): Promise<AuthenticatedHttpLaunch | null> {
      try {
        const scope = authority.authenticate(request);
        const metadata = authority.metadata(scope);
        return { provider: scope.provider === 'claude-code' ? 'claude' : 'codex', providerHome: scope.home,
          launchId: scope.launchId, workspace: metadata.workspace };
      } catch { return null; }
    },
    async resolveCall(request: import('./transport').JsonRpcRequest, launch: AuthenticatedHttpLaunch) {
      try {
        const scope = scopeOf(launch);
        if (!authority.isActive(scope)) return null;
        const call = request.params as Parameters<ManagedIdentityRegistry['resolve']>[1];
        const identity = registry.resolve(scope, call);
        if (identity.workspace !== authority.metadata(scope).workspace) return null;
        return { workspace: identity.workspace, conversationId: JSON.stringify([
          identity.provider, identity.home, identity.launchId, identity.workspace,
          identity.sessionId, identity.threadId ?? null, identity.agentId ?? null,
        ]) };
      } catch { return null; }
    },
  };
}

async function closeComponents(authority: ManagedLaunchAuthority, http?: HttpMcpServer, engine?: MCPEngine): Promise<void> {
  const errors = new Set<unknown>();
  try { authority.close(); } catch (error) { errors.add(error); }
  try { await http?.stop(); } catch (error) { errors.add(error); }
  try { await engine?.stop(); } catch (error) { errors.add(error); }
  if (errors.size === 1) throw [...errors][0];
  if (errors.size > 1) throw new AggregateError([...errors], 'Managed CodeGraph cleanup failed');
}

export async function startManagedCodeGraphService(options: ManagedCodeGraphOptions): Promise<ManagedCodeGraphService> {
  // Snapshot trusted launch options before loading the optional ESM package.
  const { now, ttlMs, maxIdentities, maxLaunches, mcpServerName } = options;
  const httpOptions = { ...options.http, allowedOrigins: [...(options.http.allowedOrigins ?? [])] };
  const engineOptions = { ...options.engine };
  const security = await loadManagedSecurity();
  const registry = new security.SharedIdentityRegistry({ now, ttlMs, maxEntries: maxIdentities });
  const authority = security.createSharedLaunchAuthority({ identityRegistry: registry, now, ttlMs, maxLaunches,
    mcpServerName, toolNames: getStaticTools().map((tool) => tool.name) });
  // Check only the optional API boundary, never duplicate its security validation.
  if (typeof registry.resolve !== 'function' || !authority ||
    (['issue', 'authenticate', 'authenticateHook', 'registerHook', 'metadata', 'isActive', 'revoke', 'close'] as const)
      .some((name) => typeof authority[name] !== 'function')) {
    throw new Error('context-mode shared security exports are incompatible');
  }
  let engine: MCPEngine | undefined;
  let http: HttpMcpServer | undefined;
  try {
    engine = new MCPEngine({ queryPool: true, ...engineOptions, explicitProjectsOnly: true });
    http = await startHttpMcpServer({ ...httpOptions, engine, authority: adaptAuthority(authority, registry), hookAuthority: authority });
  } catch (error) {
    try { await closeComponents(authority, http, engine); }
    catch (cleanup) { throw new AggregateError([error, cleanup], 'Managed CodeGraph construction failed'); }
    throw error;
  }
  const transport = http;
  const ownedEngine = engine;
  let stopped: Promise<void> | undefined;
  const close = (): Promise<void> => stopped ??= closeComponents(authority, transport, ownedEngine);
  return Object.freeze({
    url: transport.url, hookUrl: new URL('/hooks', transport.url).href,
    issue: (metadata: ManagedLaunchMetadata) => authority.issue(metadata),
    revoke: (scope: Readonly<ManagedScope>) => authority.revoke(scope),
    listen: async () => { if (stopped) throw new Error('Managed CodeGraph service is closed'); },
    address: () => transport.address(), health: () => transport.health(), close, stop: close,
  });
}
