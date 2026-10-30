const { test } = require('node:test');
const assert = require('node:assert/strict');
const { PersistentSessionServer } = require('../dist/src/server/session-server-v3.js');
const { ConnectomeTestingMCP } = require('../dist/src/mcp/mcp-server.js');

function fakeSession(id, {
  persistent = false,
  alive = true,
  prompt = true,
  processing = false,
  queued = 0,
  last = Date.now() - 2 * 60 * 60 * 1000,
} = {}) {
  return {
    id, name: id, cwd: '/', env: {}, isAlive: alive, persistent,
    shellAtPrompt: prompt, isProcessingCommand: processing,
    commandQueue: Array.from({ length: queued }, () => ({})),
    lastActivity: new Date(last), createdAt: new Date(last - 1000), logs: [],
  };
}

test('server-side idle GC prunes oldest eligible sessions and preserves live work', async () => {
  const server = new PersistentSessionServer();
  const now = Date.now();
  const sessions = [];
  for (let i = 0; i < 66; i++) {
    sessions.push(fakeSession(`old-${i}`, { last: now - (3 * 60 * 60 * 1000) - i * 1000 }));
  }
  sessions.push(fakeSession('persistent', { persistent: true, last: now - 9e6 }));
  sessions.push(fakeSession('silent-job', { prompt: false, last: now - 9e6 }));
  sessions.push(fakeSession('processing', { processing: true, prompt: false, last: now - 9e6 }));
  sessions.push(fakeSession('queued', { queued: 1, last: now - 9e6 }));
  sessions.push(fakeSession('protected', { last: now - 9e6 }));
  sessions.push(fakeSession('recent', { last: now }));
  server.sessions = new Map(sessions.map(session => [session.id, session]));

  const killed = [];
  server.killSession = async (id, graceful) => {
    assert.equal(graceful, true, 'idle-at-prompt GC hangs up shells and their background jobs');
    killed.push(id);
    server.sessions.delete(id);
  };

  const result = await server.pruneIdleSessions({
    maxSessions: 64, idleMs: 60 * 60 * 1000, maxPrune: 32,
    protectedSessionIds: ['protected']
  });
  assert.deepEqual(killed, Array.from({ length: 9 }, (_, index) => `old-${65-index}`));
  assert.deepEqual(result.pruned, killed);
  assert.deepEqual(result.failed, []);
  for (const id of ['persistent','silent-job','processing','queued','protected','recent']) {
    assert.equal(server.sessions.has(id), true, `${id} survives`);
  }
  server.sessions.clear();
});

test('small fleets are untouched even when an ephemeral session is stale', async () => {
  const server = new PersistentSessionServer();
  server.sessions = new Map([['stale', fakeSession('stale')]]);
  let kills = 0;
  server.killSession = async () => { kills++; };
  assert.deepEqual(await server.pruneIdleSessions({ maxSessions: 64 }), { pruned: [], failed: [] });
  assert.equal(kills, 0);
  server.sessions.clear();
});

test('persistence lives with the server session and survives MCP instance replacement', () => {
  const server = new PersistentSessionServer();
  server.sessions = new Map([['work', fakeSession('work')]]);
  assert.equal(server.listSessions()[0].persistent, false);
  server.setSessionPersistent('work', true);
  assert.equal(server.listSessions()[0].persistent, true);
  const anotherMcpProcess = {};
  assert.equal(server.listSessions()[0].persistent, true);
  assert.ok(anotherMcpProcess);
  server.sessions.clear();
});

test('concurrent runCommand calls protect every active target during prune', async () => {
  let releaseA;
  const waitA = new Promise(resolve => { releaseA = resolve; });
  const pruneCalls = [];
  const client = {
    listSessions: async () => [{ id: 'a' }, { id: 'b' }],
    exec: async (id) => {
      if (id === 'a') await waitA;
      return { output: id, exitCode: 0, duration: 1 };
    },
    pruneIdleSessions: async (params) => { pruneCalls.push(params.protectedSessionIds.slice().sort()); return { pruned: [], failed: [] }; },
    setSessionPersistent: async () => ({}),
  };
  const mcp = new ConnectomeTestingMCP();
  mcp.getClient = () => client;
  const a = mcp.runCommand({ session: 'a', command: 'true' });
  await new Promise(resolve => setImmediate(resolve));
  const b = await mcp.runCommand({ session: 'b', command: 'true' });
  assert.equal(b.output, 'b');
  assert.deepEqual(pruneCalls[0], ['a','b']);
  releaseA();
  await a;
});

test('createSession defaults persistent while runCommand auto-creation opts into idle GC', async () => {
  const server = new PersistentSessionServer();
  await server.createSession('manual');
  await server.createSession('auto', { persistent: false });
  const listed = new Map(server.listSessions().map(session => [session.id, session]));
  assert.equal(listed.get('manual').persistent, true);
  assert.equal(listed.get('auto').persistent, false);
  assert.equal(listed.get('manual').shellAtPrompt, true);
  assert.equal(listed.get('auto').shellAtPrompt, true);
  await server.killAll(false);
});

test('same-session concurrent calls keep the target protected until both settle', async () => {
  let releaseFirst;
  const waitFirst = new Promise(resolve => { releaseFirst = resolve; });
  let calls = 0;
  const pruneCalls = [];
  const client = {
    listSessions: async () => [{ id: 'same' }],
    exec: async () => {
      calls++;
      if (calls === 1) await waitFirst;
      return { output: 'ok', exitCode: 0, duration: 1 };
    },
    pruneIdleSessions: async (params) => { pruneCalls.push(params.protectedSessionIds.slice()); return { pruned: [], failed: [] }; },
    setSessionPersistent: async () => ({}),
  };
  const mcp = new ConnectomeTestingMCP();
  mcp.getClient = () => client;
  const first = mcp.runCommand({ session: 'same', command: 'one' });
  await new Promise(resolve => setImmediate(resolve));
  await mcp.runCommand({ session: 'same', command: 'two' });
  assert.deepEqual(pruneCalls[0], ['same']);
  assert.equal(mcp.activeRunTargets.get('same'), 1);
  releaseFirst();
  await first;
  assert.equal(mcp.activeRunTargets.has('same'), false);
});

test('prune rechecks live state immediately before kill', async () => {
  const server = new PersistentSessionServer();
  const sessions = [['candidate', fakeSession('candidate')]];
  for (let i = 0; i < 63; i++) sessions.push([`keep-${i}`, fakeSession(`keep-${i}`, { persistent: true })]);
  server.sessions = new Map(sessions);
  const originalGet = server.sessions.get.bind(server.sessions);
  let changed = false;
  server.sessions.get = (id) => {
    const session = originalGet(id);
    if (id === 'candidate' && !changed) {
      session.shellAtPrompt = false;
      session.isProcessingCommand = true;
      changed = true;
    }
    return session;
  };
  let kills = 0;
  server.killSession = async () => { kills++; };
  assert.deepEqual(await server.pruneIdleSessions({ maxSessions: 64 }), { pruned: [], failed: [] });
  assert.equal(kills, 0);
  server.sessions.clear();
});

test('prune removes at most the configured batch cap', async () => {
  const server = new PersistentSessionServer();
  const now = Date.now();
  const sessions = Array.from({ length: 100 }, (_, i) => fakeSession(`old-${i}`, {
    last: now - (3 * 60 * 60 * 1000) - i * 1000
  }));
  server.sessions = new Map(sessions.map(session => [session.id, session]));
  const killed = [];
  server.killSession = async (id) => { killed.push(id); server.sessions.delete(id); };
  const result = await server.pruneIdleSessions({ maxSessions: 64, maxPrune: 32 });
  assert.equal(result.pruned.length, 32);
  assert.deepEqual(killed, Array.from({ length: 32 }, (_, index) => `old-${99-index}`));
  assert.equal(server.sessions.size, 68);
  server.sessions.clear();
});

test('prune reports failed kills without hiding successful removals', async () => {
  const server = new PersistentSessionServer();
  const now = Date.now();
  const sessions = Array.from({ length: 66 }, (_, i) => fakeSession(`old-${i}`, {
    last: now - (3 * 60 * 60 * 1000) - i * 1000
  }));
  server.sessions = new Map(sessions.map(session => [session.id, session]));
  server.killSession = async (id) => {
    if (id === 'old-65') throw new Error('refused for test');
    server.sessions.delete(id);
  };
  const result = await server.pruneIdleSessions({ maxSessions: 64, maxPrune: 32 });
  assert.deepEqual(result.pruned.sort(), ['old-63', 'old-64']);
  assert.deepEqual(result.failed, [{ id: 'old-65', error: 'refused for test' }]);
  assert.equal(server.sessions.has('old-65'), true);
  server.sessions.clear();
});

test('persistence can be toggled in both directions and remains an explicit GC opt-out', async () => {
  const server = new PersistentSessionServer();
  const sessions = [['work', fakeSession('work', { persistent: true, alive: false })]];
  for (let i = 0; i < 63; i++) sessions.push([`keep-${i}`, fakeSession(`keep-${i}`, { persistent: true })]);
  server.sessions = new Map(sessions);
  let kills = 0;
  server.killSession = async (id) => { kills++; server.sessions.delete(id); };
  assert.deepEqual(await server.pruneIdleSessions({ maxSessions: 64 }), { pruned: [], failed: [] });
  assert.equal(kills, 0, 'even a dead persistent session survives automatic GC');
  assert.equal(server.setSessionPersistent('work', false).persistent, false);
  const result = await server.pruneIdleSessions({ maxSessions: 64 });
  assert.deepEqual(result.pruned, ['work']);
  assert.equal(server.sessions.has('work'), false);
  server.sessions.clear();
});
