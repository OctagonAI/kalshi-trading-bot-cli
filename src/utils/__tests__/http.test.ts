import { describe, test, expect, afterEach } from 'bun:test';
import { fetchWithDeadline } from '../http.js';

const originalFetch = globalThis.fetch;
afterEach(() => { globalThis.fetch = originalFetch; });

/** A fetch that never settles until its signal aborts — a half-open connection. */
function installHangingFetch(): void {
  globalThis.fetch = ((_url: string, init?: RequestInit) => new Promise((_resolve, reject) => {
    init?.signal?.addEventListener('abort', () => {
      reject(new DOMException('The operation was aborted.', 'AbortError'));
    }, { once: true });
  })) as unknown as typeof fetch;
}

describe('fetchWithDeadline', () => {
  test('aborts a hanging request once the deadline passes', async () => {
    installHangingFetch();
    const err = await fetchWithDeadline('https://example.test/hang', {}, 20).catch((e: Error) => e);
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).name).toBe('AbortError');
  });

  test("honours the caller's own signal instead of replacing it", async () => {
    installHangingFetch();
    const caller = new AbortController();
    const promise = fetchWithDeadline('https://example.test/hang', { signal: caller.signal }, 60_000);
    caller.abort();
    const err = await promise.catch((e: Error) => e);
    expect((err as Error).name).toBe('AbortError');
  });

  test('passes the response through and leaves the body readable', async () => {
    globalThis.fetch = (async () => new Response('{"ok":true}', { status: 200 })) as unknown as typeof fetch;
    const resp = await fetchWithDeadline('https://example.test/ok', {}, 1_000);
    expect(resp.status).toBe(200);
    // The deadline must still allow the body to be read after headers resolve.
    expect(await resp.json()).toEqual({ ok: true });
  });

  test('forwards method, headers and body', async () => {
    let seen: RequestInit | undefined;
    globalThis.fetch = (async (_u: string, init?: RequestInit) => {
      seen = init;
      return new Response('{}', { status: 200 });
    }) as unknown as typeof fetch;
    await fetchWithDeadline('https://example.test/post', {
      method: 'POST', headers: { 'X-Test': '1' }, body: '{"a":1}',
    }, 1_000);
    expect(seen?.method).toBe('POST');
    expect((seen?.headers as Record<string, string>)['X-Test']).toBe('1');
    expect(seen?.body).toBe('{"a":1}');
    expect(seen?.signal).toBeDefined();
  });
});
