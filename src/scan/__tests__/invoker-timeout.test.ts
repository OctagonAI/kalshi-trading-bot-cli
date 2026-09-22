import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { callOctagon } from '../invoker.js';

/**
 * fetchWithDeadline keeps its deadline armed through the body read, so the
 * timeout can fire after the headers have arrived. That abort has to reach the
 * same timeout message as one during the request itself.
 */
function abortedBody(status: number): Response {
  const stream = new ReadableStream({
    start(controller) {
      controller.error(new DOMException('The operation was aborted.', 'AbortError'));
    },
  });
  return new Response(stream, { status });
}

describe('callOctagon deadline', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test('a deadline that fires while reading the body is reported as a timeout', async () => {
    globalThis.fetch = (async () => abortedBody(200)) as unknown as typeof fetch;
    await expect(callOctagon('what is the fed doing', 'default')).rejects.toThrow(/timed out after 600s/);
  });

  test('an error response whose body read times out is not reported with an empty body', async () => {
    globalThis.fetch = (async () => abortedBody(400)) as unknown as typeof fetch;
    await expect(callOctagon('what is the fed doing', 'default')).rejects.toThrow(/timed out after 600s/);
  });
});
