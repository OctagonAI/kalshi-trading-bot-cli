import { describe, it, expect, beforeEach, afterEach, spyOn } from 'bun:test';
import { toDollarString, fromDollarString, KalshiApiError, callKalshiApi } from '../api.js';
import { auditTrail } from '../../../audit/index.js';
import { dlqWriter } from '../dlq.js';

let auditLogSpy: ReturnType<typeof spyOn>;
let dlqAppendSpy: ReturnType<typeof spyOn>;
let originalFetch: typeof globalThis.fetch;
let originalSetTimeout: typeof globalThis.setTimeout;

beforeEach(() => {
  auditLogSpy = spyOn(auditTrail, 'log').mockImplementation(() => {});
  dlqAppendSpy = spyOn(dlqWriter, 'append').mockImplementation(() => {});

  // Save originals
  originalFetch = globalThis.fetch;
  originalSetTimeout = globalThis.setTimeout;

  // Make setTimeout instant for retry tests
  // @ts-expect-error - simplified mock
  globalThis.setTimeout = (fn: () => void, _ms?: number) => {
    fn();
    return 0;
  };
});

afterEach(() => {
  globalThis.fetch = originalFetch;
  globalThis.setTimeout = originalSetTimeout;
  auditLogSpy.mockRestore();
  dlqAppendSpy.mockRestore();
});

describe('Dollar conversion', () => {
  it('toDollarString converts cents to dollar string', () => {
    expect(toDollarString(0)).toBe('0.00');
    expect(toDollarString(1)).toBe('0.01');
    expect(toDollarString(58)).toBe('0.58');
    expect(toDollarString(99)).toBe('0.99');
    expect(toDollarString(100)).toBe('1.00');
  });

  it('fromDollarString converts dollar string to cents', () => {
    expect(fromDollarString('0.00')).toBe(0);
    expect(fromDollarString('0.01')).toBe(1);
    expect(fromDollarString('0.58')).toBe(58);
    expect(fromDollarString('0.99')).toBe(99);
    expect(fromDollarString('1.00')).toBe(100);
  });

  it('round-trips correctly for 0-100', () => {
    for (let cents = 0; cents <= 100; cents++) {
      expect(fromDollarString(toDollarString(cents))).toBe(cents);
    }
  });
});

describe('KalshiApiError', () => {
  it('has statusCode, statusText, and body', () => {
    const err = new KalshiApiError(429, 'Too Many Requests', 'rate limited');
    expect(err.statusCode).toBe(429);
    expect(err.statusText).toBe('Too Many Requests');
    expect(err.body).toBe('rate limited');
    expect(err.message).toContain('429');
    expect(err.name).toBe('KalshiApiError');
  });
});

describe('withRetry via callKalshiApi', () => {
  it('retries on 429 then succeeds', async () => {
    let callCount = 0;
    globalThis.fetch = (async () => {
      callCount++;
      if (callCount <= 2) {
        return new Response('rate limited', { status: 429, statusText: 'Too Many Requests' });
      }
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    }) as unknown as typeof fetch;

    const result = await callKalshiApi('GET', '/test');
    expect(result).toEqual({ ok: true });
    expect(callCount).toBe(3);
    // Should have logged 2 API_RETRY events
    const retryLogs = auditLogSpy.mock.calls.filter(
      (c: unknown[]) => (c[0] as { type: string }).type === 'API_RETRY'
    );
    expect(retryLogs.length).toBe(2);
  });

  it('does not retry on 400', async () => {
    let callCount = 0;
    globalThis.fetch = (async () => {
      callCount++;
      return new Response('bad request', { status: 400, statusText: 'Bad Request' });
    }) as unknown as typeof fetch;

    await expect(callKalshiApi('GET', '/test')).rejects.toThrow(KalshiApiError);
    expect(callCount).toBe(1);
  });

  it('writes to DLQ after exhausting retries', async () => {
    let callCount = 0;
    globalThis.fetch = (async () => {
      callCount++;
      return new Response('rate limited', { status: 429, statusText: 'Too Many Requests' });
    }) as unknown as typeof fetch;

    await expect(callKalshiApi('GET', '/test')).rejects.toThrow(KalshiApiError);
    // 1 initial + 5 retries = 6 calls
    expect(callCount).toBe(6);
    // DLQ should have been written
    expect(dlqAppendSpy).toHaveBeenCalledTimes(1);
    const dlqCall = dlqAppendSpy.mock.calls[0][0] as { method: string; path: string; attempts: number };
    expect(dlqCall.method).toBe('GET');
    expect(dlqCall.path).toBe('/test');
    expect(dlqCall.attempts).toBe(6);
    // DLQ_ENTRY audit event
    const dlqAudit = auditLogSpy.mock.calls.filter(
      (c: unknown[]) => (c[0] as { type: string }).type === 'DLQ_ENTRY'
    );
    expect(dlqAudit.length).toBe(1);
  });

  it('retries on 500 server errors', async () => {
    let callCount = 0;
    globalThis.fetch = (async () => {
      callCount++;
      if (callCount <= 1) {
        return new Response('internal error', { status: 500, statusText: 'Internal Server Error' });
      }
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    }) as unknown as typeof fetch;

    const result = await callKalshiApi('GET', '/test');
    expect(result).toEqual({ ok: true });
    expect(callCount).toBe(2);
  });

  it('does not retry on 401 or 403', async () => {
    for (const status of [401, 403]) {
      let callCount = 0;
      globalThis.fetch = (async () => {
        callCount++;
        return new Response('forbidden', { status, statusText: 'Forbidden' });
      }) as unknown as typeof fetch;

      await expect(callKalshiApi('GET', '/test')).rejects.toThrow(KalshiApiError);
      expect(callCount).toBe(1);
    }
  });
});
