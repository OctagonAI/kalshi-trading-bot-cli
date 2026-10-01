import { jest } from 'bun:test';

// The Bun runtime has advanceTimersByTime, but bun-types 1.3.3 doesn't declare it.
const timers = jest as unknown as { advanceTimersByTime(ms: number): void };

/**
 * Drive a promise to completion under `jest.useFakeTimers()`.
 *
 * Bun's fake timers advance only synchronously, so code that awaits a mocked
 * fetch between sleeps needs its pending promises flushed before each advance.
 * Flush with microtasks only: `Bun.sleep(0)` and `setImmediate` are faked too,
 * and yielding through them never returns.
 *
 * `stepMs` stays far below fetchWithDeadline's 60s abort, so a request's own
 * deadline never fires while it is still resolving.
 */
export async function settle<T>(promise: Promise<T>, stepMs = 1_000): Promise<T> {
  let done = false;
  const result = promise.finally(() => { done = true; });
  // Swallow here so an early rejection isn't reported as unhandled; `result` still rejects.
  result.catch(() => {});
  for (let i = 0; i < 10_000 && !done; i++) {
    for (let k = 0; k < 20; k++) await Promise.resolve();
    if (!done) timers.advanceTimersByTime(stepMs);
  }
  return result;
}
