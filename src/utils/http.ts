/**
 * fetch() with a request deadline.
 *
 * Without one, a half-open connection never settles: the fetch promise hangs
 * forever and never rejects, so retry ladders and DLQ paths around it never
 * fire either. Every caller that reads network data stalls with no output.
 *
 * Two subtleties this helper exists to get right:
 *
 *   - The deadline stays armed until the body is read. `fetch()` resolves as
 *     soon as headers arrive; the body is still an unread stream, so clearing
 *     the timer at that point leaves the body read unbounded. The timer is
 *     unref'd instead, so a late abort can't hold the process open and firing
 *     after the body is consumed is a harmless no-op.
 *   - A caller's own `init.signal` is honoured rather than replaced, or
 *     cancellation by the caller silently becomes a no-op.
 *
 * Aborting produces the usual `AbortError`, so existing `err.name ===
 * 'AbortError'` handling keeps working.
 */

/** Default per-request deadline. Callers with slower endpoints pass their own. */
export const DEFAULT_TIMEOUT_MS = 30_000;

export async function fetchWithDeadline(
  url: string,
  init: RequestInit = {},
  timeoutMs: number = DEFAULT_TIMEOUT_MS,
): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  // A late abort must not keep a CLI process alive waiting on the timer.
  timer.unref?.();

  const signal = init.signal
    ? AbortSignal.any([init.signal, controller.signal])
    : controller.signal;

  try {
    return await fetch(url, { ...init, signal });
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}
