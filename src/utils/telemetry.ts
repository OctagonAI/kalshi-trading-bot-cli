import { loadConfig, saveConfig } from './config.js';

const CLIENT_KEY = 'client-TbH2tfgwg2CYu87Y932Wj2CdNTfyy7303HFszn0YZny';
const LOG_EVENT_URL = 'https://events.statsigapi.net/v1/log_event';

let userId: string | null = null;
let disabled = false;
let eventQueue: StatsigEvent[] = [];

interface StatsigEvent {
  eventName: string;
  value?: string;
  metadata?: Record<string, string | number | boolean>;
  time: number;
  user: { userID: string };
}

function isEnabled(): boolean {
  const val = process.env.TELEMETRY_ENABLED;
  if (val === 'false' || val === '0') return false;
  return true;
}

function getOrCreateAnonymousId(): string {
  const config = loadConfig();
  if (config.anonymousId) return config.anonymousId;
  const id = crypto.randomUUID();
  saveConfig({ ...config, anonymousId: id });
  return id;
}

export async function initTelemetry(): Promise<void> {
  if (disabled || userId) return;
  if (!isEnabled()) {
    disabled = true;
    return;
  }

  try {
    userId = getOrCreateAnonymousId();

    // Intercept process.exit to flush telemetry before exiting.
    // dispatch.ts calls process.exit() in many code paths, which would
    // kill the process before events are sent.
    const originalExit = process.exit;
    process.exit = (async (code?: number) => {
      await shutdownTelemetry();
      originalExit(code as any);
    }) as never;
  } catch {
    disabled = true;
  }
}

export function trackEvent(
  name: string,
  metadata?: Record<string, string | number | boolean>,
): void {
  if (!userId || disabled) return;
  try {
    eventQueue.push({
      eventName: name,
      metadata,
      time: Date.now(),
      user: { userID: userId },
    });
  } catch {}
}

async function flushEvents(): Promise<void> {
  if (eventQueue.length === 0 || !userId) return;
  const events = eventQueue;
  eventQueue = [];
  try {
    await fetch(LOG_EVENT_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'statsig-api-key': CLIENT_KEY,
        'statsig-sdk-type': 'js-mono',
        'statsig-sdk-version': '1.0.0',
      },
      body: JSON.stringify({
        events,
        statsigMetadata: {
          sdkType: 'js-mono',
          sdkVersion: '1.0.0',
        },
      }),
    });
  } catch {}
}

export async function shutdownTelemetry(): Promise<void> {
  if (disabled) return;
  try {
    await Promise.race([
      flushEvents(),
      new Promise((resolve) => setTimeout(resolve, 3000)),
    ]);
  } catch {}
  userId = null;
}
