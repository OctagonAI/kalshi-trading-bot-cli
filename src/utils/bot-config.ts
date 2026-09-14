import { readFileSync, writeFileSync, mkdirSync } from 'fs';
import { dirname } from 'path';
import { appPath } from './paths.js';

export interface BotConfig {
  scan: { interval: number; theme: string };
  risk: {
    kelly_multiplier: number;
    min_edge_threshold: number;
    max_position_pct: number;
    max_spread_cents: number;
    min_volume_24h: number;
    liquidity_haircut: number;
    liquidity_spread_threshold: number;
    liquidity_volume_threshold: number;
    max_drawdown: number;
    max_positions: number;
    max_per_category: number;
    daily_loss_limit: number;
    bear_check_min_edge_pp: number;
  };
  octagon: { daily_credit_ceiling: number; price_move_threshold: number };
  alerts: { min_edge: number; channels: string[] };
  watch: { min_interval_minutes: number; ticker_interval_seconds: number };
  gateway: { whatsapp: { enabled: boolean } };
  daemon: { interval_minutes: number };
  mandate: {
    kill_switch: boolean;
    kill_reason: string;
    max_contracts_per_order: number;
    max_notional_per_order: number;
    max_daily_loss: number;
  };
}

const DEFAULTS: BotConfig = {
  scan: { interval: 60, theme: 'top50' },
  risk: { kelly_multiplier: 0.5, min_edge_threshold: 0.05, max_position_pct: 0.10, max_spread_cents: 5, min_volume_24h: 500, liquidity_haircut: 0.50, liquidity_spread_threshold: 3, liquidity_volume_threshold: 1000, max_drawdown: 0.20, max_positions: 10, max_per_category: 3, daily_loss_limit: 200, bear_check_min_edge_pp: 15 },
  octagon: { daily_credit_ceiling: 100, price_move_threshold: 0.05 },
  alerts: { min_edge: 0.05, channels: ['terminal'] },
  watch: { min_interval_minutes: 15, ticker_interval_seconds: 5 },
  gateway: { whatsapp: { enabled: false } },
  daemon: { interval_minutes: 15 },
  mandate: { kill_switch: false, kill_reason: '', max_contracts_per_order: 100, max_notional_per_order: 100, max_daily_loss: 50 },
};

const CONFIG_PATH = appPath('config.json');

// In-memory cache to avoid re-reading config.json on every getBotSetting call
let _cachedConfig: BotConfig | null = null;

function deepMerge(defaults: Record<string, unknown>, overrides: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = { ...defaults };
  for (const key of Object.keys(overrides)) {
    if (
      result[key] !== null &&
      typeof result[key] === 'object' &&
      !Array.isArray(result[key]) &&
      typeof overrides[key] === 'object' &&
      overrides[key] !== null &&
      !Array.isArray(overrides[key])
    ) {
      result[key] = deepMerge(result[key] as Record<string, unknown>, overrides[key] as Record<string, unknown>);
    } else {
      result[key] = overrides[key];
    }
  }
  return result;
}

export function loadBotConfig(): BotConfig {
  if (_cachedConfig) return _cachedConfig;
  try {
    const raw = readFileSync(CONFIG_PATH, 'utf-8');
    const parsed = JSON.parse(raw);
    _cachedConfig = deepMerge(DEFAULTS as unknown as Record<string, unknown>, parsed) as unknown as BotConfig;
    return _cachedConfig;
  } catch {
    _cachedConfig = structuredClone(DEFAULTS);
    return _cachedConfig;
  }
}

export function saveBotConfig(config: BotConfig): boolean {
  try {
    mkdirSync(dirname(CONFIG_PATH), { recursive: true });
    writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2) + '\n');
    _cachedConfig = config;
    return true;
  } catch {
    return false;
  }
}

function walkGet(obj: Record<string, unknown>, keys: string[]): unknown {
  let current: unknown = obj;
  for (const k of keys) {
    if (current === null || typeof current !== 'object') return undefined;
    current = (current as Record<string, unknown>)[k];
  }
  return current;
}

function walkSet(obj: Record<string, unknown>, keys: string[], value: unknown): void {
  let current: Record<string, unknown> = obj;
  for (let i = 0; i < keys.length - 1; i++) {
    if (typeof current[keys[i]] !== 'object' || current[keys[i]] === null) {
      current[keys[i]] = {};
    }
    current = current[keys[i]] as Record<string, unknown>;
  }
  current[keys[keys.length - 1]] = value;
}

export function getBotSetting(dotKey: string): unknown {
  const config = loadBotConfig();
  const keys = dotKey.split('.');
  return walkGet(config as unknown as Record<string, unknown>, keys);
}

const NUMERIC_VALIDATORS: Record<string, (v: number) => string | null> = {
  'watch.min_interval_minutes': (v) => v > 0 ? null : 'must be > 0',
  'watch.ticker_interval_seconds': (v) => v > 0 ? null : 'must be > 0',
  'scan.interval': (v) => v > 0 ? null : 'must be > 0',
  'risk.min_edge_threshold': (v) => v >= 0 && v <= 1 ? null : 'must be between 0 and 1',
  'risk.max_position_pct': (v) => v > 0 && v <= 1 ? null : 'must be between 0 and 1',
  'risk.liquidity_haircut': (v) => v >= 0 && v <= 1 ? null : 'must be between 0 and 1',
  'risk.max_drawdown': (v) => v > 0 && v <= 1 ? null : 'must be between 0 and 1',
  'risk.kelly_multiplier': (v) => v > 0 && v <= 1 ? null : 'must be between 0 and 1',
  'risk.daily_loss_limit': (v) => v > 0 ? null : 'must be > 0',
  'risk.max_spread_cents': (v) => v >= 0 ? null : 'must be >= 0',
  'risk.min_volume_24h': (v) => v >= 0 ? null : 'must be >= 0',
  'risk.max_positions': (v) => v > 0 && Number.isInteger(v) ? null : 'must be a positive integer',
  'risk.max_per_category': (v) => v > 0 && Number.isInteger(v) ? null : 'must be a positive integer',
  'octagon.price_move_threshold': (v) => v >= 0 && v <= 1 ? null : 'must be between 0 and 1',
  'octagon.daily_credit_ceiling': (v) => v >= 0 ? null : 'must be >= 0',
  'alerts.min_edge': (v) => v >= 0 && v <= 1 ? null : 'must be between 0 and 1',
  'mandate.max_contracts_per_order': (v) => v > 0 ? null : 'must be > 0',
  'mandate.max_notional_per_order': (v) => v > 0 ? null : 'must be > 0',
  'mandate.max_daily_loss': (v) => v > 0 ? null : 'must be > 0',
  'risk.bear_check_min_edge_pp': (v) => v > 0 ? null : 'must be > 0',
  'daemon.interval_minutes': (v) => v > 0 ? null : 'must be > 0',
};

export function setBotSetting(dotKey: string, rawValue: string): { oldValue: unknown; newValue: unknown } {
  const keys = dotKey.split('.');
  const defaultValue = walkGet(DEFAULTS as unknown as Record<string, unknown>, keys);
  if (defaultValue === undefined) {
    throw new Error(`Unknown config key: ${dotKey}`);
  }

  let newValue: unknown;
  const defaultType = typeof defaultValue;
  if (defaultType === 'number') {
    newValue = Number(rawValue);
    if (isNaN(newValue as number)) throw new Error(`Invalid number for ${dotKey}: ${rawValue}`);
    const validator = NUMERIC_VALIDATORS[dotKey];
    if (validator) {
      const err = validator(newValue as number);
      if (err) throw new Error(`Invalid value for ${dotKey}: ${rawValue} (${err})`);
    }
  } else if (defaultType === 'boolean') {
    const lower = rawValue.toLowerCase();
    if (lower !== 'true' && lower !== 'false') {
      throw new Error(`Invalid boolean for ${dotKey}: ${rawValue} (expected 'true' or 'false')`);
    }
    newValue = lower === 'true';
  } else if (Array.isArray(defaultValue)) {
    try {
      newValue = JSON.parse(rawValue);
    } catch {
      throw new Error(`Invalid JSON for ${dotKey}: ${rawValue}`);
    }
    if (!Array.isArray(newValue)) {
      throw new Error(`Expected JSON array for ${dotKey}: ${rawValue}`);
    }
  } else {
    newValue = rawValue;
  }

  const current = loadBotConfig();
  const oldValue = walkGet(current as unknown as Record<string, unknown>, keys);

  // Mutate a clone, not the cached object: loadBotConfig() hands back
  // _cachedConfig by reference, so writing through it would leave the new value
  // live in memory even when the disk write fails below — and a later
  // successful save from elsewhere would then persist it.
  const next = structuredClone(current);
  walkSet(next as unknown as Record<string, unknown>, keys, newValue);

  // saveBotConfig swallows write errors and returns false; dropping that makes
  // a failed write indistinguishable from success. It adopts `next` as the new
  // cache only on success, so a failure leaves the old value in place.
  if (!saveBotConfig(next)) {
    throw new Error(`Failed to write config to disk — ${dotKey} was not saved.`);
  }

  return { oldValue, newValue };
}

export interface FlatSetting {
  key: string;
  value: unknown;
  default: unknown;
  isDefault: boolean;
}

function flatten(
  obj: Record<string, unknown>,
  defaults: Record<string, unknown>,
  prefix: string,
  result: FlatSetting[],
): void {
  for (const key of Object.keys(defaults)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    const val = obj[key];
    const def = defaults[key];

    if (def !== null && typeof def === 'object' && !Array.isArray(def)) {
      flatten(
        (val ?? {}) as Record<string, unknown>,
        def as Record<string, unknown>,
        fullKey,
        result,
      );
    } else {
      result.push({
        key: fullKey,
        value: val ?? def,
        default: def,
        isDefault: JSON.stringify(val ?? def) === JSON.stringify(def),
      });
    }
  }
}

export function getAllSettings(): FlatSetting[] {
  const config = loadBotConfig();
  const result: FlatSetting[] = [];
  flatten(
    config as unknown as Record<string, unknown>,
    DEFAULTS as unknown as Record<string, unknown>,
    '',
    result,
  );
  return result;
}
