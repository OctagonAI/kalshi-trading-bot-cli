import { describe, test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/**
 * bot-config resolves its path from homedir() at module load, and caches the
 * config in module state. Both make it unsafe to exercise in-process, so each
 * case runs in a child bun with HOME pointed at a throwaway directory.
 */
function runInTempHome(home: string): { threw: boolean; after: unknown } {
  const script = `
    const { setBotSetting, getBotSetting } = await import(${JSON.stringify(join(process.cwd(), 'src/utils/bot-config.ts'))});
    let threw = false;
    try { setBotSetting('risk.kelly_multiplier', '0.25'); } catch { threw = true; }
    console.log('RESULT' + JSON.stringify({ threw, after: getBotSetting('risk.kelly_multiplier') }));
  `;
  const proc = Bun.spawnSync(['bun', '-e', script], { env: { ...process.env, HOME: home } });
  const line = proc.stdout.toString().split('\n').find((l) => l.startsWith('RESULT'));
  if (!line) throw new Error(`child produced no result: ${proc.stderr.toString().slice(0, 400)}`);
  return JSON.parse(line.slice('RESULT'.length));
}

describe('setBotSetting', () => {
  test('a successful write persists the new value', () => {
    const home = mkdtempSync(join(tmpdir(), 'kalshi-cfg-ok-'));
    const result = runInTempHome(home);
    expect(result.threw).toBe(false);
    expect(result.after).toBe(0.25);
  });

  test('a failed write neither persists nor retains the new value', () => {
    const home = mkdtempSync(join(tmpdir(), 'kalshi-cfg-fail-'));
    // config.json as a directory makes writeFileSync fail with EISDIR, so
    // loadBotConfig falls back to defaults and saveBotConfig returns false.
    mkdirSync(join(home, '.kalshi-bot', 'config.json'), { recursive: true });

    const result = runInTempHome(home);
    expect(result.threw).toBe(true);
    // The in-memory value must still be the default: loadBotConfig hands out
    // _cachedConfig by reference, so mutating it in place would leave 0.25 live
    // for the rest of the process despite the write having failed.
    expect(result.after).toBe(0.5);
  });
});
