/**
 * Preloaded before every test file (bunfig.toml). Guarantees unit tests never
 * see real credentials — not from the shell, and not from the .env that
 * src/utils/env.ts loads on import (~/.kalshi-bot/.env or ./.env).
 *
 * Every credential starts from a fixed baseline, and the baseline is put back
 * around every test, so a suite that overrides or clears a key can't leak that
 * into the next one. Integration tests (*.itest.ts) need the real values and
 * opt out with KALSHI_BOT_LIVE_TESTS=1 (see `bun run test:integration`).
 */
import { afterEach, beforeEach } from 'bun:test';
import { TEST_PRIVATE_KEY } from './fixtures/test-private-key.js';

export const CREDENTIAL_BASELINE: Record<string, string> = {
  OCTAGON_API_KEY: 'sk_test',
  KALSHI_API_KEY: 'test-key',
  KALSHI_PRIVATE_KEY: TEST_PRIVATE_KEY,
  KALSHI_PRIVATE_KEY_FILE: '',
  TAVILY_API_KEY: '',
  OPENAI_API_KEY: '',
  ANTHROPIC_API_KEY: '',
  GOOGLE_API_KEY: '',
  XAI_API_KEY: '',
  MOONSHOT_API_KEY: '',
  DEEPSEEK_API_KEY: '',
  OPENROUTER_API_KEY: '',
};

// Empty strings rather than `delete`: dotenv fills in keys that are missing
// but never overwrites ones that are set, even to ''.
function restoreCredentials(): void {
  for (const [name, value] of Object.entries(CREDENTIAL_BASELINE)) {
    process.env[name] = value;
  }
}

if (process.env.KALSHI_BOT_LIVE_TESTS !== '1') {
  restoreCredentials();
  // env.ts runs dotenv once, on first import. Trigger that now, while every
  // key is already set, so no later import can pull real values back in.
  await import('../utils/env.js');
  beforeEach(restoreCredentials);
  afterEach(restoreCredentials);
}
