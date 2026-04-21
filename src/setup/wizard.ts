import { existsSync } from 'fs';
import { config } from 'dotenv';
import { ApiKeyInputComponent, createProviderSelector } from '../components/index.js';
import { VimSelectList } from '../components/select-list.js';
import { selectListTheme, theme } from '../theme.js';
import { checkApiKeyExists, saveApiKeyToEnv, ENV_PATH } from '../utils/env.js';
import { callKalshiApi } from '../tools/kalshi/api.js';
import { loadBotConfig, saveBotConfig } from '../utils/bot-config.js';
import { appPath } from '../utils/paths.js';
import type { SelectItem } from '@mariozechner/pi-tui';

export type WizardState =
  | 'welcome'
  | 'kalshi_api_key'
  | 'kalshi_private_key'
  | 'octagon_api_key'
  | 'llm_provider_select'
  | 'llm_api_key'
  | 'testing'
  | 'complete';

interface TestResult {
  name: string;
  status: 'pending' | 'ok' | 'fail' | 'skip';
  message?: string;
}

export class SetupWizardController {
  private wizardState: WizardState = 'welcome';
  private collectedKeys: Record<string, string> = {};
  private originalEnvValues: Record<string, string | undefined> = {};
  private testResults: TestResult[] = [];
  private configWritten = false;
  private selectedProvider: string | null = null;
  private readonly onComplete: () => void;
  private readonly onChange: () => void;
  private active = false;
  private stepError: string | null = null;

  // Reusable UI components for the current step
  private currentInput: ApiKeyInputComponent | null = null;
  private currentSelector: VimSelectList | null = null;

  constructor(onChange: () => void, onComplete: () => void) {
    this.onChange = onChange;
    this.onComplete = onComplete;
  }

  get state(): WizardState {
    return this.wizardState;
  }

  get isActive(): boolean {
    return this.active;
  }

  start() {
    this.active = true;
    this.wizardState = 'welcome';
    this.collectedKeys = {};
    this.originalEnvValues = {};
    this.testResults = [];
    this.configWritten = false;
    this.selectedProvider = null;
    this.currentInput = null;
    this.currentSelector = null;
    this.onChange();
  }

  cancel() {
    this.restoreStagedEnv();
    this.active = false;
    this.wizardState = 'welcome';
    this.currentInput = null;
    this.currentSelector = null;
    this.onChange();
  }

  /** Snapshot and stage an env var — records original value for cancel/restore */
  private stageEnv(key: string, value: string) {
    if (!(key in this.originalEnvValues)) {
      this.originalEnvValues[key] = process.env[key];
    }
    this.collectedKeys[key] = value;
    process.env[key] = value;
  }

  /** Restore all staged env vars to their original values and clear collected keys */
  private restoreStagedEnv() {
    for (const key of Object.keys(this.collectedKeys)) {
      const original = this.originalEnvValues[key];
      if (original !== undefined) {
        process.env[key] = original;
      } else {
        delete process.env[key];
      }
    }
    this.collectedKeys = {};
    this.originalEnvValues = {};
  }

  // --- Rendering info for cli.ts ---

  getTitle(): string {
    switch (this.wizardState) {
      case 'welcome':
        return 'Welcome to Kalshi Trading Bot CLI';
      case 'kalshi_api_key':
        return 'Step 1/5: Kalshi API Key';
      case 'kalshi_private_key':
        return 'Step 2/5: Kalshi Private Key';
      case 'octagon_api_key':
        return 'Step 3/5: Octagon API Key';
      case 'llm_provider_select':
        return 'Step 4/5: LLM Provider';
      case 'llm_api_key':
        return `Step 5/5: ${this.selectedProvider ?? 'LLM'} API Key`;
      case 'testing':
        return 'Testing connections...';
      case 'complete':
        return "You're all set!";
    }
  }

  getDescription(): string {
    switch (this.wizardState) {
      case 'welcome':
        return "Let's get you set up. This takes ~2 minutes.\nYou'll need your Kalshi API credentials and at least one LLM API key.";
      case 'kalshi_api_key':
        return 'Paste your Kalshi API key below.\nGet one at: https://kalshi.com/account/api';
      case 'kalshi_private_key': {
        let desc = 'Paste your Kalshi private key below.\nCopy it from the Kalshi API key creation screen.\nYou can also enter a path to a .pem file.';
        if (this.stepError) desc += `\n\n${this.stepError}`;
        return desc;
      }
      case 'octagon_api_key':
        return 'Paste your Octagon API key (recommended for deep research).\nGet one at: https://app.octagonai.co\nLeave empty and press Enter to skip.';
      case 'llm_provider_select':
        return 'Select your LLM provider. You can change this later with /model.';
      case 'llm_api_key':
        return `Paste your ${this.selectedProvider ?? 'LLM'} API key below.`;
      case 'testing':
        return '';
      case 'complete':
        return this.configWritten
          ? 'All keys saved to .env. Default thresholds written to config.json.'
          : 'All keys saved to .env. Type /help to get started.';
    }
  }

  getFooter(): string {
    switch (this.wizardState) {
      case 'welcome':
        return 'Enter to continue';
      case 'kalshi_api_key':
      case 'kalshi_private_key':
      case 'octagon_api_key':
      case 'llm_api_key':
        return 'Enter to confirm · Esc to cancel setup';
      case 'llm_provider_select':
        return 'Enter to confirm · Esc to cancel setup';
      case 'testing':
        return '';
      case 'complete':
        if (this.testResults.some((r) => r.status === 'fail')) {
          return 'R to restart wizard · Enter to continue anyway';
        }
        return 'Press Enter to continue';
    }
  }

  /** Returns the component that should receive focus, or null for text-only states */
  getFocusTarget(): ApiKeyInputComponent | VimSelectList | null {
    if (this.wizardState === 'llm_provider_select' && this.currentSelector) {
      return this.currentSelector;
    }
    if (this.currentInput) {
      return this.currentInput;
    }
    return null;
  }

  /** Returns extra body lines for states without an interactive component */
  getBodyLines(): string[] {
    if (this.wizardState === 'testing') {
      return this.testResults.map((r) => {
        const icon =
          r.status === 'ok'   ? theme.success('  OK') :
          r.status === 'fail' ? theme.error('  FAIL') :
          r.status === 'skip' ? theme.muted('  --') :
          theme.muted('  ...');
        const msg = r.message ? theme.muted(` ${r.message}`) : '';
        return `${icon}  ${r.name}${msg}`;
      });
    }
    if (this.wizardState === 'complete') {
      const lines = this.testResults.map((r) => {
        const icon = r.status === 'ok' ? theme.success('  OK') : r.status === 'skip' ? theme.muted('  --') : theme.error('  FAIL');
        const msg = r.message ? theme.muted(` ${r.message}`) : '';
        return `${icon}  ${r.name}${msg}`;
      });
      if (this.configWritten) {
        lines.push('');
        lines.push(theme.muted('  Default thresholds (to customize, run the command shown):'));
        lines.push(`    min_edge_threshold  = 5%     ${theme.muted('e.g. kalshi config risk.min_edge_threshold 0.10')}`);
        lines.push(`    kelly_multiplier    = 0.5    ${theme.muted('e.g. kalshi config risk.kelly_multiplier 0.25')}`);
        lines.push(`    max_position_pct    = 10%    ${theme.muted('e.g. kalshi config risk.max_position_pct 0.05')}`);
        lines.push(`    daily_loss_limit    = $200   ${theme.muted('e.g. kalshi config risk.daily_loss_limit 100')}`);
        lines.push(`    max_positions       = 10     ${theme.muted('e.g. kalshi config risk.max_positions 5')}`);
        lines.push('');
        lines.push(theme.muted('  Run "kalshi config" to see all settings.'));
      }
      return lines;
    }
    return [];
  }

  /** Create the input/selector component for the current step (called by cli.ts during render) */
  ensureComponent(): ApiKeyInputComponent | VimSelectList | null {
    switch (this.wizardState) {
      case 'kalshi_api_key': {
        if (!this.currentInput) {
          const input = new ApiKeyInputComponent(true);
          input.onSubmit = (value) => this.handleApiKeySubmit('KALSHI_API_KEY', value, 'kalshi_private_key');
          input.onCancel = () => this.cancel();
          this.currentInput = input;
        }
        return this.currentInput;
      }
      case 'kalshi_private_key': {
        if (!this.currentInput) {
          const input = new ApiKeyInputComponent(true); // Masked — it's a private key
          input.onSubmit = (value) => this.handlePrivateKeySubmit(value);
          input.onCancel = () => this.cancel();
          this.currentInput = input;
        }
        return this.currentInput;
      }
      case 'octagon_api_key': {
        if (!this.currentInput) {
          const input = new ApiKeyInputComponent(true);
          input.onSubmit = (value) => this.handleOptionalKeySubmit('OCTAGON_API_KEY', value, 'llm_provider_select');
          input.onCancel = () => this.cancel();
          this.currentInput = input;
        }
        return this.currentInput;
      }
      case 'llm_provider_select': {
        if (!this.currentSelector) {
          const items: SelectItem[] = [
            { value: 'openai', label: '1. OpenAI' },
            { value: 'anthropic', label: '2. Anthropic' },
            { value: 'google', label: '3. Google' },
            { value: 'xai', label: '4. xAI' },
            { value: 'deepseek', label: '5. DeepSeek' },
            { value: 'openrouter', label: '6. OpenRouter' },
            { value: 'ollama', label: '7. Ollama (local, no key needed)' },
            { value: 'skip', label: '8. Skip (set up later with /model)' },
          ];
          const list = new VimSelectList(items, 10, selectListTheme);
          list.onSelect = (item) => this.handleProviderSelect(item.value);
          list.onCancel = () => this.cancel();
          this.currentSelector = list;
        }
        return this.currentSelector;
      }
      case 'llm_api_key': {
        if (!this.currentInput) {
          const input = new ApiKeyInputComponent(true);
          input.onSubmit = (value) => this.handleLlmApiKeySubmit(value);
          input.onCancel = () => this.cancel();
          this.currentInput = input;
        }
        return this.currentInput;
      }
      default:
        return null;
    }
  }

  /** Handle keyboard input for non-component states (welcome, testing, complete) */
  handleInput(keyData: string): void {
    if (keyData === '\r') {
      if (this.wizardState === 'welcome') {
        this.transition('kalshi_api_key');
        return;
      }
      if (this.wizardState === 'complete') {
        const failed = this.flushKeysToEnv();
        if (failed.length > 0) {
          this.testResults.push(...failed.map((k) => ({ name: `Save ${k}`, status: 'fail' as const, message: 'Failed to write to .env' })));
          this.onChange();
          return;
        }
        this.active = false;
        this.onComplete();
        return;
      }
    }
    if ((keyData === 'r' || keyData === 'R') && this.wizardState === 'complete') {
      if (this.testResults.some((r) => r.status === 'fail')) {
        this.restoreStagedEnv();
        this.testResults = [];
        this.transition('kalshi_api_key');
        return;
      }
    }
    if (keyData === '\u001b') {
      // Esc
      if (this.wizardState === 'welcome' || this.wizardState === 'complete') {
        if (this.wizardState === 'complete') {
          const failed = this.flushKeysToEnv();
          if (failed.length > 0) {
            this.testResults.push(...failed.map((k) => ({ name: `Save ${k}`, status: 'fail' as const, message: 'Failed to write to .env' })));
            this.onChange();
            return;
          }
          this.active = false;
          this.onComplete();
        } else {
          this.cancel();
        }
      }
    }
  }

  /** Persist all collected keys to .env — called only when the user confirms completion.
   *  Returns list of keys that failed to persist (empty on full success). */
  private flushKeysToEnv(): string[] {
    const failed: string[] = [];
    for (const [key, value] of Object.entries(this.collectedKeys)) {
      if (!saveApiKeyToEnv(key, value)) {
        failed.push(key);
      }
    }
    return failed;
  }

  // --- Internal state transitions ---

  private transition(next: WizardState) {
    this.wizardState = next;
    this.stepError = null;
    this.currentInput = null;
    this.currentSelector = null;
    this.onChange();
  }

  private handleApiKeySubmit(envName: string, value: string | null, nextState: WizardState) {
    if (!value) {
      // Required key — don't advance
      return;
    }
    this.stageEnv(envName, value);
    this.transition(nextState);
  }

  private handlePrivateKeySubmit(value: string | null) {
    if (!value) return; // Required

    const trimmed = value.trim();

    // Check if it's a file path
    if (trimmed.endsWith('.pem') || trimmed.startsWith('/') || trimmed.startsWith('~') || trimmed.startsWith('.')) {
      // Expand ~ to home
      const expanded = trimmed.startsWith('~')
        ? trimmed.replace('~', process.env.HOME ?? '')
        : trimmed;

      if (!existsSync(expanded)) {
        this.stepError = `File not found: ${expanded}`;
        this.onChange();
        return;
      }
      this.stageEnv('KALSHI_PRIVATE_KEY_FILE', expanded);
    } else {
      // Raw PEM content pasted — the single-line input strips newlines,
      // so reconstruct PEM structure: header, base64 body in 64-char lines, footer
      let pem = trimmed;
      const pemHeaderRe = /^(-----BEGIN [A-Z ]+-----)(.*?)(-----END [A-Z ]+-----)$/;
      const match = pem.match(pemHeaderRe);
      if (!match) {
        this.stepError = 'Invalid private key. Expected PEM format starting with -----BEGIN RSA PRIVATE KEY-----';
        this.onChange();
        return;
      }
      if (match) {
        const header = match[1];
        const body = match[2].replace(/\s+/g, '');
        const footer = match[3];
        // Split base64 body into 64-character lines (standard PEM format)
        const bodyLines: string[] = [];
        for (let i = 0; i < body.length; i += 64) {
          bodyLines.push(body.slice(i, i + 64));
        }
        pem = [header, ...bodyLines, footer].join('\n');
      }
      // Encode newlines for .env compatibility — dotenv expands \n in double-quoted values
      const encoded = `"${pem.replace(/\n/g, '\\n')}"`;
      this.collectedKeys['KALSHI_PRIVATE_KEY'] = encoded;
      // Store actual PEM (with real newlines) in process.env so API clients can use it directly
      if (!(('KALSHI_PRIVATE_KEY') in this.originalEnvValues)) {
        this.originalEnvValues['KALSHI_PRIVATE_KEY'] = process.env['KALSHI_PRIVATE_KEY'];
      }
      process.env['KALSHI_PRIVATE_KEY'] = pem;
    }

    // Only set KALSHI_USE_DEMO default if not already configured
    if (!process.env.KALSHI_USE_DEMO) {
      this.stageEnv('KALSHI_USE_DEMO', 'false');
    }
    this.transition('octagon_api_key');
  }

  private handleOptionalKeySubmit(envName: string, value: string | null, nextState: WizardState) {
    if (value) {
      this.stageEnv(envName, value);
    }
    this.transition(nextState);
  }

  private readonly providerEnvMap: Record<string, string> = {
    openai: 'OPENAI_API_KEY',
    anthropic: 'ANTHROPIC_API_KEY',
    google: 'GOOGLE_API_KEY',
    xai: 'XAI_API_KEY',
    deepseek: 'DEEPSEEK_API_KEY',
    openrouter: 'OPENROUTER_API_KEY',
    moonshot: 'MOONSHOT_API_KEY',
  };

  private handleProviderSelect(providerId: string) {
    if (providerId === 'skip') {
      this.selectedProvider = null;
      this.runTests().catch((err) => {
        this.testResults = [{ name: 'Setup error', status: 'fail', message: String(err) }];
        this.wizardState = 'complete';
        this.onChange();
      });
      return;
    }
    if (providerId === 'ollama') {
      // Ollama runs locally — no API key needed, but track the selection
      this.selectedProvider = 'ollama';
      this.runTests().catch((err) => {
        this.testResults = [{ name: 'Setup error', status: 'fail', message: String(err) }];
        this.wizardState = 'complete';
        this.onChange();
      });
      return;
    }
    this.selectedProvider = providerId;
    this.transition('llm_api_key');
  }

  private handleLlmApiKeySubmit(value: string | null) {
    if (!value || !value.trim()) {
      // Empty submission — treat as skip
      this.selectedProvider = null;
      this.runTests().catch((err) => {
        this.testResults = [{ name: 'Setup error', status: 'fail', message: String(err) }];
        this.wizardState = 'complete';
        this.onChange();
      });
      return;
    }
    if (this.selectedProvider) {
      const envName = this.providerEnvMap[this.selectedProvider];
      if (envName) {
        this.stageEnv(envName, value);
      }
    }
    this.runTests().catch((err) => {
      this.testResults = [{ name: 'Setup error', status: 'fail', message: String(err) }];
      this.wizardState = 'complete';
      this.onChange();
    });
  }

  /** Map provider id → base URL for /models endpoint test */
  private readonly providerBaseUrlMap: Record<string, string> = {
    openai: 'https://api.openai.com/v1',
    xai: 'https://api.x.ai/v1',
    openrouter: 'https://openrouter.ai/api/v1',
    moonshot: 'https://api.moonshot.cn/v1',
    deepseek: 'https://api.deepseek.com',
  };

  /** Test an API key by hitting a lightweight endpoint */
  private async testBearerKey(baseUrl: string, apiKey: string): Promise<void> {
    const res = await fetch(`${baseUrl}/models`, {
      headers: { Authorization: `Bearer ${apiKey}` },
      signal: AbortSignal.timeout(10_000),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`${res.status} ${text.slice(0, 80)}`);
    }
  }

  private async runTests() {
    this.testResults = [
      { name: 'Kalshi API', status: 'pending' },
      { name: 'Octagon API', status: 'pending' },
      { name: 'LLM API', status: 'pending' },
    ];
    this.transition('testing');

    // Reload env from .env (non-overwriting so staged process.env values are preserved)
    config({ path: ENV_PATH, quiet: true });

    // Test Kalshi
    try {
      await callKalshiApi('GET', '/exchange/status');
      this.testResults[0] = { name: 'Kalshi API', status: 'ok', message: 'Connected' };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.testResults[0] = { name: 'Kalshi API', status: 'fail', message: msg.slice(0, 60) };
    }
    this.onChange();

    // Test Octagon
    const octagonKey = process.env.OCTAGON_API_KEY;
    if (octagonKey) {
      try {
        const octagonBase = process.env.OCTAGON_BASE_URL ?? 'https://api-gateway.octagonagents.com/v1';
        const res = await fetch(`${octagonBase}/models`, {
          headers: { Authorization: `Bearer ${octagonKey}` },
          signal: AbortSignal.timeout(10_000),
        });
        if (res.ok || res.status === 404) {
          // 404 is fine — key is valid, endpoint just doesn't exist
          this.testResults[1] = { name: 'Octagon API', status: 'ok', message: 'Connected' };
        } else if (res.status === 401 || res.status === 403) {
          this.testResults[1] = { name: 'Octagon API', status: 'fail', message: 'Invalid API key' };
        } else {
          this.testResults[1] = { name: 'Octagon API', status: 'fail', message: `HTTP ${res.status}` };
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        this.testResults[1] = { name: 'Octagon API', status: 'fail', message: msg.slice(0, 60) };
      }
    } else {
      this.testResults[1] = { name: 'Octagon API', status: 'skip', message: 'Skipped (set later in .env)' };
    }
    this.onChange();

    // Test LLM
    if (this.selectedProvider === 'ollama') {
      try {
        const ollamaBase = process.env.OLLAMA_BASE_URL ?? 'http://localhost:11434';
        const res = await fetch(`${ollamaBase}/api/tags`, {
          signal: AbortSignal.timeout(5_000),
        });
        if (res.ok) {
          this.testResults[2] = { name: 'LLM API', status: 'ok', message: 'Ollama connected' };
        } else {
          this.testResults[2] = { name: 'LLM API', status: 'fail', message: `Ollama returned ${res.status}` };
        }
      } catch {
        this.testResults[2] = { name: 'LLM API', status: 'fail', message: 'Ollama not reachable at localhost:11434' };
      }
    } else if (this.selectedProvider === 'anthropic') {
      // Anthropic doesn't have a /models endpoint — test with a minimal messages call
      const apiKey = process.env.ANTHROPIC_API_KEY;
      if (apiKey) {
        try {
          const res = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
              'x-api-key': apiKey,
              'anthropic-version': '2023-06-01',
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              model: process.env.ANTHROPIC_TEST_MODEL ?? 'claude-haiku-4-5-20251001',
              max_tokens: 1,
              messages: [{ role: 'user', content: 'hi' }],
            }),
            signal: AbortSignal.timeout(10_000),
          });
          if (res.ok) {
            this.testResults[2] = { name: 'LLM API', status: 'ok', message: 'Anthropic connected' };
          } else if (res.status === 401) {
            this.testResults[2] = { name: 'LLM API', status: 'fail', message: 'Invalid API key' };
          } else {
            // 400, 429, etc. still means the key authenticated
            this.testResults[2] = { name: 'LLM API', status: 'ok', message: 'Anthropic key valid' };
          }
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          this.testResults[2] = { name: 'LLM API', status: 'fail', message: msg.slice(0, 60) };
        }
      } else {
        this.testResults[2] = { name: 'LLM API', status: 'fail', message: 'Key not found' };
      }
    } else if (this.selectedProvider === 'google') {
      // Google Gemini uses API key as query param
      const apiKey = process.env.GOOGLE_API_KEY;
      if (apiKey) {
        try {
          const res = await fetch(`https://generativelanguage.googleapis.com/v1/models?key=${apiKey}`, {
            signal: AbortSignal.timeout(10_000),
          });
          if (res.ok) {
            this.testResults[2] = { name: 'LLM API', status: 'ok', message: 'Google connected' };
          } else if (res.status === 400 || res.status === 403) {
            this.testResults[2] = { name: 'LLM API', status: 'fail', message: 'Invalid API key' };
          } else {
            this.testResults[2] = { name: 'LLM API', status: 'fail', message: `HTTP ${res.status}` };
          }
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          this.testResults[2] = { name: 'LLM API', status: 'fail', message: msg.slice(0, 60) };
        }
      } else {
        this.testResults[2] = { name: 'LLM API', status: 'fail', message: 'Key not found' };
      }
    } else if (this.selectedProvider) {
      // OpenAI-compatible providers: openai, xai, openrouter, moonshot, deepseek
      const envName = this.providerEnvMap[this.selectedProvider];
      const apiKey = envName ? process.env[envName] : undefined;
      const baseUrl = this.providerBaseUrlMap[this.selectedProvider];
      if (apiKey && baseUrl) {
        try {
          await this.testBearerKey(baseUrl, apiKey);
          this.testResults[2] = { name: 'LLM API', status: 'ok', message: `${this.selectedProvider} connected` };
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          if (msg.includes('401') || msg.includes('403')) {
            this.testResults[2] = { name: 'LLM API', status: 'fail', message: 'Invalid API key' };
          } else {
            this.testResults[2] = { name: 'LLM API', status: 'fail', message: msg.slice(0, 60) };
          }
        }
      } else {
        this.testResults[2] = { name: 'LLM API', status: 'fail', message: 'Key not found' };
      }
    } else {
      this.testResults[2] = { name: 'LLM API', status: 'skip', message: 'Skipped (use /model to set up)' };
    }
    this.onChange();

    // Small delay so user can see results
    await new Promise((r) => setTimeout(r, 800));

    // Write default config.json if it doesn't exist yet
    if (!existsSync(appPath('config.json'))) {
      const defaults = loadBotConfig(); // returns DEFAULTS when no file exists
      this.configWritten = saveBotConfig(defaults);
      if (!this.configWritten) {
        this.testResults.push({ name: 'Write config.json', status: 'fail', message: `Could not write to ${appPath('config.json')}` });
      }
    }

    this.wizardState = 'complete';
    this.onChange();
  }
}
