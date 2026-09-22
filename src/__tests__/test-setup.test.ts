import { describe, test, expect } from 'bun:test';
import { CREDENTIAL_BASELINE } from './test-setup.js';

describe('test-setup preload', () => {
  test('every credential is its test placeholder, whatever the shell or .env holds', () => {
    for (const [name, value] of Object.entries(CREDENTIAL_BASELINE)) {
      expect(process.env[name]).toBe(value);
    }
  });

  test('a test that clears a key does not leak into the next test', () => {
    process.env.OCTAGON_API_KEY = '';
  });

  test('…because the baseline is restored around every test', () => {
    expect(process.env.OCTAGON_API_KEY).toBe(CREDENTIAL_BASELINE.OCTAGON_API_KEY);
  });
});
