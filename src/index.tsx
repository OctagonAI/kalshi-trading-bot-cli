#!/usr/bin/env bun
import { config } from 'dotenv';
import { runCli } from './cli.js';
import { parseArgs } from './commands/parse-args.js';
import { dispatch } from './commands/dispatch.js';
import { initTelemetry, trackEvent, shutdownTelemetry } from './utils/telemetry.js';
import packageJson from '../package.json';

// Load environment variables
config({ quiet: true });

const parsed = parseArgs();

await initTelemetry();
trackEvent('app_start', {
  mode: parsed.subcommand === 'chat' || parsed.subcommand === 'init' ? 'tui' : 'cli',
  command: parsed.subcommand,
  version: packageJson.version,
});

if (parsed.subcommand === 'chat') {
  await runCli();
  await shutdownTelemetry();
} else if (parsed.subcommand === 'init') {
  await runCli({ forceSetup: true });
  await shutdownTelemetry();
} else {
  await dispatch(parsed);
  await shutdownTelemetry();
}
