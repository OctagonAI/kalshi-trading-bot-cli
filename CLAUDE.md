# CLAUDE.md

## Workflow

- Always commit and push after each change.
- Before opening a PR, ask whether it should bump the `package.json` version (patch, minor or
  major). Releases are cut only when a version change lands on `main` — `release-tag.yml`
  tags it and publishes to npm — so a PR merged without a bump never ships.
- When a command's flags or signature changes, update ALL of these:
  - `src/commands/parse-args.ts` — flag parsing and `ParsedArgs` interface
  - `src/commands/help.ts` — detailed help topic and overview section
  - `src/commands/index.ts` — TUI slash command handler and `defaultArgs()`
  - `src/commands/dispatch.ts` — CLI dispatch block
  - `src/cli.ts` — autocomplete `slashCommands` array
  - `src/components/intro.ts` — welcome screen command list
  - `README.md` — commands table, flags table, and examples
  - `src/__tests__/e2e.test.ts` — `makeParsedArgs()` defaults
  - `src/gateway/commands/handler.ts` — `makeArgs()` defaults
