import { Container, ProcessTerminal, Spacer, Text, TUI, CombinedAutocompleteProvider } from '@mariozechner/pi-tui';
import type { SlashCommand, AutocompleteItem } from '@mariozechner/pi-tui';
import type {
  ApprovalDecision,
  ToolEndEvent,
  ToolErrorEvent,
  ToolStartEvent,
} from './agent/index.js';
import { checkApiKeyExists, getApiKeyNameForProvider, getProviderDisplayName } from './utils/env.js';
import { logger } from './utils/logger.js';
import {
  AgentRunnerController,
  BrowseController,
  InputHistoryController,
  ModelSelectionController,
} from './controllers/index.js';
import {
  ApiKeyInputComponent,
  ApprovalPromptComponent,
  ChatLogComponent,
  CustomEditor,
  DebugPanelComponent,
  IntroComponent,
  WorkingIndicatorComponent,
  createApiKeyConfirmSelector,
  createBrowseActionSelector,
  createBrowseMarketSelector,
  updateBrowseMarketSelector,
  createModelSelector,
  createProviderSelector,
} from './components/index.js';
import { editorTheme, theme } from './theme.js';
import { handleSlashCommand, executePendingTrade } from './commands/index.js';
import type { CommandResult } from './commands/index.js';
import { formatResponse } from './utils/markdown-table.js';
import { ensureIndex, onIndexProgress, getRefreshPromise } from './tools/kalshi/search-index.js';
import { callKalshiApi } from './tools/kalshi/api.js';
import type { KalshiMarket } from './tools/kalshi/types.js';
import { SetupWizardController } from './setup/wizard.js';
import { trackEvent } from './utils/telemetry.js';

function truncateAtWord(str: string, maxLength: number): string {
  if (str.length <= maxLength) {
    return str;
  }
  const lastSpace = str.lastIndexOf(' ', maxLength);
  if (lastSpace > maxLength * 0.5) {
    return `${str.slice(0, lastSpace)}...`;
  }
  return `${str.slice(0, maxLength)}...`;
}

function summarizeToolResult(tool: string, args: Record<string, unknown>, result: string): string {
  try {
    const parsed = JSON.parse(result);
    if (parsed.data) {
      if (Array.isArray(parsed.data)) {
        return `Received ${parsed.data.length} items`;
      }
      if (typeof parsed.data === 'object') {
        const keys = Object.keys(parsed.data).filter((key) => !key.startsWith('_'));
        if (tool === 'kalshi_search') {
          return keys.length === 1 ? 'Called 1 data source' : `Called ${keys.length} data sources`;
        }
        if (tool === 'kalshi_trade') {
          return 'Trade executed';
        }
        if (tool === 'portfolio_overview') {
          return 'Fetched portfolio';
        }
        if (tool === 'exchange_status') {
          return 'Fetched exchange status';
        }
        if (tool === 'web_search') {
          return 'Did 1 search';
        }
        return `Received ${keys.length} fields`;
      }
    }
  } catch {
    return truncateAtWord(result, 50);
  }
  return 'Received data';
}

function createScreen(
  title: string,
  description: string,
  body: any,
  footer?: string,
): Container {
  const container = new Container();
  if (title) {
    container.addChild(new Text(theme.bold(theme.primary(title)), 0, 0));
  }
  if (description) {
    container.addChild(new Text(theme.muted(description), 0, 0));
  }
  container.addChild(new Spacer(1));
  container.addChild(body);
  if (footer) {
    container.addChild(new Spacer(1));
    container.addChild(new Text(theme.muted(footer), 0, 0));
  }
  return container;
}

function renderHistory(chatLog: ChatLogComponent, history: AgentRunnerController['history']) {
  chatLog.clearAll();
  for (const item of history) {
    chatLog.addQuery(item.query);
    chatLog.resetToolGrouping();

    if (item.status === 'interrupted') {
      chatLog.addInterrupted();
    }

    for (const display of item.events) {
      const event = display.event;
      if (event.type === 'thinking') {
        const message = event.message.trim();
        if (message) {
          chatLog.addChild(
            new Text(message.length > 200 ? `${message.slice(0, 200)}...` : message, 0, 0),
          );
        }
        continue;
      }

      if (event.type === 'tool_start') {
        const toolStart = event as ToolStartEvent;
        const component = chatLog.startTool(display.id, toolStart.tool, toolStart.args);
        if (display.completed && display.endEvent?.type === 'tool_end') {
          const done = display.endEvent as ToolEndEvent;
          component.setComplete(
            summarizeToolResult(done.tool, toolStart.args, done.result),
            done.duration,
          );
        } else if (display.completed && display.endEvent?.type === 'tool_error') {
          const toolError = display.endEvent as ToolErrorEvent;
          component.setError(toolError.error);
        } else if (display.progressMessage) {
          component.setActive(display.progressMessage);
        }
        continue;
      }

      if (event.type === 'tool_approval') {
        const approval = chatLog.startTool(display.id, event.tool, event.args);
        approval.setApproval(event.approved);
        continue;
      }

      if (event.type === 'tool_denied') {
        const denied = chatLog.startTool(display.id, event.tool, event.args);
        const path = (event.args.path as string) ?? '';
        denied.setDenied(path, event.tool);
        continue;
      }

      if (event.type === 'tool_limit') {
        continue;
      }

      if (event.type === 'context_cleared') {
        chatLog.addContextCleared(event.clearedCount, event.keptCount);
      }
    }

    if (item.answer) {
      chatLog.finalizeAnswer(item.answer);
    }
    if (item.status === 'complete') {
      chatLog.addPerformanceStats(item.duration ?? 0, item.tokenUsage, item.tokensPerSecond);
    }
  }
}

export async function runCli(options?: { forceSetup?: boolean }) {
  const tui = new TUI(new ProcessTerminal());
  const root = new Container();
  const chatLog = new ChatLogComponent(tui);
  const inputHistory = new InputHistoryController(() => tui.requestRender());
  let lastError: string | null = null;
  let pendingTrade: CommandResult['pendingTrade'] | null = null;

  const onError = (message: string) => {
    lastError = message;
    logger.error(message);
    tui.requestRender();
  };

  const modelSelection = new ModelSelectionController(onError, () => {
    intro.setModel(modelSelection.model);
    renderSelectionOverlay();
    tui.requestRender();
  });

  const browseController = new BrowseController(onError, () => {
    renderSelectionOverlay();
    tui.requestRender();
  });

  // Slash command autocomplete — start with top-level themes, load subcategories in background
  const baseThemes = ['top50', 'climate', 'companies', 'crypto', 'economics', 'elections', 'entertainment', 'financials', 'health', 'mentions', 'politics', 'science', 'social', 'sports', 'transportation', 'world'];
  let allThemes = baseThemes.map((t) => ({ value: t, label: t }));

  // Pre-warm the event index on startup (non-blocking, only if credentials exist)
  let indexStatusMessage: string | null = null;
  const hasKalshiCreds = checkApiKeyExists('KALSHI_API_KEY') &&
    (checkApiKeyExists('KALSHI_PRIVATE_KEY_FILE') || checkApiKeyExists('KALSHI_PRIVATE_KEY'));
  const unsubIndexProgress = onIndexProgress((info) => {
    if (info.phase === 'fetching_events') {
      indexStatusMessage = `Indexing markets... ${info.fetchedItems} fetched (page ${info.page}/${info.maxPages})`;
    } else if (info.detail) {
      indexStatusMessage = info.detail;
    }
    tui.requestRender();
  });
  const initPostCredentials = () => {
    void ensureIndex();
    const refreshPromise = getRefreshPromise();
    if (refreshPromise) {
      void refreshPromise.catch((err) => {
        console.warn(`[warn] Background index refresh failed: ${err instanceof Error ? err.message : String(err)}`);
      }).finally(() => {
        unsubIndexProgress();
        indexStatusMessage = null;
        tui.requestRender();
      });
    }
    // Load subcategories in background
    void (async () => {
      try {
        const { fetchSubcategories, CATEGORY_MAP } = await import('./scan/theme-resolver.js');
        const labelToKey: Record<string, string> = {};
        for (const [key, label] of Object.entries(CATEGORY_MAP)) {
          labelToKey[label] = key;
        }
        const subcats = await fetchSubcategories();
        const subEntries: Array<{ value: string; label: string }> = [];
        for (const [catLabel, tags] of Object.entries(subcats)) {
          const catKey = labelToKey[catLabel];
          if (!catKey) continue;
          for (const tag of tags) {
            const kebab = tag.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
            const value = `${catKey}:${kebab}`;
            subEntries.push({ value, label: value });
          }
        }
        allThemes = [
          ...baseThemes.map((t) => ({ value: t, label: t })),
          ...subEntries,
        ];
      } catch {
        // Subcategory loading failed — keep base themes only
      }
    })();
  };

  if (hasKalshiCreds) initPostCredentials();


  const agentRunner = new AgentRunnerController(
    { model: modelSelection.model, modelProvider: modelSelection.provider, maxIterations: 10 },
    modelSelection.inMemoryChatHistory,
    () => {
      renderHistory(chatLog, agentRunner.history);
      workingIndicator.setState(agentRunner.workingState);
      renderSelectionOverlay();
      tui.requestRender();
    },
  );

  const intro = new IntroComponent(modelSelection.model);
  const errorText = new Text('', 0, 0);
  const workingIndicator = new WorkingIndicatorComponent(tui);
  const editor = new CustomEditor(tui, editorTheme);
  const debugPanel = new DebugPanelComponent(8, true);

  // Setup wizard for first-run or /setup command
  const setupWizard = new SetupWizardController(
    () => {
      renderSelectionOverlay();
      tui.requestRender();
    },
    () => {
      // On complete: trigger index + subcategory loading that was skipped at startup
      initPostCredentials();
      renderSelectionOverlay();
      tui.requestRender();
    },
  );

  const themeCompletions = (typed: string) => {
    if (!typed) return allThemes;
    const lower = typed.toLowerCase();
    return allThemes.filter((t) => t.value.toLowerCase().startsWith(lower));
  };

  const usageHint = (label: string, description: string) =>
    (prefix: string): AutocompleteItem[] | null =>
      prefix ? null : [{ value: '', label, description }];

  const portfolioSubcommands = (typed: string): AutocompleteItem[] | null => {
    const subs = [
      { value: 'positions', label: 'positions', description: 'Open positions with P&L' },
      { value: 'orders', label: 'orders', description: 'Resting orders' },
      { value: 'balance', label: 'balance', description: 'Account balance' },
      { value: 'status', label: 'status', description: 'Exchange status' },
    ];
    if (!typed) return subs;
    const lower = typed.toLowerCase();
    return subs.filter((s) => s.value.startsWith(lower));
  };

  const searchSubcommands = (typed: string): AutocompleteItem[] | null => {
    const themesItem = { value: 'themes', label: 'themes', description: 'List all available themes' };
    if (!typed) return [themesItem, ...allThemes];
    const lower = typed.toLowerCase();
    const results = [themesItem, ...allThemes].filter((t) => t.value.toLowerCase().startsWith(lower));
    return results.length > 0 ? results : null;
  };

  const watchSubcommands = (typed: string): AutocompleteItem[] | null => {
    const themeFlag = { value: '--theme', label: '--theme', description: 'Continuous theme scan (e.g. --theme crypto)' };
    if (!typed) return [{ value: '', label: '<ticker>', description: 'Live price/orderbook feed (e.g. KXBTC-26MAR14-T50049)' }, themeFlag];
    const lower = typed.toLowerCase();
    // After --theme, complete with theme names
    if (lower.startsWith('--theme ')) {
      const themeTyped = typed.slice('--theme '.length);
      const themeLower = themeTyped.toLowerCase();
      const results = allThemes
        .map((t) => ({ value: `--theme ${t.value}`, label: t.label, description: `Scan theme: ${t.value}` }))
        .filter((t) => !themeLower || t.label.toLowerCase().startsWith(themeLower));
      return results.length > 0 ? results : null;
    }
    if ('--theme'.startsWith(lower)) return [themeFlag];
    return null;
  };

  const helpTopicCompletions = (typed: string): AutocompleteItem[] | null => {
    const topics = [
      { value: 'search', label: 'search', description: 'Discovery commands' },
      { value: 'portfolio', label: 'portfolio', description: 'Account state' },
      { value: 'analyze', label: 'analyze', description: 'Market analysis' },
      { value: 'watch', label: 'watch', description: 'Live monitoring' },
      { value: 'buy', label: 'buy', description: 'Buy contracts' },
      { value: 'sell', label: 'sell', description: 'Sell contracts' },
      { value: 'cancel', label: 'cancel', description: 'Cancel an order' },
      { value: 'help', label: 'help', description: 'Show help' },
      { value: 'setup', label: 'setup', description: 'Re-run setup wizard' },
    ];
    if (!typed) return topics;
    const lower = typed.toLowerCase();
    return topics.filter((t) => t.value.startsWith(lower));
  };

  const slashCommands: SlashCommand[] = [
    // Core 6 commands
    { name: 'search', description: 'Search events by theme, ticker, or free-text (use "themes" to list)', getArgumentCompletions: searchSubcommands },
    { name: 'portfolio', description: 'Portfolio overview, positions, orders, balance, status', getArgumentCompletions: portfolioSubcommands },
    { name: 'analyze', description: 'Full market analysis: edge, research, Kelly sizing', getArgumentCompletions: usageHint('<ticker>', 'e.g. KXBTC-26MAR14-T50049') },
    { name: 'watch', description: 'Live monitoring: ticker feed or continuous theme scan', getArgumentCompletions: watchSubcommands },
    { name: 'buy', description: 'Buy contracts (defaults to YES side)', getArgumentCompletions: usageHint('<ticker> <count> [price] [yes|no]', 'e.g. KXBTC-26MAR14-T50049 10 56') },
    { name: 'sell', description: 'Sell contracts (defaults to YES side)', getArgumentCompletions: usageHint('<ticker> <count> [price] [yes|no]', 'e.g. KXBTC-26MAR14-T50049 10 56') },
    { name: 'cancel', description: 'Cancel a resting order', getArgumentCompletions: usageHint('<order_id>', 'the order UUID') },
    // Utility
    { name: 'help', description: 'Show help (/help <command> for details)', getArgumentCompletions: helpTopicCompletions },
    { name: 'model', description: 'Change LLM model/provider', getArgumentCompletions: usageHint('<provider:model>', 'e.g. anthropic:sonnet') },
    { name: 'setup', description: 'Re-run the setup wizard to configure API keys' },
    { name: 'quit', description: 'Quit CLI session' },
  ];
  editor.setAutocompleteProvider(new CombinedAutocompleteProvider(slashCommands));

  tui.addChild(root);

  const refreshError = () => {
    const message = lastError ?? agentRunner.error;
    errorText.setText(message ? theme.error(`Error: ${message}`) : '');
  };

  const handleSubmit = async (query: string) => {
    // Gracefully quit the CLI on exit or quit commands with or without leading slash
    if (query.match(/^\/?(quit|exit)$/i)) {
      tui.stop();
      process.exit(0);
      return;
    }

    // While wizard is active, Enter progresses non-component states
    if (setupWizard.isActive) {
      setupWizard.handleInput('\r');
      return;
    }

    if (modelSelection.isInSelectionFlow() || browseController.isInBrowseFlow() || agentRunner.pendingApproval || agentRunner.isProcessing) {
      return;
    }

    if (query === '/model') {
      modelSelection.startSelection();
      return;
    }

    if (query === '/setup') {
      setupWizard.start();
      return;
    }

    if (query.startsWith('/search')) {
      const themeArg = query.slice('/search'.length).trim() || 'top50';
      // /search themes → inline themes list (no browse flow)
      if (themeArg === 'themes') {
        // Handled as slash command in handleSlashCommand via 'themes' case
        chatLog.addQuery(query);
        chatLog.resetToolGrouping();
        try {
          workingIndicator.setState({ status: 'thinking' });
          tui.requestRender();
          const cmdResult = await handleSlashCommand('/themes');
          workingIndicator.setState({ status: 'idle' });
          if (cmdResult) {
            chatLog.finalizeAnswer(formatResponse(cmdResult.output));
            tui.requestRender();
          }
        } catch (err) {
          workingIndicator.setState({ status: 'idle' });
          chatLog.finalizeAnswer(`Error: ${err instanceof Error ? err.message : String(err)}`);
          tui.requestRender();
        }
        return;
      }
      browseController.startBrowse(themeArg);
      return;
    }

    // Handle pending trade confirmation (yes/no)
    if (pendingTrade) {
      const answer = query.trim().toLowerCase();
      if (answer === 'y' || answer === 'yes') {
        chatLog.addQuery(query);
        chatLog.resetToolGrouping();
        try {
          const result = await executePendingTrade(pendingTrade);
          chatLog.finalizeAnswer(result);
        } catch (err) {
          chatLog.finalizeAnswer(`Error: ${err instanceof Error ? err.message : String(err)}`);
        }
        pendingTrade = null;
        tui.requestRender();
        return;
      } else {
        trackEvent('trade_rejected', { action: pendingTrade.action, side: pendingTrade.side });
        chatLog.addQuery(query);
        chatLog.resetToolGrouping();
        chatLog.finalizeAnswer('Order canceled.');
        pendingTrade = null;
        tui.requestRender();
        return;
      }
    }

    // Handle slash commands
    if (query.startsWith('/')) {
      chatLog.addQuery(query);
      chatLog.resetToolGrouping();
      try {
        // Show loading state while slash command runs
        workingIndicator.setState({ status: 'thinking' });
        tui.requestRender();
        const cmdResult = await handleSlashCommand(query);
        workingIndicator.setState({ status: 'idle' });
        if (cmdResult !== null) {
          const formatted = formatResponse(cmdResult.output);
          chatLog.finalizeAnswer(formatted);
          if (cmdResult.pendingTrade) {
            pendingTrade = cmdResult.pendingTrade;
            chatLog.finalizeAnswer(
              formatResponse(
                `\n**Confirm order?** Type **yes** to submit or **no** to cancel.`
              )
            );
          }
          tui.requestRender();
          return;
        }
      } catch (err) {
        workingIndicator.setState({ status: 'idle' });
        chatLog.finalizeAnswer(`Error: ${err instanceof Error ? err.message : String(err)}`);
        tui.requestRender();
        return;
      }
      // Unknown slash command — fall through to agent
      workingIndicator.setState({ status: 'idle' });
    }

    await inputHistory.saveMessage(query);
    inputHistory.resetNavigation();
    const result = await agentRunner.runQuery(query);
    if (result?.answer) {
      await inputHistory.updateAgentResponse(result.answer);
    }
    refreshError();
    tui.requestRender();
  };

  editor.onSubmit = (text) => {
    const value = text.trim();
    // Allow empty Enter to progress the setup wizard
    if (setupWizard.isActive) {
      setupWizard.handleInput('\r');
      return;
    }
    if (!value) return;
    editor.setText('');
    editor.addToHistory(value);
    void handleSubmit(value);
  };

  editor.onEscape = () => {
    if (setupWizard.isActive) {
      setupWizard.handleInput('\u001b');
      return;
    }
    if (browseController.isInBrowseFlow()) {
      browseController.cancelBrowse();
      return;
    }
    if (modelSelection.isInSelectionFlow()) {
      modelSelection.cancelSelection();
      return;
    }
    if (agentRunner.isProcessing || agentRunner.pendingApproval) {
      agentRunner.cancelExecution();
      return;
    }
  };

  editor.onCtrlC = () => {
    if (setupWizard.isActive) {
      setupWizard.cancel();
      tui.stop();
      process.exit(0);
      return;
    }
    if (browseController.isInBrowseFlow()) {
      browseController.cancelBrowse();
      return;
    }
    if (modelSelection.isInSelectionFlow()) {
      modelSelection.cancelSelection();
      return;
    }
    if (agentRunner.isProcessing || agentRunner.pendingApproval) {
      agentRunner.cancelExecution();
      return;
    }
    tui.stop();
    process.exit(0);
  };

  const renderMainView = () => {
    root.clear();
    root.addChild(intro);
    if (indexStatusMessage) {
      root.addChild(new Text(theme.muted(indexStatusMessage), 0, 0));
    }
    root.addChild(chatLog);
    if (lastError ?? agentRunner.error) {
      root.addChild(errorText);
    }
    if (agentRunner.workingState.status !== 'idle') {
      root.addChild(workingIndicator);
    }
    root.addChild(new Spacer(1));
    root.addChild(editor);
    root.addChild(debugPanel);
    tui.setFocus(editor);
  };

  const renderScreenView = (
    title: string,
    description: string,
    body: any,
    footer?: string,
    focusTarget?: any,
  ) => {
    root.clear();
    root.addChild(createScreen(title, description, body, footer));
    if (focusTarget) {
      tui.setFocus(focusTarget);
    }
  };

  // Cache for browse market selector to enable in-place updates without flicker
  let cachedBrowseSelector: Container | null = null;
  let cachedBrowseTheme = '';
  let cachedBrowseEventCount = 0;

  const renderSelectionOverlay = () => {
    // Setup wizard overlay
    if (setupWizard.isActive) {
      const component = setupWizard.ensureComponent();
      const bodyLines = setupWizard.getBodyLines();
      const bodyContainer = new Container();
      if (component) {
        bodyContainer.addChild(component as any);
      }
      for (const line of bodyLines) {
        bodyContainer.addChild(new Text(line, 0, 0));
      }
      const focusTarget = setupWizard.getFocusTarget();
      renderScreenView(
        setupWizard.getTitle(),
        setupWizard.getDescription(),
        bodyContainer,
        setupWizard.getFooter(),
        focusTarget ?? editor, // editor for non-interactive states so keys route through
      );
      return;
    }

    const browseState = browseController.state;
    const state = modelSelection.state;

    // Invalidate cache when leaving event_list
    if (browseState.appState !== 'event_list') {
      cachedBrowseSelector = null;
    }

    // Check for pending recommend ticker from browse
    if (browseState.appState === 'idle' && browseState.pendingRecommendTicker) {
      const ticker = browseController.consumePendingRecommendTicker();
      if (ticker) {
        refreshError();
        renderMainView();
        // Feed the ticker into the analyze flow
        void handleSubmit(`/analyze ${ticker}`);
        return;
      }
    }

    // Check for pending trade ticker from browse
    if (browseState.appState === 'idle' && browseState.pendingTradeTicker) {
      const ticker = browseController.consumePendingTradeTicker();
      if (ticker) {
        refreshError();
        renderMainView();
        // Fetch live prices then show trade prompt
        void (async () => {
          let priceInfo = '';
          try {
            const res = await callKalshiApi('GET', `/markets/${ticker}`);
            const mkt = (res.market ?? res) as KalshiMarket;
            const yesBid = mkt.yes_bid ?? Math.round((parseFloat(mkt.yes_bid_dollars ?? mkt.dollar_yes_bid ?? '0') || 0) * 100);
            const yesAsk = mkt.yes_ask ?? Math.round((parseFloat(mkt.yes_ask_dollars ?? mkt.dollar_yes_ask ?? '0') || 0) * 100);
            const noBid = mkt.no_bid ?? (Math.round((parseFloat(mkt.no_bid_dollars ?? mkt.dollar_no_bid ?? '0') || 0) * 100) || (100 - yesAsk));
            const noAsk = mkt.no_ask ?? (Math.round((parseFloat(mkt.no_ask_dollars ?? mkt.dollar_no_ask ?? '0') || 0) * 100) || (100 - yesBid));
            priceInfo = `**Current market prices:**\n` +
              `  YES: ${yesBid}c bid / ${yesAsk}c ask\n` +
              `  NO:  ${noBid}c bid / ${noAsk}c ask\n\n`;
          } catch {
            // Skip price info on error
          }
          chatLog.finalizeAnswer(
            `Trade **${ticker}**\n\n` +
            priceInfo +
            `**Examples** (count = number of contracts):\n` +
            `  /buy ${ticker} 10        ← buy 10 YES contracts at market price\n` +
            `  /buy ${ticker} 10 no     ← buy 10 NO contracts at market price\n` +
            `  /buy ${ticker} 10 50     ← buy 10 YES contracts, limit 50c each\n` +
            `  /sell ${ticker} 10 no    ← sell 10 NO contracts at market price`);
          tui.requestRender();
        })();
        return;
      }
    }

    // Browse states
    if (browseState.appState === 'loading') {
      const loadingMsg = browseState.progressMessage ?? 'Please wait...';
      const isReport = loadingMsg.includes('report for');
      renderScreenView(
        isReport ? 'Loading Octagon Report...' : 'Search',
        isReport ? '' : `Loading events for "${browseState.theme}"...`,
        new Text(theme.muted(loadingMsg), 0, 0),
      );
      return;
    }

    if (browseState.appState === 'event_list') {
      // If the cached selector still matches, update labels in-place (no flicker)
      if (cachedBrowseSelector && cachedBrowseTheme === browseState.theme
          && cachedBrowseEventCount === browseState.events.length) {
        updateBrowseMarketSelector(cachedBrowseSelector, browseState.events);
        tui.requestRender();
        return;
      }
      const selector = createBrowseMarketSelector(
        browseState.events,
        (eventTicker, marketTicker) => browseController.selectMarket(eventTicker, marketTicker),
        () => browseController.cancelBrowse(),
        browseState.lastError,
        browseState.progressMessage,
      );
      cachedBrowseSelector = selector;
      cachedBrowseTheme = browseState.theme;
      cachedBrowseEventCount = browseState.events.length;
      const focusTarget = (selector as any)._browseList;
      renderScreenView(
        `Browse: ${browseState.theme}`,
        `${browseState.events.length} events, ${browseState.events.reduce((n, e) => n + e.markets.length, 0)} markets`,
        selector,
        'Enter to select · esc to exit',
        focusTarget,
      );
      return;
    }

    if (browseState.appState === 'view_report' && browseState.reportText) {
      const reportBody = new Text(browseState.reportText, 0, 0);
      renderScreenView(
        '',
        '',
        reportBody,
        'esc to go back',
      );
      tui.setFocus(editor);
      return;
    }

    if (browseState.appState === 'action_menu' && browseState.selectedMarket) {
      const hasReport = browseState.selectedMarket.modelProb !== null;
      const selector = createBrowseActionSelector(
        (action) => browseController.handleAction(action),
        () => browseController.handleAction('back'),
        hasReport,
        browseController.isDirectReport,
      );
      const focusTarget = (selector as any)._browseList;
      renderScreenView(
        browseState.selectedMarket.ticker,
        `${browseState.selectedMarket.title} — Mkt: ${browseState.selectedMarket.marketProb !== null ? `${(browseState.selectedMarket.marketProb * 100).toFixed(1)}%` : '—'}`,
        selector,
        'Enter to confirm · esc to go back',
        focusTarget,
      );
      return;
    }


    if (state.appState === 'idle' && !agentRunner.pendingApproval) {
      refreshError();
      renderMainView();
      return;
    }

    if (agentRunner.pendingApproval) {
      const prompt = new ApprovalPromptComponent(
        agentRunner.pendingApproval.tool,
        agentRunner.pendingApproval.args,
      );
      prompt.onSelect = (decision: ApprovalDecision) => {
        agentRunner.respondToApproval(decision);
      };
      renderScreenView('', '', prompt, undefined, prompt.selector);
      return;
    }

    if (state.appState === 'provider_select') {
      const selector = createProviderSelector(modelSelection.provider, (providerId) => {
        void modelSelection.handleProviderSelect(providerId);
      });
      renderScreenView(
        'Select provider',
        'Switch between LLM providers. Applies to this session and future sessions.',
        selector,
        'Enter to confirm · esc to exit',
        selector,
      );
      return;
    }

    if (state.appState === 'model_select' && state.pendingProvider) {
      const selector = createModelSelector(
        state.pendingModels,
        modelSelection.provider === state.pendingProvider ? modelSelection.model : undefined,
        (modelId) => modelSelection.handleModelSelect(modelId),
        state.pendingProvider,
      );
      renderScreenView(
        `Select model for ${getProviderDisplayName(state.pendingProvider)}`,
        '',
        selector,
        'Enter to confirm · esc to go back',
        selector,
      );
      return;
    }

    if (state.appState === 'model_input' && state.pendingProvider) {
      const input = new ApiKeyInputComponent();
      input.onSubmit = (value) => modelSelection.handleModelInputSubmit(value);
      input.onCancel = () => modelSelection.handleModelInputSubmit(null);
      renderScreenView(
        `Enter model name for ${getProviderDisplayName(state.pendingProvider)}`,
        'Type or paste the model name from openrouter.ai/models',
        input,
        'Examples: anthropic/claude-3.5-sonnet, openai/gpt-4-turbo, meta-llama/llama-3-70b\nEnter to confirm · esc to go back',
        input,
      );
      return;
    }

    if (state.appState === 'api_key_confirm' && state.pendingProvider) {
      const selector = createApiKeyConfirmSelector((wantsToSet) =>
        modelSelection.handleApiKeyConfirm(wantsToSet),
      );
      renderScreenView(
        'Set API Key',
        `Would you like to set your ${getProviderDisplayName(state.pendingProvider)} API key?`,
        selector,
        'Enter to confirm · esc to decline',
        selector,
      );
      return;
    }

    if (state.appState === 'api_key_input' && state.pendingProvider) {
      const input = new ApiKeyInputComponent(true);
      input.onSubmit = (apiKey) => modelSelection.handleApiKeySubmit(apiKey);
      input.onCancel = () => modelSelection.handleApiKeySubmit(null);
      const apiKeyName = getApiKeyNameForProvider(state.pendingProvider) ?? '';
      renderScreenView(
        `Enter ${getProviderDisplayName(state.pendingProvider)} API Key`,
        apiKeyName ? `(${apiKeyName})` : '',
        input,
        'Enter to confirm · Esc to cancel',
        input,
      );
    }
  };

  await inputHistory.init();
  for (const msg of inputHistory.getMessages().reverse()) {
    editor.addToHistory(msg);
  }

  // Auto-launch setup wizard if credentials are missing or `bun start init` was used
  if (!hasKalshiCreds || options?.forceSetup) {
    setupWizard.start();
  }

  renderSelectionOverlay();
  refreshError();

  tui.start();
  await new Promise<void>((resolve) => {
    const finish = () => resolve();
    process.once('exit', finish);
    process.once('SIGINT', finish);
    process.once('SIGTERM', finish);
  });

  workingIndicator.dispose();
  debugPanel.dispose();
}
