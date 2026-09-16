import { Container, Text, type SelectItem } from '@mariozechner/pi-tui';
import { VimSelectList } from './select-list.js';
import { selectListTheme, theme } from '../theme.js';
import type { BrowseEventRow, BrowseMarketRow } from '../controllers/browse.js';

function pad(s: string, len: number): string {
  return s.length > len ? s.slice(0, len - 1) + '…' : s.padEnd(len);
}

function fmtPct(val: number | null): string {
  if (val === null) return '--';
  return `${(val * 100).toFixed(1)}%`;
}

/**
 * A contract's own identity: the market ticker minus its event prefix.
 * `KXBTCD-33APR0610-T59599.99` → `T59599.99`.
 *
 * Kept local rather than imported from the command layer — this is three lines,
 * and browse-list.ts is a TUI component that otherwise pulls in nothing from
 * `commands/`.
 */
function contractOf(marketTicker: string, eventTicker: string): string {
  const prefix = `${eventTicker}-`;
  return marketTicker.startsWith(prefix) ? marketTicker.slice(prefix.length) : marketTicker;
}

/**
 * Within one event, show whichever of label/title actually varies.
 *
 * A Kalshi strike ladder carries one title on all 188 markets and differs only
 * by label, so the old Title column showed the same string on every row while
 * the 26-char ticker was cut to 19 — losing the strike digits too.
 */
function describeMarket(m: BrowseMarketRow, labelsVary: boolean): string {
  if (labelsVary && m.label) return m.label;
  return m.title;
}

function buildMarketItems(events: BrowseEventRow[]): SelectItem[] {
  const items: SelectItem[] = [];
  for (const ev of events) {
    const labelsVary = new Set(ev.markets.map((m) => m.label ?? '')).size > 1;
    for (const m of ev.markets) {
      const contract = pad(contractOf(m.ticker, ev.eventTicker), 14);
      const what = pad(describeMarket(m, labelsVary), 40);
      const mktPct = pad(fmtPct(m.marketProb), 7);
      const isPending = ev.pending === true;
      const modelPct = pad(isPending && m.modelProb === null ? '...' : fmtPct(m.modelProb), 7);
      const edgeStr = pad(isPending && m.edge === null ? '...' : (m.edge !== null ? `${m.edge > 0 ? '+' : ''}${(m.edge * 100).toFixed(1)}%` : '--'), 7);
      const conf = pad(isPending && m.confidence === null ? 'pending' : (m.confidence ?? '--'), 8);

      items.push({
        value: JSON.stringify({ eventTicker: ev.eventTicker, marketTicker: m.ticker }),
        label: `${contract} ${what} ${mktPct} ${modelPct} ${edgeStr} ${conf}`,
      });
    }
  }
  return items;
}

/** Update an existing browse selector's item labels in-place (preserves scroll/selection). */
export function updateBrowseMarketSelector(
  container: Container,
  events: BrowseEventRow[],
): void {
  const list = (container as any)._browseList as VimSelectList | undefined;
  if (!list) return;
  const newItems = buildMarketItems(events);
  // Access private arrays via any cast to update labels without recreating the list
  const items = (list as any).items as SelectItem[];
  const filtered = (list as any).filteredItems as SelectItem[];
  for (let i = 0; i < items.length && i < newItems.length; i++) {
    items[i].label = newItems[i].label;
  }
  for (let i = 0; i < filtered.length && i < newItems.length; i++) {
    filtered[i].label = newItems[i].label;
  }
}

export function createBrowseMarketSelector(
  events: BrowseEventRow[],
  onSelect: (eventTicker: string, marketTicker: string) => void,
  onCancel: () => void,
  errorMessage?: string | null,
  progressMessage?: string | null,
): Container {
  const items = buildMarketItems(events);

  const container = new Container();

  // Progress message (shown above header)
  if (progressMessage) {
    container.addChild(new Text(theme.muted(progressMessage), 0, 0));
  }

  // Error message (shown above header so it's always visible)
  if (errorMessage) {
    container.addChild(new Text(theme.bold(theme.warning(errorMessage)), 0, 0));
  }

  // Header row
  const header = `${pad('Contract', 14)} ${pad('Strike / Title', 40)} ${pad('Mkt %', 7)} ${pad('Model%', 7)} ${pad('Edge', 7)} ${pad('Conf', 8)}`;
  container.addChild(new Text(theme.muted(header), 0, 0));

  if (items.length === 0) {
    container.addChild(new Text(theme.muted('No markets found.'), 0, 0));
    container.addChild(new Text(theme.muted('esc to go back'), 0, 0));
    return container;
  }

  const list = new VimSelectList(items, Math.min(items.length, 20), selectListTheme);
  list.onSelect = (item) => {
    let parsed: { eventTicker: string; marketTicker: string };
    try {
      parsed = JSON.parse(item.value);
    } catch {
      return;
    }
    onSelect(parsed.eventTicker, parsed.marketTicker);
  };
  list.onCancel = () => onCancel();
  container.addChild(list);

  // Store list reference for focus
  (container as any)._browseList = list;

  return container;
}

/**
 * Event-level list: one row per event, not one per market.
 *
 * The list used to flatten every event's markets into sibling rows, so an event
 * with 40 outcomes filled the screen and there was no way to open it. Rows
 * arrive already sorted by total market volume.
 */
export function createBrowseEventSelector(
  events: BrowseEventRow[],
  onSelect: (eventTicker: string) => void,
  onCancel: () => void,
  errorMessage?: string | null,
  progressMessage?: string | null,
): Container {
  const container = new Container();

  if (progressMessage) {
    container.addChild(new Text(theme.muted(progressMessage), 0, 0));
  }
  if (errorMessage) {
    container.addChild(new Text(theme.bold(theme.warning(errorMessage)), 0, 0));
  }

  const header = `${pad('Event', 24)} ${pad('Title', 50)} ${pad('Mkts', 5)} ${pad('Category', 18)}`;
  container.addChild(new Text(theme.muted(header), 0, 0));

  if (events.length === 0) {
    container.addChild(new Text(theme.muted('No events found.'), 0, 0));
    container.addChild(new Text(theme.muted('esc to go back'), 0, 0));
    return container;
  }

  const items: SelectItem[] = events.map((ev) => ({
    value: ev.eventTicker,
    label: `${pad(ev.eventTicker, 24)} ${pad(ev.title, 50)} ${pad(String(ev.markets.length), 5)} ${pad(ev.category || '-', 18)}`,
  }));

  const list = new VimSelectList(items, Math.min(items.length, 20), selectListTheme);
  list.onSelect = (item) => onSelect(item.value);
  list.onCancel = () => onCancel();
  container.addChild(list);
  (container as any)._browseList = list;

  return container;
}

export function createBrowseActionSelector(
  onSelect: (action: string) => void,
  onCancel: () => void,
  hasReport = true,
  directMode = false,
): Container {
  const items: SelectItem[] = [];
  let n = 1;
  if (hasReport) {
    items.push({ value: 'view_report', label: `${n++}. View research report` });
  } else {
    items.push({ value: 'no_report', label: theme.muted(`${n++}. No cached report available`) });
  }
  items.push({ value: 'refresh', label: `${n++}. Refresh this research report (costs credits)` });
  if (!directMode) {
    items.push({ value: 'refresh_all', label: `${n++}. Refresh all research reports for this theme (costs credits)` });
  }
  items.push({ value: 'trade', label: `${n++}. Make a trade` });
  items.push({ value: 'back', label: `${n++}. Back` });

  const list = new VimSelectList(items, items.length, selectListTheme);
  list.onSelect = (item) => onSelect(item.value);
  list.onCancel = () => onCancel();

  const container = new Container();
  container.addChild(list);
  (container as any)._browseList = list;

  return container;
}
