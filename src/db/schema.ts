import type { Database } from 'bun:sqlite';

/**
 * Run all CREATE TABLE IF NOT EXISTS statements and indexes.
 * Safe to call on every startup — idempotent.
 */
export function migrate(db: Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS themes (
      theme_id        TEXT PRIMARY KEY,
      name            TEXT NOT NULL,
      filter_query    TEXT,
      tickers         TEXT,
      last_resolved_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS events (
      ticker      TEXT PRIMARY KEY,
      category    TEXT,
      expiry      INTEGER,
      vol_24h     REAL,
      theme_id    TEXT REFERENCES themes,
      active      INTEGER DEFAULT 1,
      updated_at  INTEGER
    );

    CREATE TABLE IF NOT EXISTS edge_history (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker            TEXT NOT NULL,
      event_ticker      TEXT NOT NULL,
      timestamp         INTEGER NOT NULL,
      model_prob        REAL NOT NULL,
      market_prob       REAL NOT NULL,
      edge              REAL NOT NULL,
      octagon_report_id TEXT,
      drivers_json      TEXT,
      sources_json      TEXT,
      catalysts_json    TEXT,
      cache_hit         INTEGER,
      cache_miss        INTEGER DEFAULT 0,
      confidence        TEXT,
      UNIQUE(ticker, timestamp)
    );

    CREATE INDEX IF NOT EXISTS idx_edge_ticker_ts
      ON edge_history(ticker, timestamp DESC);

    CREATE INDEX IF NOT EXISTS idx_edge_confidence
      ON edge_history(confidence, timestamp DESC);

    CREATE TABLE IF NOT EXISTS octagon_reports (
      report_id                TEXT PRIMARY KEY,
      ticker                   TEXT NOT NULL,
      event_ticker             TEXT NOT NULL,
      model_prob               REAL NOT NULL,
      market_prob              REAL,
      mispricing_signal        TEXT,
      drivers_json             TEXT,
      catalysts_json           TEXT,
      sources_json             TEXT,
      resolution_history_json  TEXT,
      contract_snapshot_json   TEXT,
      raw_response             TEXT,
      model_accuracy           REAL,
      variant_used             TEXT,
      fetched_at               INTEGER NOT NULL,
      expires_at               INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_octagon_ticker
      ON octagon_reports(ticker, fetched_at DESC);

    CREATE TABLE IF NOT EXISTS positions (
      position_id  TEXT PRIMARY KEY,
      ticker       TEXT NOT NULL,
      event_ticker TEXT NOT NULL,
      direction    TEXT NOT NULL,
      size         REAL NOT NULL,
      entry_price  REAL NOT NULL,
      entry_edge   REAL,
      entry_kelly  REAL,
      current_pnl  REAL DEFAULT 0,
      status       TEXT DEFAULT 'open',
      opened_at    INTEGER,
      closed_at    INTEGER
    );

    CREATE TABLE IF NOT EXISTS trades (
      trade_id     TEXT PRIMARY KEY,
      position_id  TEXT REFERENCES positions,
      order_id     TEXT,
      ticker       TEXT NOT NULL,
      action       TEXT NOT NULL,
      side         TEXT NOT NULL,
      size         REAL NOT NULL,
      price        REAL NOT NULL,
      fill_status  TEXT,
      kalshi_response TEXT,
      created_at   INTEGER
    );

    CREATE TABLE IF NOT EXISTS risk_snapshots (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp           INTEGER NOT NULL,
      cash_balance        REAL,
      portfolio_value     REAL,
      open_exposure       REAL,
      available_bankroll  REAL,
      daily_pnl           REAL,
      drawdown_current    REAL,
      drawdown_max        REAL,
      correlation_max     REAL,
      positions_count     INTEGER,
      circuit_breaker_on  INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS alerts (
      alert_id    TEXT PRIMARY KEY,
      ticker      TEXT,
      alert_type  TEXT NOT NULL,
      edge        REAL,
      message     TEXT NOT NULL,
      channels    TEXT,
      status      TEXT DEFAULT 'pending',
      created_at  INTEGER
    );

    CREATE TABLE IF NOT EXISTS brier_scores (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker          TEXT NOT NULL,
      event_ticker    TEXT NOT NULL,
      category        TEXT NOT NULL,
      model_prob      REAL NOT NULL,
      actual_outcome  INTEGER NOT NULL,
      brier_score     REAL NOT NULL,
      settled_at      INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_brier_category
      ON brier_scores(category, settled_at DESC);

    CREATE TABLE IF NOT EXISTS event_index (
      event_ticker   TEXT PRIMARY KEY,
      series_ticker  TEXT,
      title          TEXT NOT NULL,
      category       TEXT,
      strike_date    TEXT,
      sub_title      TEXT,
      tags           TEXT,
      markets_json   TEXT,
      indexed_at     INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_event_index_title
      ON event_index(title);
    CREATE INDEX IF NOT EXISTS idx_event_index_series
      ON event_index(series_ticker);
    CREATE INDEX IF NOT EXISTS idx_event_index_category
      ON event_index(category);

    CREATE TABLE IF NOT EXISTS event_index_meta (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS octagon_history (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      history_id          INTEGER NOT NULL,
      event_ticker        TEXT NOT NULL,
      captured_at         TEXT NOT NULL,
      model_probability   REAL NOT NULL,
      market_probability  REAL NOT NULL,
      edge_pp             REAL,
      confidence_score    REAL,
      series_category     TEXT,
      close_time          TEXT,
      name                TEXT,
      UNIQUE(event_ticker, history_id)
    );

    CREATE INDEX IF NOT EXISTS idx_history_event
      ON octagon_history(event_ticker, captured_at);
  `);

  // Schema migrations for columns added after initial release
  const edgeCols = db.query(`PRAGMA table_info(edge_history)`).all() as Array<{ name: string }>;
  if (!edgeCols.some((c) => c.name === 'cache_miss')) {
    db.exec(`ALTER TABLE edge_history ADD COLUMN cache_miss INTEGER DEFAULT 0`);
  }

  const reportCols = db.query(`PRAGMA table_info(octagon_reports)`).all() as Array<{ name: string }>;
  if (!reportCols.some((c) => c.name === 'raw_response')) {
    db.exec(`ALTER TABLE octagon_reports ADD COLUMN raw_response TEXT`);
  }

  const eventIndexCols = db.query(`PRAGMA table_info(event_index)`).all() as Array<{ name: string }>;
  if (!eventIndexCols.some((c) => c.name === 'tags')) {
    db.exec(`ALTER TABLE event_index ADD COLUMN tags TEXT`);
    // Force re-index so tags get populated on next ensureIndex() call
    db.exec(`DELETE FROM event_index_meta WHERE key = 'last_refresh'`);
  }

  if (!reportCols.some((c) => c.name === 'has_history')) {
    db.exec(`ALTER TABLE octagon_reports ADD COLUMN has_history INTEGER DEFAULT 0`);
  }
  if (!reportCols.some((c) => c.name === 'mutually_exclusive')) {
    db.exec(`ALTER TABLE octagon_reports ADD COLUMN mutually_exclusive INTEGER DEFAULT 0`);
  }
  if (!reportCols.some((c) => c.name === 'series_category')) {
    db.exec(`ALTER TABLE octagon_reports ADD COLUMN series_category TEXT`);
  }
  if (!reportCols.some((c) => c.name === 'confidence_score')) {
    db.exec(`ALTER TABLE octagon_reports ADD COLUMN confidence_score REAL`);
  }
}
