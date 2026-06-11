import { describe, test, expect } from 'bun:test';
import { formatAnalyzeHuman, type AnalyzeData } from '../analyze.js';

function base(overrides: Partial<AnalyzeData>): AnalyzeData {
  return {
    ticker: 'KX-A',
    eventTicker: 'KX-A',
    title: 'Test market',
    expirationTime: null,
    refreshedAt: '2026-06-10 12:00 UTC',
    modelRunAt: '2026-06-09 18:30 UTC',
    staleUpstream: false,
    hasModel: true,
    hasMarketPrice: true,
    modelProb: 0.72,
    marketProb: 0.58,
    edge: 0.14,
    edgePp: '+14pp',
    confidence: 'very_high',
    mispricingSignal: 'underpriced',
    signal: 'BUY YES',
    drivers: [],
    catalysts: [],
    sources: [],
    kelly: {
      side: 'yes', fraction: 0.05, adjustedFraction: 0.025, contracts: 5,
      dollarAmountCents: 100, entryPriceCents: 58, availableBankroll: 10000,
      openExposure: 0, cashBalance: 10000, portfolioValue: 10000,
      liquidityAdjusted: false,
    } as any,
    riskGate: { passed: true, checks: [] } as any,
    liquidityGrade: 'Good',
    fromCache: true,
    reportAge: '12m ago',
    reportId: 'r-1',
    rawReport: '',
    ...overrides,
  };
}

describe('formatAnalyzeHuman — model coverage display', () => {
  test('renders real probabilities when hasModel=true', () => {
    const out = formatAnalyzeHuman(base({ hasModel: true }));
    expect(out).toContain('Model Prob:  72.0%');
    expect(out).toContain('Market Prob: 58.0%');
    expect(out).toContain('Edge:        +14pp');
    expect(out).not.toContain('no Octagon model coverage');
  });

  test('renders -- for model/edge/confidence when hasModel=false', () => {
    const out = formatAnalyzeHuman(base({ hasModel: false, modelProb: 0.5 }));
    // Must NOT show the 0.5 placeholder as if it were a real prediction
    expect(out).not.toContain('Model Prob:  50.0%');
    expect(out).toContain('Model Prob:  --');
    expect(out).toContain('no Octagon model coverage');
    expect(out).toContain('Edge:        --');
    expect(out).toContain('Confidence:  --');
    expect(out).toContain('Mispricing:  --');
    // Market price always shows — it's from Kalshi, not Octagon's model
    expect(out).toContain('Market Prob: 58.0%');
  });
});

describe('formatAnalyzeHuman — date label clarity', () => {
  test('shows Cache refreshed and Report body updated as two distinct, labeled lines', () => {
    const out = formatAnalyzeHuman(base({
      refreshedAt: '2026-06-10 12:00 UTC',
      modelRunAt: '2026-06-09 18:30 UTC',
    }));
    expect(out).toContain('Cache refreshed at:    2026-06-10 12:00 UTC');
    expect(out).toContain('Report body updated at: 2026-06-09 18:30 UTC');
    expect(out).toContain('when the bot last fetched the Octagon payload');
    expect(out).toContain('when Octagon last ran the model upstream');
  });

  test('omits Report body line when upstream timestamp is missing', () => {
    const out = formatAnalyzeHuman(base({ modelRunAt: null }));
    expect(out).toContain('Cache refreshed at:');
    expect(out).not.toContain('Report body updated at:');
  });

  test('explains that --refresh bumps Cache refreshed at', () => {
    const out = formatAnalyzeHuman(base({ fromCache: true }));
    expect(out).toContain('bumps on --refresh');
  });
});

describe('formatAnalyzeHuman — no market price', () => {
  test('shows "--" for market prob + edge and skips Kelly when no last price', () => {
    const out = formatAnalyzeHuman(base({
      hasMarketPrice: false, hasModel: true,
      modelProb: 0.62, marketProb: 0.5,
    }));
    expect(out).toContain('Market Prob: --');
    expect(out).toContain("no last traded price");
    // Edge must NOT be computed from a placeholder
    expect(out).toContain('Edge:        --');
    expect(out).toContain('Confidence:  --');
    // Model prob still renders since Octagon does have coverage
    expect(out).toContain('Model Prob:  62.0%');
    // Kelly block is replaced with a skip message
    expect(out).toContain('Skipped — market has no last traded price');
    expect(out).not.toContain('Cash Balance:');
  });

  test('shows "--" for both Model Prob and Market Prob when neither is available', () => {
    const out = formatAnalyzeHuman(base({
      hasMarketPrice: false, hasModel: false,
      modelProb: 0.5, marketProb: 0.5,
    }));
    expect(out).toContain('Model Prob:  --');
    expect(out).toContain('Market Prob: --');
    expect(out).toContain('Edge:        --');
  });
});

describe('formatAnalyzeHuman — stale upstream warning', () => {
  test('warns when --refresh did not get a newer upstream analysis', () => {
    const out = formatAnalyzeHuman(base({
      staleUpstream: true,
      refreshedAt: '2026-06-10 21:15 UTC',
      modelRunAt: '2026-04-13 00:13 UTC',
    }));
    expect(out).toContain('--refresh pulled the same Octagon report body');
    expect(out).toContain('upstream analysis hasn');  // straight quote in test; em-dash and apostrophe variants both possible
    expect(out).toContain('2026-04-13 00:13 UTC');
    expect(out).toContain('stale upstream report');
  });

  test('no warning on normal cache hit', () => {
    const out = formatAnalyzeHuman(base({ staleUpstream: false }));
    expect(out).not.toContain('stale upstream');
    expect(out).not.toContain('pulled the same');
  });
});
