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
    hasModel: true,
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
  test('shows Refreshed and Model run as two distinct, labeled lines', () => {
    const out = formatAnalyzeHuman(base({
      refreshedAt: '2026-06-10 12:00 UTC',
      modelRunAt: '2026-06-09 18:30 UTC',
    }));
    expect(out).toContain('Refreshed:   2026-06-10 12:00 UTC');
    expect(out).toContain('our local fetch time');
    expect(out).toContain('Model run:   2026-06-09 18:30 UTC');
    expect(out).toContain('upstream Octagon');
  });

  test('omits Model run line when upstream timestamp is missing', () => {
    const out = formatAnalyzeHuman(base({ modelRunAt: null }));
    expect(out).toContain('Refreshed:');
    expect(out).not.toContain('Model run:');
  });

  test('hint says Refreshed is the freshness indicator', () => {
    const out = formatAnalyzeHuman(base({ fromCache: true }));
    // Refreshed line includes the explanatory caption
    expect(out).toContain('bumps on --refresh');
  });
});
