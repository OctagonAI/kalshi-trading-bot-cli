import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { tmpdir } from 'os';
import { join } from 'path';
import { existsSync, readFileSync, rmSync } from 'fs';
import { askOctagonAgent, handleOctagonChat, octagonConversationLength, resetOctagonConversation } from '../octagon-chat';

const realFetch = globalThis.fetch;
let requests: Array<Record<string, unknown>> = [];
let responder: (body: Record<string, unknown>) => Response | Promise<Response>;
let transcriptFile: string;

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status });
}

beforeEach(() => {
  requests = [];
  transcriptFile = join(tmpdir(), `octagon-transcript-${Date.now()}-${Math.random().toString(36).slice(2)}.json`);
  process.env.OCTAGON_TRANSCRIPT_PATH = transcriptFile;
  resetOctagonConversation();
  globalThis.fetch = (async (_url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>;
    requests.push(body);
    return responder(body);
  }) as typeof fetch;
});

afterEach(() => {
  globalThis.fetch = realFetch;
  resetOctagonConversation();
  delete process.env.OCTAGON_TRANSCRIPT_PATH;
  try { rmSync(transcriptFile); } catch { /* already gone */ }
});

describe('handleOctagonChat', () => {
  test('no args prints usage', () => {
    const res = handleOctagonChat([]);
    expect('followUp' in res).toBe(false);
    expect(res.output).toContain('Usage: /octagon');
  });

  test('reset clears the conversation', async () => {
    responder = () => json(200, { id: 'resp-1', output_text: 'answer' });
    await askOctagonAgent('first question');
    expect(octagonConversationLength()).toBe(2);
    const res = handleOctagonChat(['reset']);
    expect(res.output).toContain('reset');
    expect(octagonConversationLength()).toBe(0);
  });

  test('question returns a followUp', () => {
    const res = handleOctagonChat(['find', 'fed', 'markets']);
    expect('followUp' in res).toBe(true);
  });
});

describe('askOctagonAgent multi-turn', () => {
  test('first turn sends the bare question', async () => {
    responder = () => json(200, { id: 'resp-1', output_text: 'first answer' });
    await askOctagonAgent('q1');
    expect(requests[0].input).toBe('q1');
    expect(requests[0].model).toBe('octagon-prediction-markets-agent');
  });

  test('second turn replays the transcript', async () => {
    responder = () => json(200, { id: 'resp-1', output_text: 'first answer' });
    await askOctagonAgent('q1');
    responder = () => json(200, { id: 'resp-2', output_text: 'second answer' });
    await askOctagonAgent('q2');
    const replay = String(requests[1].input);
    expect(replay).toContain('Conversation so far');
    expect(replay).toContain('User: q1');
    expect(replay).toContain('Agent: first answer');
    expect(replay).toContain('User: q2');
  });

  test('thirteenth request replays all twelve prior exchanges', async () => {
    responder = (body) => json(200, { output_text: `ans-${String(body.input).slice(-3)}` });
    for (let i = 1; i <= 12; i++) await askOctagonAgent(`q${i}`);
    await askOctagonAgent('q13');
    const replay = String(requests[12].input);
    for (let i = 1; i <= 12; i++) {
      expect(replay).toContain(`User: q${i}\n`);
    }
    expect(replay).toContain('User: q13');
  });

  test('transcript persists to disk and reloads', async () => {
    responder = () => json(200, { output_text: 'persisted answer' });
    await askOctagonAgent('remember me');
    expect(existsSync(transcriptFile)).toBe(true);
    const saved = JSON.parse(readFileSync(transcriptFile, 'utf-8')) as { turns: Array<{ text: string }> };
    expect(saved.turns).toHaveLength(2);
    expect(saved.turns[1].text).toBe('persisted answer');
  });

  test('concurrent questions are serialized in order', async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    responder = async (body) => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await new Promise((r) => setTimeout(r, 10));
      inFlight--;
      return json(200, { output_text: `ans for ${body.input}` });
    };
    const [a, b] = await Promise.all([askOctagonAgent('first'), askOctagonAgent('second')]);
    expect(maxInFlight).toBe(1);
    expect(a).toContain('first');
    // Second request replayed the first exchange.
    expect(String(requests[1].input)).toContain('User: first');
    expect(b).toContain('second');
  });

  test('reset while a request is pending discards its transcript write', async () => {
    let release: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    responder = async () => {
      await gate;
      return json(200, { output_text: 'late answer' });
    };
    const pending = askOctagonAgent('slow question');
    resetOctagonConversation();
    release!();
    const answer = await pending;
    expect(answer).toBe('late answer');
    expect(octagonConversationLength()).toBe(0);
  });

  test('API error surfaces the envelope message', async () => {
    responder = () => json(429, { error: { message: 'Not enough credits', code: 'insufficient_credits' } });
    await expect(askOctagonAgent('q')).rejects.toThrow(/Not enough credits \(insufficient_credits\)/);
  });
});
