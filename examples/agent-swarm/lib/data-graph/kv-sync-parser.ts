import type { KvInstruction } from './types.js';

const VALID_OPS = new Set(['ASN', 'DEL', 'CLR', 'RPL']);

export function parseKvEvent(data: string): KvInstruction[] {
  const instructions: KvInstruction[] = [];

  for (const line of data.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const spaceIndex = trimmed.indexOf(' ');
    const op = spaceIndex === -1 ? trimmed : trimmed.slice(0, spaceIndex);
    const rest = spaceIndex === -1 ? '' : trimmed.slice(spaceIndex + 1);

    if (!VALID_OPS.has(op)) {
      throw new Error(`Unknown kv-sync op: "${op}"`);
    }

    if (op === 'CLR') {
      instructions.push({ op: 'CLR' });
    } else if (op === 'ASN' || op === 'RPL') {
      instructions.push({ op, data: JSON.parse(rest) as Record<string, unknown> });
    } else if (op === 'DEL') {
      instructions.push({ op: 'DEL', data: JSON.parse(rest) as string[] });
    }
  }

  return instructions;
}
