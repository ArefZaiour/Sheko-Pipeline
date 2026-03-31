import type { NextApiRequest, NextApiResponse } from 'next';
import { getMetrics, computeTotals, type Rollup } from '../../../../lib/metrics';

const ROLLUPS: Rollup[] = ['daily', 'weekly', 'monthly'];

function isRollup(v: unknown): v is Rollup {
  return ROLLUPS.includes(v as Rollup);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { clientId, startDate, endDate, rollup = 'daily' } = req.query;

  if (
    typeof clientId !== 'string' ||
    typeof startDate !== 'string' ||
    typeof endDate !== 'string'
  ) {
    return res.status(400).json({ error: 'clientId, startDate, and endDate are required' });
  }

  if (!isRollup(rollup)) {
    return res.status(400).json({ error: 'rollup must be daily, weekly, or monthly' });
  }

  const rows = await getMetrics(clientId, startDate, endDate, rollup);
  const totals = computeTotals(rows);

  return res.status(200).json({ rows, totals });
}
