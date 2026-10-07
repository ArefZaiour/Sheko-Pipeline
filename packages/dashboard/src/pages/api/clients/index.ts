import type { NextApiRequest, NextApiResponse } from 'next';
import { getPool } from '../../../lib/db';

interface ClientRow {
  id: string;
  name: string;
  created_at: string;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method not allowed' });
  }
  const pool = getPool();
  const { rows } = await pool.query<ClientRow>(
    'SELECT id, name, created_at FROM clients ORDER BY name ASC',
  );
  return res.status(200).json(rows);
}
