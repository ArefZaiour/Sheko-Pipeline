import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import type { MetricsRow } from '../lib/metrics';

interface TimeSeriesChartProps {
  rows: MetricsRow[];
  metric: 'spend' | 'impressions' | 'clicks' | 'conversions';
}

const COLORS: Record<string, string> = {
  google_ads: '#4285F4',
  meta: '#1877F2',
};

const LABELS: Record<string, string> = {
  google_ads: 'Google Ads',
  meta: 'Meta',
};

export function TimeSeriesChart({ rows, metric }: TimeSeriesChartProps) {
  // Pivot: { period -> { google_ads?: number, meta?: number } }
  const periodMap = new Map<string, Record<string, number>>();
  for (const r of rows) {
    if (!periodMap.has(r.period)) periodMap.set(r.period, { period: r.period as unknown as number });
    periodMap.get(r.period)![r.platform] = r[metric] as number;
  }
  const data = Array.from(periodMap.values()).sort((a, b) =>
    String(a.period) < String(b.period) ? -1 : 1,
  );
  const platforms = Array.from(new Set(rows.map((r) => r.platform)));

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
        <XAxis dataKey="period" tick={{ fontSize: 11 }} />
        <YAxis tick={{ fontSize: 11 }} />
        <Tooltip />
        <Legend />
        {platforms.map((p) => (
          <Line
            key={p}
            type="monotone"
            dataKey={p}
            name={LABELS[p] ?? p}
            stroke={COLORS[p] ?? '#888'}
            dot={false}
            strokeWidth={2}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}
