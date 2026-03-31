import type { MetricsRow } from '../lib/metrics';

interface ChannelBreakdownProps {
  rows: MetricsRow[];
}

function fmtUsd(n: number) {
  return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmt(n: number) {
  return n.toLocaleString('en-US');
}

const PLATFORM_LABELS: Record<string, string> = {
  google_ads: 'Google Ads',
  meta: 'Meta',
};

interface ChannelTotals {
  platform: string;
  impressions: number;
  clicks: number;
  spend: number;
  conversions: number;
  revenue: number;
  cpa: number | null;
  roas: number | null;
}

export function ChannelBreakdown({ rows }: ChannelBreakdownProps) {
  const byPlatform = new Map<string, ChannelTotals>();
  for (const r of rows) {
    if (!byPlatform.has(r.platform)) {
      byPlatform.set(r.platform, {
        platform: r.platform,
        impressions: 0,
        clicks: 0,
        spend: 0,
        conversions: 0,
        revenue: 0,
        cpa: null,
        roas: null,
      });
    }
    const t = byPlatform.get(r.platform)!;
    t.impressions += r.impressions;
    t.clicks += r.clicks;
    t.spend += r.spend;
    t.conversions += r.conversions;
    t.revenue += r.revenue;
  }

  const channels: ChannelTotals[] = Array.from(byPlatform.values()).map((t) => ({
    ...t,
    cpa: t.conversions > 0 ? t.spend / t.conversions : null,
    roas: t.spend > 0 ? t.revenue / t.spend : null,
  }));

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={tableStyle}>
        <thead>
          <tr>
            {['Channel', 'Spend', 'Impressions', 'Clicks', 'Conversions', 'CPA', 'ROAS'].map(
              (h) => (
                <th key={h} style={thStyle}>
                  {h}
                </th>
              ),
            )}
          </tr>
        </thead>
        <tbody>
          {channels.map((c) => (
            <tr key={c.platform}>
              <td style={tdStyle}>{PLATFORM_LABELS[c.platform] ?? c.platform}</td>
              <td style={tdStyle}>{fmtUsd(c.spend)}</td>
              <td style={tdStyle}>{fmt(c.impressions)}</td>
              <td style={tdStyle}>{fmt(c.clicks)}</td>
              <td style={tdStyle}>{fmt(c.conversions)}</td>
              <td style={tdStyle}>{c.cpa !== null ? fmtUsd(c.cpa) : '—'}</td>
              <td style={tdStyle}>{c.roas !== null ? c.roas.toFixed(2) + 'x' : '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: 14,
};

const thStyle: React.CSSProperties = {
  textAlign: 'left',
  padding: '8px 12px',
  borderBottom: '2px solid #e5e7eb',
  color: '#6b7280',
  fontSize: 12,
  fontWeight: 600,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
};

const tdStyle: React.CSSProperties = {
  padding: '8px 12px',
  borderBottom: '1px solid #f3f4f6',
  color: '#111827',
};
