import { getPool } from './db';
import type { Platform } from '@perf-marketing/shared';

export type Rollup = 'daily' | 'weekly' | 'monthly';

export interface MetricsRow {
  period: string;
  platform: Platform;
  impressions: number;
  clicks: number;
  spend: number;
  conversions: number;
  revenue: number;
  cpa: number | null;
  roas: number | null;
}

export interface MetricsTotals {
  impressions: number;
  clicks: number;
  spend: number;
  conversions: number;
  revenue: number;
  cpa: number | null;
  roas: number | null;
}

const TRUNC: Record<Rollup, string> = {
  daily: 'day',
  weekly: 'week',
  monthly: 'month',
};

export async function getMetrics(
  clientId: string,
  startDate: string,
  endDate: string,
  rollup: Rollup,
): Promise<MetricsRow[]> {
  const trunc = TRUNC[rollup];
  const pool = getPool();
  const { rows } = await pool.query<{
    period: string;
    platform: string;
    impressions: string;
    clicks: string;
    spend_usd: string;
    conversions: string;
    revenue_usd: string;
  }>(
    `SELECT
       date_trunc($1, cm.date)::date::text AS period,
       cm.platform,
       SUM(cm.impressions)   AS impressions,
       SUM(cm.clicks)        AS clicks,
       SUM(cm.spend_usd)     AS spend_usd,
       SUM(cm.conversions)   AS conversions,
       SUM(cm.revenue_usd)   AS revenue_usd
     FROM campaign_metrics cm
     JOIN ad_accounts aa ON aa.id = cm.account_id
     WHERE aa.client_id = $2
       AND cm.date BETWEEN $3 AND $4
     GROUP BY 1, 2
     ORDER BY 1 ASC, 2 ASC`,
    [trunc, clientId, startDate, endDate],
  );

  return rows.map((r) => {
    const spend = Number(r.spend_usd);
    const conversions = Number(r.conversions);
    const revenue = Number(r.revenue_usd);
    return {
      period: r.period,
      platform: r.platform as Platform,
      impressions: Number(r.impressions),
      clicks: Number(r.clicks),
      spend,
      conversions,
      revenue,
      cpa: conversions > 0 ? spend / conversions : null,
      roas: spend > 0 ? revenue / spend : null,
    };
  });
}

export function computeTotals(rows: MetricsRow[]): MetricsTotals {
  const totals = rows.reduce(
    (acc, r) => ({
      impressions: acc.impressions + r.impressions,
      clicks: acc.clicks + r.clicks,
      spend: acc.spend + r.spend,
      conversions: acc.conversions + r.conversions,
      revenue: acc.revenue + r.revenue,
    }),
    { impressions: 0, clicks: 0, spend: 0, conversions: 0, revenue: 0 },
  );
  return {
    ...totals,
    cpa: totals.conversions > 0 ? totals.spend / totals.conversions : null,
    roas: totals.spend > 0 ? totals.revenue / totals.spend : null,
  };
}
