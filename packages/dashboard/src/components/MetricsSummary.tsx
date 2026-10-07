import { KpiCard } from './KpiCard';
import type { MetricsTotals } from '../lib/metrics';

interface MetricsSummaryProps {
  totals: MetricsTotals;
}

function fmt(n: number, decimals = 0) {
  return n.toLocaleString('en-US', { maximumFractionDigits: decimals });
}

function fmtUsd(n: number) {
  return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function MetricsSummary({ totals }: MetricsSummaryProps) {
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12 }}>
      <KpiCard label="Spend" value={fmtUsd(totals.spend)} />
      <KpiCard label="Impressions" value={fmt(totals.impressions)} />
      <KpiCard label="Clicks" value={fmt(totals.clicks)} />
      <KpiCard label="Conversions" value={fmt(totals.conversions)} />
      <KpiCard
        label="CPA"
        value={totals.cpa !== null ? fmtUsd(totals.cpa) : '—'}
        sub="Cost per acquisition"
      />
      <KpiCard
        label="ROAS"
        value={totals.roas !== null ? totals.roas.toFixed(2) + 'x' : '—'}
        sub="Return on ad spend"
      />
    </div>
  );
}
