import { useState } from 'react';
import { useRouter } from 'next/router';
import { useQuery } from '@tanstack/react-query';
import { format, subDays } from 'date-fns';
import { MetricsSummary } from '../../../components/MetricsSummary';
import { TimeSeriesChart } from '../../../components/TimeSeriesChart';
import { ChannelBreakdown } from '../../../components/ChannelBreakdown';
import { ExportButton } from '../../../components/ExportButton';
import type { MetricsRow, MetricsTotals, Rollup } from '../../../lib/metrics';

interface MetricsResponse {
  rows: MetricsRow[];
  totals: MetricsTotals;
}

type ChartMetric = 'spend' | 'impressions' | 'clicks' | 'conversions';

const ROLLUPS: Rollup[] = ['daily', 'weekly', 'monthly'];
const CHART_METRICS: ChartMetric[] = ['spend', 'impressions', 'clicks', 'conversions'];

export default function ClientDashboard() {
  const router = useRouter();
  const clientId = typeof router.query.clientId === 'string' ? router.query.clientId : null;

  const today = format(new Date(), 'yyyy-MM-dd');
  const thirtyDaysAgo = format(subDays(new Date(), 30), 'yyyy-MM-dd');

  const [startDate, setStartDate] = useState(thirtyDaysAgo);
  const [endDate, setEndDate] = useState(today);
  const [rollup, setRollup] = useState<Rollup>('daily');
  const [chartMetric, setChartMetric] = useState<ChartMetric>('spend');

  const { data, isLoading, error } = useQuery<MetricsResponse>({
    queryKey: ['metrics', clientId, startDate, endDate, rollup],
    queryFn: () =>
      fetch(
        `/api/clients/${clientId}/metrics?startDate=${startDate}&endDate=${endDate}&rollup=${rollup}`,
      ).then((r) => r.json()),
    enabled: !!clientId,
  });

  const exportFilename = `metrics_${clientId}_${startDate}_${endDate}_${rollup}`;

  return (
    <div style={pageStyle}>
      <div style={headerRow}>
        <h1 style={headingStyle}>Performance Report</h1>
        {data && <ExportButton rows={data.rows} filename={exportFilename} />}
      </div>

      {/* Controls */}
      <div style={controlsRow}>
        <label style={labelStyle}>
          From
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            style={inputStyle}
          />
        </label>
        <label style={labelStyle}>
          To
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            style={inputStyle}
          />
        </label>
        <label style={labelStyle}>
          Rollup
          <select value={rollup} onChange={(e) => setRollup(e.target.value as Rollup)} style={inputStyle}>
            {ROLLUPS.map((r) => (
              <option key={r} value={r}>
                {r.charAt(0).toUpperCase() + r.slice(1)}
              </option>
            ))}
          </select>
        </label>
      </div>

      {isLoading && <p>Loading metrics...</p>}
      {error && <p style={{ color: 'red' }}>Failed to load metrics.</p>}

      {data && (
        <>
          <section style={sectionStyle}>
            <MetricsSummary totals={data.totals} />
          </section>

          <section style={sectionStyle}>
            <div style={sectionHeaderRow}>
              <h2 style={sectionHeadingStyle}>Trend</h2>
              <div style={{ display: 'flex', gap: 8 }}>
                {CHART_METRICS.map((m) => (
                  <button
                    key={m}
                    onClick={() => setChartMetric(m)}
                    style={chartMetric === m ? activeTabStyle : tabStyle}
                  >
                    {m.charAt(0).toUpperCase() + m.slice(1)}
                  </button>
                ))}
              </div>
            </div>
            <TimeSeriesChart rows={data.rows} metric={chartMetric} />
          </section>

          <section style={sectionStyle}>
            <h2 style={sectionHeadingStyle}>Channel Breakdown</h2>
            <ChannelBreakdown rows={data.rows} />
          </section>
        </>
      )}
    </div>
  );
}

const pageStyle: React.CSSProperties = {
  maxWidth: 1100,
  margin: '32px auto',
  padding: '0 24px',
  fontFamily: 'system-ui, sans-serif',
};

const headerRow: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  marginBottom: 20,
};

const headingStyle: React.CSSProperties = {
  fontSize: 26,
  fontWeight: 700,
  color: '#111827',
  margin: 0,
};

const controlsRow: React.CSSProperties = {
  display: 'flex',
  gap: 16,
  flexWrap: 'wrap',
  marginBottom: 24,
  alignItems: 'flex-end',
};

const labelStyle: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  fontSize: 12,
  color: '#6b7280',
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
};

const inputStyle: React.CSSProperties = {
  padding: '6px 10px',
  border: '1px solid #d1d5db',
  borderRadius: 6,
  fontSize: 14,
  color: '#111827',
  background: '#fff',
};

const sectionStyle: React.CSSProperties = {
  background: '#fff',
  border: '1px solid #e5e7eb',
  borderRadius: 10,
  padding: '20px 20px',
  marginBottom: 20,
};

const sectionHeaderRow: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  marginBottom: 16,
};

const sectionHeadingStyle: React.CSSProperties = {
  fontSize: 16,
  fontWeight: 600,
  color: '#374151',
  margin: 0,
};

const tabStyle: React.CSSProperties = {
  padding: '4px 12px',
  border: '1px solid #e5e7eb',
  borderRadius: 6,
  background: '#fff',
  fontSize: 13,
  color: '#6b7280',
  cursor: 'pointer',
};

const activeTabStyle: React.CSSProperties = {
  ...tabStyle,
  background: '#111827',
  color: '#fff',
  borderColor: '#111827',
};
