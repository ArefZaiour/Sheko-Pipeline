import type { MetricsRow } from '../lib/metrics';

interface ExportButtonProps {
  rows: MetricsRow[];
  filename: string;
}

const HEADERS = ['period', 'platform', 'impressions', 'clicks', 'spend', 'conversions', 'revenue', 'cpa', 'roas'];

export function ExportButton({ rows, filename }: ExportButtonProps) {
  function handleExport() {
    const csvRows = [
      HEADERS.join(','),
      ...rows.map((r) =>
        [
          r.period,
          r.platform,
          r.impressions,
          r.clicks,
          r.spend.toFixed(4),
          r.conversions,
          r.revenue.toFixed(4),
          r.cpa !== null ? r.cpa.toFixed(4) : '',
          r.roas !== null ? r.roas.toFixed(4) : '',
        ].join(','),
      ),
    ];
    const blob = new Blob([csvRows.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename.endsWith('.csv') ? filename : filename + '.csv';
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <button onClick={handleExport} style={btnStyle}>
      Export CSV
    </button>
  );
}

const btnStyle: React.CSSProperties = {
  padding: '8px 16px',
  background: '#111827',
  color: '#fff',
  border: 'none',
  borderRadius: 6,
  fontSize: 14,
  cursor: 'pointer',
};
