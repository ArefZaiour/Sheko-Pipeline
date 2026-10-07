import { useQuery } from '@tanstack/react-query';
import Link from 'next/link';

interface Client {
  id: string;
  name: string;
  created_at: string;
}

export default function ClientsPage() {
  const { data: clients, isLoading, error } = useQuery<Client[]>({
    queryKey: ['clients'],
    queryFn: () => fetch('/api/clients').then((r) => r.json()),
  });

  return (
    <div style={pageStyle}>
      <h1 style={headingStyle}>Clients</h1>
      {isLoading && <p>Loading...</p>}
      {error && <p style={{ color: 'red' }}>Failed to load clients.</p>}
      {clients && clients.length === 0 && <p style={{ color: '#6b7280' }}>No clients yet.</p>}
      {clients && clients.length > 0 && (
        <ul style={listStyle}>
          {clients.map((c) => (
            <li key={c.id}>
              <Link href={`/clients/${c.id}`} style={linkStyle}>
                {c.name}
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

const pageStyle: React.CSSProperties = {
  maxWidth: 720,
  margin: '40px auto',
  padding: '0 24px',
  fontFamily: 'system-ui, sans-serif',
};

const headingStyle: React.CSSProperties = {
  fontSize: 28,
  fontWeight: 700,
  marginBottom: 24,
  color: '#111827',
};

const listStyle: React.CSSProperties = {
  listStyle: 'none',
  padding: 0,
  margin: 0,
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
};

const linkStyle: React.CSSProperties = {
  display: 'block',
  padding: '14px 16px',
  background: '#fff',
  border: '1px solid #e5e7eb',
  borderRadius: 8,
  color: '#111827',
  textDecoration: 'none',
  fontSize: 15,
  fontWeight: 500,
};
