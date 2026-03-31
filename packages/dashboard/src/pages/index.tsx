// Dashboard home — redirects to /clients
import { useEffect } from 'react';
import { useRouter } from 'next/router';

export default function Home() {
  const router = useRouter();
  useEffect(() => { router.replace('/clients'); }, [router]);
  return null;
}
