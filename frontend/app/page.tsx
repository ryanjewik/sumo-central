import HomepageClient from '../components/HomepageClient'

export const dynamic = 'force-dynamic';

export default async function Page() {
  // Fetch initial homepage and upcoming documents server-side so the client
  // wrapper can hydrate from these payloads without an extra round-trip.
  let homepage: any = null;
  let upcoming: any = null;
  try {
    const res = await fetch('/api/homepage', { cache: 'no-store' });
    if (res.ok) homepage = await res.json();
  } catch (e) {
    // ignore server fetch errors and let the client re-fetch if needed
  }
  try {
    const res2 = await fetch('/api/upcoming', { cache: 'no-store' });
    if (res2.ok) upcoming = await res2.json();
  } catch (e) {
    // ignore
  }

  return <HomepageClient initialHomepage={homepage} initialUpcoming={upcoming} />;
}
