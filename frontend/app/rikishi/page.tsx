import Link from 'next/link';

type RikishiItem = { id: string; shikona?: string };

async function fetchRikishiList(): Promise<RikishiItem[]> {
  // Prefer an explicit backend base URL via env var. If not provided, try relative
  // paths (assuming the host proxies to the backend).
  const backend = process.env.BACKEND_URL || '';
  const attempts = [
    `${backend}/rikishi`,
    `${backend}/api/rikishi`,
    `${backend}/api/homepage`,
  ];

  for (const url of attempts) {
    try {
      const res = await fetch(url, { cache: 'no-store' });
      if (!res.ok) continue;
      const data = await res.json();

      if (data && Array.isArray((data as any).items)) {
        return (data as any).items.map((it: any) => ({ id: String(it.id), shikona: it.shikona ?? it.name ?? '' }));
      }

      if (Array.isArray(data)) {
        return data.map((id: any) => ({ id: String(id) }));
      }

      // If homepage doc contains a list
      if (typeof data === 'object') {
        const possible = (data as any).rikishi_ids || (data as any).rikishi_list || (data as any).all_rikishi || (data as any).rikishi_index;
        if (Array.isArray(possible)) return possible.map((id: any) => ({ id: String(id) }));
      }
    } catch (err) {
      // try next
    }
  }

  return [];
}

export default async function RikishiIndexPage() {
  const items = await fetchRikishiList();

  return (
    <div style={{ marginTop: '13rem', padding: '1rem' }} className="content-box">
      <div style={{ width: '100%' }}>
        <h1 className="app-text" style={{ marginBottom: '1rem' }}>Rikishi — Index</h1>

        {items.length === 0 ? (
          <div className="app-text">No rikishi found or backend endpoint unavailable.</div>
        ) : (
          <ul style={{ listStyle: 'none', padding: 0 }}>
            {items.map((it) => (
              <li key={it.id} style={{ margin: '0.4rem 0' }}>
                <Link href={`/rikishi/${encodeURIComponent(String(it.id))}`} className="navbar-link">{it.shikona && it.shikona.length > 0 ? `${it.shikona} (${it.id})` : it.id}</Link>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
