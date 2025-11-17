import ForumListClient from '@/components/ForumListClient';
type Props = { params: { id: string } };

export default async function UserPage({ params }: Props) {
	// In some Next dev/runtime configurations `params` can be a Promise; unwrap it to
	// ensure we have the actual params object on the server before accessing `id`.
	const resolvedParams: any = await params;
	const { id } = resolvedParams;

	// Fetch public user profile from backend.
	// Prefer an explicit backend URL when available (BACKEND_URL or NEXT_PUBLIC_API_URL).
	// On the server we try an explicit backend host first (so server-side render
	// calls the backend container directly), then the Next proxy `/api/*`, then
	// finally a same-origin `/users/:id` fallback.
	// Keep fetch logic simple and use Next's proxy by default. This avoids
	// container-host fallback logic and ensures server-side requests go through
	// the same `/api` rewrite configured in `next.config.js`.
	let user: any = null;

	// Fetch up to 20 authored posts for this user to show on their profile.
	let authoredPosts: any[] = [];
	let authoredFailed = false;
	try {
		const apiPath = `/api/discussions?author_id=${encodeURIComponent(id)}&skip=0&limit=20`;
		let res: Response | null = null;
		try {
			res = await fetch(apiPath, { cache: 'no-store' });
		} catch (e) {
			res = null;
		}
		if (res && res.ok) {
			authoredPosts = await res.json();
		} else {
			const backendBase = process.env.BACKEND_URL || process.env.NEXT_PUBLIC_API_URL || 'http://gin-backend:8080';
			try {
				const res2 = await fetch(`${backendBase}/discussions?author_id=${encodeURIComponent(id)}&skip=0&limit=20`, { cache: 'no-store' });
				if (res2 && res2.ok) {
					authoredPosts = await res2.json();
				}
			} catch (e) {
				// ignore
			}
		}
	} catch (err) {
		authoredFailed = true;
	}
	try {
		// Try Next proxy first (works in most dev/prod setups). If that doesn't
		// succeed (non-ok or network error), fall back to an explicit backend
		// host if available so SSR can still retrieve data inside containers.
		const apiPath = `/api/users/${encodeURIComponent(id)}`;
		let res: Response | null = null;
		try {
			res = await fetch(apiPath, { cache: 'no-store' });
		} catch (e) {
			res = null;
		}
		if (res && res.ok) {
			user = await res.json();
			} else {
				// Prefer the Docker service hostname for the backend when available
				// (works when Next and Gin backend run in the same compose network).
				const backendBase = process.env.BACKEND_URL || process.env.NEXT_PUBLIC_API_URL || 'http://gin-backend:8080';
			try {
				const res2 = await fetch(`${backendBase}/users/${encodeURIComponent(id)}`, { cache: 'no-store' });
				if (res2 && res2.ok) {
					user = await res2.json();
				}
			} catch (e) {
				// ignore
			}
		}
	} catch (err) {
		// ignore; user stays null
	}

	// Ensure authoredPosts only contains posts authored by this user.
	// Match by explicit author_id/authorId when available, or by username when
	// the `user` payload is present. If the server returned unrelated posts for
	// any reason, this protects the public profile from showing others' posts.
	// Strictly enforce that posts shown on a public profile belong to the
	// requested route id. The DB/response we saw includes `author_id` values like
	// 'f2273140-3f12-4588-ae05-a9e1030911ab' which must match the route UUID.
	if (Array.isArray(authoredPosts)) {
		const routeIdStr = String(id)
		authoredPosts = authoredPosts.filter((p: any) => {
			// Check common top-level id fields first; this should catch the
			// typical backend shape where `author_id` is present.
			const postAuthorId = String(p.author_id ?? p.authorId ?? p.user_id ?? p.userId ?? p.author?._id ?? p.author?.id ?? '')
			if (postAuthorId && postAuthorId === routeIdStr) return true

			// Fallback: some payloads might put the author id under nested fields
			// or use the username as the route; we don't want to be permissive
			// here — prefer the strict UUID match. If the route looks like a
			// non-UUID username, allow a username match as a secondary fallback.
			const routeLooksLikeUUID = /^[0-9a-fA-F-]{8,}$/.test(routeIdStr)
			if (!routeLooksLikeUUID) {
				const postAuthorName = String(p.author_username ?? p.authorUsername ?? p.username ?? p.author?.username ?? '')
				if (postAuthorName && postAuthorName.toLowerCase() === routeIdStr.toLowerCase()) return true
			}

			return false
		})
	}

	return (
		<>
			<div id="background"></div>
			<main className="page-offset">
				<section className="profile-columns" style={{ justifyContent: 'center' }}>
					<div className="left-rail" style={{ background: '#ffffff', padding: 20, borderRadius: 12, border: '1px solid rgba(86,56,97,0.06)', boxShadow: '0 8px 30px rgba(17,24,39,0.06)', color: '#111' }}>
						<h2 style={{ margin: '0 0 8px 0', color: '#1f1f1f' }}>{user ? user.username : `User ${id}`}</h2>
						{user?.country ? <div style={{ color: '#666', marginBottom: 8 }}>Country: {user.country}</div> : null}
						<div style={{ color: '#666', marginBottom: 8 }}>Joined: {user?.createdAt ? new Date(user.createdAt).toLocaleDateString() : '—'}</div>
						<div style={{ marginTop: 12 }}>
							<strong>Stats</strong>
							<ul style={{ marginTop: 8 }}>
								<li>Predictions ratio: {user?.predictions_ratio ?? '—'}</li>
								<li>Correct predictions: {user?.correct_predictions ?? 0}</li>
								<li>False predictions: {user?.false_predictions ?? 0}</li>
								<li>Num predictions: {user?.num_predictions ?? 0}</li>
								<li>Num posts: {user?.num_posts ?? 0}</li>
							</ul>
						</div>

						{user?.favorite_rikishi ? (
							<div style={{ marginTop: 14 }}>
								<strong style={{ display: 'block', marginBottom: 8 }}>Favorite rikishi</strong>
								<div style={{ color: '#333', marginTop: 4 }}>
									{typeof user.favorite_rikishi === 'object' ? (
										<div style={{ lineHeight: 1.5 }}>
											<a href={`/rikishi/${user.favorite_rikishi.id}`} style={{ textDecoration: 'none', color: '#563861', fontWeight: 700, fontSize: 16 }}>
												{user.favorite_rikishi.shikona ?? user.favorite_rikishi.name ?? `#${user.favorite_rikishi.id}`}
											</a>
											<div style={{ color: '#666', marginTop: 6 }}>Rank: {user.favorite_rikishi.current_rank ?? '—'}</div>
											<div style={{ color: '#666' }}>Heya: {user.favorite_rikishi.heya ?? '—'}</div>
											<div style={{ marginTop: 8 }}><strong style={{ color: '#444' }}>{user.favorite_rikishi.wins ?? '—'}</strong> wins • <strong style={{ color: '#444' }}>{user.favorite_rikishi.losses ?? '—'}</strong> losses</div>
											<div style={{ color: '#666', marginTop: 6 }}>Matches: {user.favorite_rikishi.matches ?? '—'}</div>
											<div style={{ color: '#666', marginTop: 6 }}>Yusho: {user.favorite_rikishi.yusho_count ?? '—'} • Sansho: {user.favorite_rikishi.sansho_count ?? '—'}</div>
										</div>
									) : (
										<div>Rikishi id: {String(user.favorite_rikishi)}</div>
									)}
								</div>
							</div>
						) : (
							<div style={{ marginTop: 14, color: '#777' }}>No favorite rikishi chosen.</div>
						)}
					</div>

					<div className="profile-main">
						{/* Show posts authored by this user (server-side fetched and passed as initial state to client list) */}
						{
							(() => {
								const mapped = Array.isArray(authoredPosts)
									? authoredPosts.map((p: any) => ({
										id: p.id || (p._id && p._id.toString && p._id.toString()) || p.author_id || '',
										upvotes: p.upvotes ?? 0,
										downvotes: p.downvotes ?? 0,
										author: p.author_username ?? 'anonymous',
										date_created: p.created_at ?? new Date().toISOString(),
										title: p.title,
										body: p.body,
									}))
									: [];
								return <ForumListClient initial={mapped} initialLoadFailed={authoredFailed} baseApi={`/api/discussions?author_id=${encodeURIComponent(id)}`} />;
							})()
						}
					</div>
				</section>
			</main>
		</>
	);
}
