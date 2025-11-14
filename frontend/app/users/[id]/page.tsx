type Props = { params: { id: string } };

export default function UserPage({ params }: Props) {
	const { id } = params;
	return (
		<>
			<div id="background"></div>
			<main style={{ maxWidth: 1100, margin: '15rem auto 0', padding: '0 1rem', position: 'relative', zIndex: 1 }}>
				<h1>User {id}</h1>
				<p>Placeholder user profile for {id}.</p>
			</main>
		</>
	);
}
