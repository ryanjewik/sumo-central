type Props = { params: { id: string } };

export default function UserPage({ params }: Props) {
	const { id } = params;
	return (
		<main style={{padding: '2rem'}}>
			<h1>User {id}</h1>
			<p>Placeholder user profile for {id}.</p>
		</main>
	);
}
