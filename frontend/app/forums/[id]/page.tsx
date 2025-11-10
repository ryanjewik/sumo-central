type Props = { params: { id: string } };

export default function ForumPage({ params }: Props) {
	const { id } = params;
	return (
		<main style={{padding: '2rem'}}>
			<h1>Forum {id}</h1>
			<p>Placeholder forum thread {id}.</p>
		</main>
	);
}
