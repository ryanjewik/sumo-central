type Props = { params: { id: string } };

export default function BashoPage({ params }: Props) {
	const { id } = params;
	return (
		<main style={{padding: '2rem'}}>
			<h1>Basho {id}</h1>
			<p>Placeholder detail page for basho {id}.</p>
		</main>
	);
}
