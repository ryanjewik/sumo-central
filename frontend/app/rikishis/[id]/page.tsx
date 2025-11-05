type Props = { params: { id: string } };

export default function RikishiPage({ params }: Props) {
	const { id } = params;
	return (
		<main style={{padding: '2rem'}}>
			<h1>Rikishi {id}</h1>
			<p>Placeholder detail page for rikishi {id}.</p>
		</main>
	);
}
