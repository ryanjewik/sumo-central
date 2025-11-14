export const metadata = {
  title: 'About - Sumopedia',
};

export default function AboutPage() {
  return (
    <>
  <div id="background"></div>
  <main style={{ maxWidth: 1100, margin: '15rem auto 0', padding: '0 1rem', position: 'relative', zIndex: 1, fontSize: '1.05rem' }}>
      <h1 style={{ color: '#563861' }}>About Sumo — A Newcomer's Guide</h1>

      <section>
        <p className="app-text">Welcome — this page is a concise, practical primer for people new to sumo wrestling. Below you'll find the essential terminology, how matches work, the ranking system, common kimarite (winning techniques), tournament structure, etiquette, and a few resources to learn more.</p>
      </section>

      <section style={{ marginTop: 20 }}>
        <h2>Quick overview</h2>
        <p className="app-text">Sumo is Japan's national sport: a one-on-one full-contact wrestling discipline with deep traditions. Two rikishi (wrestlers) face each other on a circular clay ring called the <strong>dohyo</strong>. A match ends quickly — often in a few seconds — when one wrestler forces the other out of the ring or makes any part of the opponent other than the soles of the feet touch the ground.</p>
      </section>

      <section style={{ marginTop: 20 }}>
        <h2>Match basics</h2>
        <ul className="app-text">
          <li><strong>Objective:</strong> Force your opponent out of the dohyo or make them touch the ground with any body part besides the soles of their feet.</li>
          <li><strong>Dohyo:</strong> A circular ring about 4.55m in diameter, made of clay and covered with a layer of sand.</li>
          <li><strong>Mawashi:</strong> The heavy belt each rikishi wears — it is both clothing and a grip point for throws and force-outs.</li>
          <li><strong>Gyoji:</strong> The referee who announces the start of the match and declares the winner; there are also ringside judges (<em>shimpan</em>) who can intervene.</li>
        </ul>

        <figure style={{ marginTop: 12, background: '#F5E6C8', border: '2px solid #563861', borderRadius: '12px', padding: '0.6rem', maxWidth: 520 }}>
          <a href="/sumo1.webp" target="_blank" rel="noopener noreferrer"><img src="/sumo1.webp" alt="Sumo match photo" style={{ width: '100%', borderRadius: '8px' }} /></a>
          <figcaption className="app-text" style={{ marginTop: 8, fontSize: '0.9rem' }}>Match photo: rikishi facing off on the dohyo.</figcaption>
        </figure>

      </section>

      <section style={{ marginTop: 20 }}>
        <h2>Ranks and divisions</h2>
        <p className="app-text">Professional sumo is organized into divisions (from the top down):</p>
        <ol className="app-text">
          <li><strong>Makuuchi</strong> — the top division. Wrestlers here are the ones you see on the main broadcast. Within makuuchi there are the <em>sanyaku</em> ranks (champion-level and titled ranks):</li>
          <ul className="app-text">
            <li><strong>Yokozuna</strong> — grand champion. A yokozuna is promoted for consistent tournament victories and cannot be demoted; they are expected to retire if performance declines.</li>
            <li><strong>Ozeki</strong> — champion rank below yokozuna; can be demoted after poor results but can return with strong performance.</li>
            <li><strong>Sekiwake</strong> and <strong>Komusubi</strong> — junior titled ranks within sanyaku.</li>
            <li><strong>Maegashira</strong> — the numbered ranks that make up the rest of makuuchi.</li>
          </ul>
          <li><strong>Juryo</strong> — the second-highest professional division; together with makuuchi they form the salaried ranks (<em>sekitori</em>).</li>
          <li><strong>Makushita</strong>, <strong>Sandanme</strong>, <strong>Jonidan</strong>, and <strong>Jonokuchi</strong> — the lower divisions where younger and lower-ranked wrestlers compete.</li>
        </ol>
        <p className="app-text">Promotions and demotions are determined by the official ranking list called the <strong>banzuke</strong>, which is published before each official tournament (basho).</p>

        <figure style={{ marginTop: 12, background: '#F5E6C8', border: '2px solid #563861', borderRadius: '12px', padding: '0.6rem', maxWidth: 420 }}>
          <a href="/sumo-rankings.webp" target="_blank" rel="noopener noreferrer"><img src="/sumo-rankings.webp" alt="Sumo ranks chart" style={{ width: '100%', borderRadius: '8px' }} /></a>
          <figcaption className="app-text" style={{ marginTop: 8, fontSize: '0.9rem' }}>Rank chart: yokozuna, ozeki, sekiwake, komusubi, maegashira, and lower divisions.</figcaption>
        </figure>
      </section>

      <section style={{ marginTop: 20 }}>
        <h2>Terminology & common words</h2>
        <dl className="app-text">
          <dt>Rikishi</dt>
          <dd>Wrestler. Often referred to by their <strong>shikona</strong> (ring name).</dd>

          <dt>Basho</dt>
          <dd>An official tournament (six major basho are held each year in Japan).</dd>

          <dt>Kachi-koshi / Make-koshi</dt>
          <dd><em>Kachi-koshi</em> is a winning record in a tournament (more wins than losses); <em>make-koshi</em> is a losing record. These determine promotion/demotion.</dd>

          <dt>Heya</dt>
          <dd>Stable or training house where wrestlers live and train under an <em>oyakata</em> (stablemaster).</dd>

          <dt>Shimpan</dt>
          <dd>Ringside judges who can call for a mono-ii (video review) if the outcome is unclear.</dd>
        </dl>
      </section>

      <section style={{ marginTop: 20 }}>
        <h2>Kimarite (winning techniques)</h2>
        <p className="app-text">A <strong>kimarite</strong> is the officially recorded method by which a rikishi wins a bout. The Japan Sumo Association recognizes many kimarite — below are several common ones and short descriptions.</p>

        <figure style={{ marginTop: 12, background: '#F5E6C8', border: '2px solid #563861', borderRadius: '12px', padding: '0.6rem', maxWidth: 420 }}>
          <a href="/kimarite.webp" target="_blank" rel="noopener noreferrer"><img src="/kimarite.webp" alt="Kimarite examples" style={{ width: '100%', borderRadius: '8px' }} /></a>
          <figcaption className="app-text" style={{ marginTop: 8, fontSize: '0.9rem' }}>Kimarite (techniques): common winning moves such as yorikiri, oshidashi, and throws.</figcaption>
        </figure>
        <ul className="app-text">
          <li><strong>Yorikiri</strong> — force out while maintaining a grip on the opponent's belt (a common victory).</li>
          <li><strong>Oshidashi</strong> — push out without necessarily grabbing the belt.</li>
          <li><strong>Hatakikomi</strong> — slap down; the attacker sidesteps and slaps the opponent down to the clay.</li>
          <li><strong>Hikiotoshi</strong> — pull down; use of a backward pull to topple the opponent.</li>
          <li><strong>Uwatenage</strong> — overarm throw using the outer grip on the mawashi.</li>
          <li><strong>Shitatenage</strong> — underarm throw using the inner grip on the mawashi.</li>
          <li><strong>Tsukidashi</strong> / <strong>Tsukiotoshi</strong> — thrusting techniques that push or topple the opponent.</li>
        </ul>
  <p className="app-text">There are many more kimarite covering throws, trips, and sacrifices.</p>
      </section>

      <section style={{ marginTop: 20 }}>
        <h2>Tournament structure</h2>
        <p className="app-text">A professional basho lasts 15 days. Makuuchi wrestlers fight once per day, and the rikishi's win/loss record over those 15 days determines their movement on the next banzuke. The wrestler with the best record in the top division wins the tournament (yusho). Playoffs may be used to break ties.</p>
      </section>

      <section style={{ marginTop: 20 }}>
        <figure style={{ marginTop: 12, background: '#F5E6C8', border: '2px solid #563861', borderRadius: '12px', padding: '0.6rem', maxWidth: 520 }}>
          <a href="/sumo2.webp" target="_blank" rel="noopener noreferrer"><img src="/sumo2.webp" alt="Sumo match action" style={{ width: '100%', borderRadius: '8px' }} /></a>
          <figcaption className="app-text" style={{ marginTop: 8, fontSize: '0.9rem' }}>Match action: example of a pushing/thrusting technique in play.</figcaption>
        </figure>
      </section>

      <section style={{ marginTop: 20 }}>
        <h2>Etiquette & viewing tips</h2>
        <ul className="app-text">
          <li>Stand for the ring-entering ceremony and the dohyo-iri when yokozuna or other top wrestlers perform their ritual entrances if you're in the venue.</li>
          <li>Photography rules vary by venue — follow signage and staff instructions.</li>
          <li>When watching on TV, commentators often use Japanese terms — this page's glossary should help decode common phrases.</li>
        </ul>

        <figure style={{ marginTop: 12, background: '#F5E6C8', border: '2px solid #563861', borderRadius: '12px', padding: '0.6rem', maxWidth: 520 }}>
          <a href="/sumo3.webp" target="_blank" rel="noopener noreferrer"><img src="/sumo3.webp" alt="Ring-side view" style={{ width: '100%', borderRadius: '8px' }} /></a>
          <figcaption className="app-text" style={{ marginTop: 8, fontSize: '0.9rem' }}>Ring-side view: crowd and ritual elements around the dohyo.</figcaption>
        </figure>
      </section>

      <section style={{ marginTop: 20 }}>
        <h2>Further reading & data sources</h2>
        <p className="app-text">If you'd like more detail or data to explore, these sites are great starting points:</p>
        <ul className="app-text">
          <li><a href="https://en.wikipedia.org/wiki/Sumo" target="_blank" rel="noopener noreferrer">Sumo — Wikipedia</a> — overview and historical background.</li>
          <li><a href="https://sumodb.sumogames.de/" target="_blank" rel="noopener noreferrer">SumoDB</a> — match records and historical stats.</li>
          <li><a href="https://sumo-api.com/" target="_blank" rel="noopener noreferrer">Sumo API</a> — the API used by this site for match and rikishi data.</li>
        </ul>
      </section>

      
    </main>
    </>
  );
}
