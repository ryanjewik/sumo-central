export const metadata = {
  title: 'Resources - Sumopedia',
};

export default function ResourcesPage() {
  return (
    <>
      {/* page background element (homepage uses this id) */}
  <div id="background"></div>
  <main style={{ maxWidth: 1100, margin: '15rem auto 0', padding: '0 1rem', position: 'relative', zIndex: 1, fontSize: '1.05rem' }}>
      <h1 style={{ color: '#563861' }}>Resources</h1>
      <p className="app-text">A curated list of resources about sumo: official sites, media, community APIs, and ticketing outlets.</p>
      {/* Grid of resource cards that matches site visual style */}
      <div style={{ marginTop: 28 }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '1rem' }}>
          {/** Official */}
          <ResourceCard
            title="Japan Sumo Association"
            href="https://www.sumo.or.jp/"
            description="Official site for tournaments, rikishi profiles, and announcements."
            color="#F5E6C8"
            icon={OfficialIcon}
          />

          <ResourceCard
            title="SumoDB"
            href="https://sumodb.sumogames.de/"
            description="Comprehensive sumo results & historical records (community database)."
            color="#F5E6C8"
            icon={DatabaseIcon}
          />

          {/** Wikimedia / Reference */}
          <ResourceCard
            title="Sumo — Wikipedia"
            href="https://en.wikipedia.org/wiki/Sumo"
            description="Background, rules, history, and glossary."
            color="#F5E6C8"
            icon={WikiIcon}
          />

          <ResourceCard
            title="Wikimedia Commons: Sumo"
            href="https://commons.wikimedia.org/wiki/Category:Sumo"
            description="Photos and media usable under free licenses."
            color="#F5E6C8"
            icon={ImageIcon}
          />

          {/** Community APIs & Data */}
          <ResourceCard
            title="GitHub: sumo API projects"
            href="https://github.com/search?q=sumo+api"
            description="Community projects, open APIs and example scrapers."
            color="#F5E6C8"
            icon={CodeIcon}
          />

          <ResourceCard
            title="SumoDB Data"
            href="https://sumodb.sumogames.de/"
            description="Some community sites provide data end-points or downloadable results."
            color="#F5E6C8"
            icon={ApiIcon}
          />

          {/** Ticketing & Events */}
          <ResourceCard
            title="Ticket Pia"
            href="https://t.pia.jp/"
            description="Major Japanese ticket distribution service."
            color="#F5E6C8"
            icon={TicketIcon}
          />

          <ResourceCard
            title="eplus"
            href="https://eplus.jp/"
            description="Event tickets and schedules."
            color="#F5E6C8"
            icon={TicketIcon}
          />

          <ResourceCard
            title="Lawson Ticket"
            href="https://l-tike.com/"
            description="Another common ticketing provider in Japan."
            color="#F5E6C8"
            icon={TicketIcon}
          />

          {/** Media */}
          <ResourceCard
            title="NHK World"
            href="https://www3.nhk.or.jp/nhkworld/en/"
            description="News coverage; English-language broadcasts and features about sumo."
            color="#F5E6C8"
            icon={NewsIcon}
          />

          <ResourceCard
            title="Twitter search: #sumo"
            href="https://twitter.com/search?q=sumo"
            description="Quick way to follow live commentary and fan discussion."
            color="#F5E6C8"
            icon={SocialIcon}
          />
          
          {/* User requested tiles */}
          <ResourceCard
            title="Sumo API"
            href="https://sumo-api.com/"
            description="Primary API used by this site — historical and live sumo match data."
            color="#F5E6C8"
            icon={ApiIcon}
          />

          <ResourceCard
            title="Ryan Hideo — Portfolio"
            href="https://ryanhideo.dev/"
            description="Author portfolio: projects, contact info, and background on this site." 
            color="#F5E6C8"
            icon={CodeIcon}
          />
        </div>

        <footer style={{ marginTop: 24, color: '#666' }}>
          <small>If you have a specific resource (API or guide) you'd like added, tell me the link and I can add it.</small>
        </footer>
      </div>
    </main>
    </>
  );
}

function ResourceCard({ title, href, description, color, icon: Icon }: { title: string; href: string; description: string; color?: string; icon: (props: { className?: string }) => any }) {
  return (
    <article
      role="article"
      aria-label={title}
      style={{
        background: color ?? '#F5E6C8',
        borderRadius: '1rem',
        boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
        border: '2px solid #563861',
        padding: '1rem 1rem',
        display: 'flex',
        gap: '0.8rem',
        alignItems: 'flex-start'
      }}
    >
      <div style={{ width: 44, height: 44, flex: '0 0 44px' }}>
        <Icon className="resource-icon" />
      </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.28rem' }}>
        <a href={href} target="_blank" rel="noopener noreferrer" style={{ color: '#563861', fontWeight: 700, textDecoration: 'none' }}>{title}</a>
        <div className="app-text" style={{ fontSize: '1.05rem' }}>{description}</div>
      </div>
    </article>
  );
}

const OfficialIcon = ({ className }: { className?: string }) => (
  <svg width="44" height="44" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
    <rect x="2" y="4" width="20" height="16" rx="2" stroke="#563861" strokeWidth="1.5" fill="#fff" />
    <path d="M6 8h12M6 12h12M6 16h8" stroke="#563861" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

const DatabaseIcon = ({ className }: { className?: string }) => (
  <svg width="44" height="44" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
    <ellipse cx="12" cy="6" rx="8" ry="3" stroke="#563861" strokeWidth="1.5" fill="#fff" />
    <path d="M4 6v6c0 1.657 3.582 3 8 3s8-1.343 8-3V6" stroke="#563861" strokeWidth="1.2" />
    <path d="M4 12v6c0 1.657 3.582 3 8 3s8-1.343 8-3v-6" stroke="#563861" strokeWidth="1.2" opacity="0.6" />
  </svg>
);

const WikiIcon = ({ className }: { className?: string }) => (
  <svg width="44" height="44" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
    <circle cx="12" cy="12" r="9" stroke="#563861" strokeWidth="1.5" fill="#fff" />
    <path d="M8 9h8M8 12h8M8 15h5" stroke="#563861" strokeWidth="1.2" strokeLinecap="round" />
  </svg>
);

const ImageIcon = ({ className }: { className?: string }) => (
  <svg width="44" height="44" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
    <rect x="3" y="4" width="18" height="16" rx="2" stroke="#563861" strokeWidth="1.5" fill="#fff" />
    <circle cx="8" cy="9" r="1" fill="#563861" />
    <path d="M21 19l-6-8-4 5-3-4-4 6" stroke="#563861" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" fill="none" />
  </svg>
);

const CodeIcon = ({ className }: { className?: string }) => (
  <svg width="44" height="44" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
    <path d="M8 18L3 12l5-6" stroke="#563861" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M16 6l5 6-5 6" stroke="#563861" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

const ApiIcon = ({ className }: { className?: string }) => (
  <svg width="44" height="44" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
    <path d="M12 2v20" stroke="#563861" strokeWidth="1.2" strokeLinecap="round" />
    <circle cx="7" cy="7" r="2" stroke="#563861" strokeWidth="1.2" fill="#fff" />
    <circle cx="17" cy="17" r="2" stroke="#563861" strokeWidth="1.2" fill="#fff" />
  </svg>
);

const TicketIcon = ({ className }: { className?: string }) => (
  <svg width="44" height="44" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
    <rect x="2" y="6" width="20" height="12" rx="2" stroke="#563861" strokeWidth="1.5" fill="#fff" />
    <path d="M7 6v12M17 6v12" stroke="#563861" strokeWidth="1.2" opacity="0.6" />
  </svg>
);

const NewsIcon = ({ className }: { className?: string }) => (
  <svg width="44" height="44" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
    <rect x="3" y="5" width="18" height="14" rx="1" stroke="#563861" strokeWidth="1.5" fill="#fff" />
    <path d="M7 9h10M7 13h6" stroke="#563861" strokeWidth="1.2" />
  </svg>
);

const SocialIcon = ({ className }: { className?: string }) => (
  <svg width="44" height="44" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
    <path d="M22 5.92c-.77.34-1.6.57-2.47.67a4.3 4.3 0 0 0 1.88-2.38c-.85.5-1.8.86-2.8 1.06A4.26 4.26 0 0 0 12.1 9.4c0 .33.04.65.11.96A12.12 12.12 0 0 1 3.16 6.3a4.26 4.26 0 0 0 1.32 5.67c-.63-.02-1.23-.2-1.75-.48v.05c0 2.06 1.46 3.78 3.4 4.18a4.3 4.3 0 0 1-1.93.07 4.27 4.27 0 0 0 3.99 2.96A8.55 8.55 0 0 1 2 19.55 12.06 12.06 0 0 0 8.29 21c7.55 0 11.68-6.26 11.68-11.69 0-.18-.01-.36-.02-.54A8.36 8.36 0 0 0 22 5.92z" stroke="#563861" strokeWidth="0.6" fill="#fff" />
  </svg>
);
