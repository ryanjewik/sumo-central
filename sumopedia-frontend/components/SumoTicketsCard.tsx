"use client";

import React from "react";

const SumoTicketsCard: React.FC = () => (
  <div
    style={{
      background: 'linear-gradient(135deg, #f5e6c8 0%, #e0a3c2 100%)',
      border: '2px solid #563861',
      borderRadius: '1rem',
      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
      marginTop: '1.5rem',
      padding: '1.2rem 1rem',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'flex-start',
      gap: '0.8rem',
      width: '100%',
    }}
  >
    <span
      style={{
        display: 'inline-block',
        fontWeight: 'bold',
        fontSize: '1.05rem',
        color: '#fff',
        background: '#563861',
        borderRadius: '0.5rem',
        padding: '0.18rem 0.7rem',
        letterSpacing: '0.05em',
        marginBottom: '0.5rem',
        alignSelf: 'flex-start',
      }}
    >
      Buy Sumo Match Tickets
    </span>
    <a href="https://sumo.pia.jp/en/" target="_blank" rel="noopener noreferrer" style={{ color: '#388eec', fontWeight: 600, textDecoration: 'underline', fontSize: '1rem' }}>Official Sumo Ticket Site (English)</a>
    <a href="https://www.sumo.or.jp/EnTicket/" target="_blank" rel="noopener noreferrer" style={{ color: '#388eec', fontWeight: 600, textDecoration: 'underline', fontSize: '1rem' }}>Japan Sumo Association Tickets</a>
    <a href="https://www.viagogo.com/Sports-Tickets/Other-Sports/Sumo-Tickets" target="_blank" rel="noopener noreferrer" style={{ color: '#388eec', fontWeight: 600, textDecoration: 'underline', fontSize: '1rem' }}>Viagogo Sumo Tickets</a>
  </div>
);

export default SumoTicketsCard;