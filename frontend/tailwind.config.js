/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        pink: '#E0A3C2',
        green: '#A3E0B8',
        beige: '#E4E0BE',
        purple: '#563861',
        bordersecondary: '#d1b2c7',
        forumgreen: '#A3E0B8',
        navbarbg: '#E0A3C2',
        navbartext: '#563861',
        mainbg: '#fff',
        leftbarbg: '#ccc',
        rightbarbg: '#ccc',
        bracketbg: '#f9f9f9',
        bracketborder: '#000',
      },
      fontFamily: {
        sans: ['Space Grotesk', 'Inter', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'monospace'],
        heading: ['Space Grotesk', 'sans-serif'],
      },
      boxShadow: {
        xs: '0 1px 2px 0 rgba(0,0,0,0.05)',
      },
      borderRadius: {
        xl: '1rem',
      },
    },
  },
  plugins: [
    require('tailwindcss-react-aria-components'),
    require('tailwindcss-animate'),
  ],
  darkMode: 'class',
}
