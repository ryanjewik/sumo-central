/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: '#E0A3C2', // pink
        secondary: '#A3E0B8', // green
        beige: '#E4E0BE', // beige
        purple: '#563861', // purple
        'border-secondary': '#d1b2c7', // light purple border
        'forum-green': '#A3E0B8', // forum post green
        'navbar-bg': '#E0A3C2', // navbar pink
        'navbar-text': '#563861', // navbar purple
        'main-bg': '#fff', // main content white
        'left-bar-bg': '#ccc', // left bar gray
        'right-bar-bg': '#ccc', // right bar gray
        'bracket-bg': '#f9f9f9', // bracket area bg
        'bracket-border': '#000', // bracket area border
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
  plugins: [],
  darkMode: 'class',
}
