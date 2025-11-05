/** @type {import('tailwindcss').Config} */
import tailwindReactAria from 'tailwindcss-react-aria-components';
import tailwindAnimate from 'tailwindcss-animate';

const config = {
  content: [
    "./app/**/*.{ts,tsx,js,jsx,mdx}",
    "./components/**/*.{ts,tsx,js,jsx,mdx}",
    "./providers/**/*.{ts,tsx,js,jsx,mdx}",
    "./lib/**/*.{ts,tsx,js,jsx,mdx}",
    "./styles/**/*.{css}"
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
      boxShadow: { xs: '0 1px 2px 0 rgba(0,0,0,0.05)' },
      borderRadius: { xl: '1rem' },
    },
  },
  plugins: [
    tailwindReactAria,
    tailwindAnimate,
  ],
  darkMode: 'class',
};

export default config;
