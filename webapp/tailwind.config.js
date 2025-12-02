/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        // Telegram theme colors
        tg: {
          bg: 'var(--tg-theme-bg-color, #faf9f7)',
          'secondary-bg': 'var(--tg-theme-secondary-bg-color, #f3f2ef)',
          text: 'var(--tg-theme-text-color, #1a1a2e)',
          hint: 'var(--tg-theme-hint-color, #6b7280)',
          link: 'var(--tg-theme-link-color, #0d9488)',
          button: 'var(--tg-theme-button-color, #f97316)',
          'button-text': 'var(--tg-theme-button-text-color, #ffffff)',
          'header-bg': 'var(--tg-theme-header-bg-color, #faf9f7)',
          accent: 'var(--tg-theme-accent-text-color, #0d9488)',
          destructive: 'var(--tg-theme-destructive-text-color, #dc2626)',
          subtitle: 'var(--tg-theme-subtitle-text-color, #9ca3af)',
          section: 'var(--tg-theme-section-bg-color, #ffffff)',
          'section-header': 'var(--tg-theme-section-header-text-color, #9ca3af)',
          separator: 'var(--tg-theme-section-separator-color, #e5e5e5)',
        },
        // Brand accent colors
        brand: {
          50: '#fff7ed',
          100: '#ffedd5',
          200: '#fed7aa',
          300: '#fdba74',
          400: '#fb923c',
          500: '#f97316',
          600: '#ea580c',
          700: '#c2410c',
          800: '#9a3412',
          900: '#7c2d12',
        },
        // Secondary accent (teal)
        accent: {
          50: '#f0fdfa',
          100: '#ccfbf1',
          200: '#99f6e4',
          300: '#5eead4',
          400: '#2dd4bf',
          500: '#14b8a6',
          600: '#0d9488',
          700: '#0f766e',
          800: '#115e59',
          900: '#134e4a',
        },
      },
      fontFamily: {
        sans: ['Manrope', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
      },
      animation: {
        'slide-up': 'slideUp 0.3s ease-out',
        'slide-down': 'slideDown 0.3s ease-out',
        'fade-in': 'fadeIn 0.2s ease-out',
        'scale-in': 'scaleIn 0.2s ease-out',
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'shimmer': 'shimmer 2s linear infinite',
      },
      keyframes: {
        slideUp: {
          '0%': { transform: 'translateY(10px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        slideDown: {
          '0%': { transform: 'translateY(-10px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        scaleIn: {
          '0%': { transform: 'scale(0.95)', opacity: '0' },
          '100%': { transform: 'scale(1)', opacity: '1' },
        },
        shimmer: {
          '0%': { backgroundPosition: '200% 0' },
          '100%': { backgroundPosition: '-200% 0' },
        },
      },
      borderRadius: {
        '4xl': '2rem',
      },
      boxShadow: {
        'glow': '0 0 20px rgba(249, 115, 22, 0.3)',
        'glow-teal': '0 0 20px rgba(20, 184, 166, 0.3)',
      },
    },
  },
  plugins: [],
}
