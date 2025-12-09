/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class',
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Outfit', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'sans-serif'],
      },
      colors: {
        tg: {
          bg: 'var(--tg-theme-bg-color)',
          text: 'var(--tg-theme-text-color)',
          hint: 'var(--tg-theme-hint-color)',
          link: 'var(--tg-theme-link-color)',
          button: 'var(--tg-theme-button-color)',
          'button-text': 'var(--tg-theme-button-text-color)',
          'secondary-bg': 'var(--tg-theme-secondary-bg-color)',
          'header-bg': 'var(--tg-theme-header-bg-color)',
          'section-bg': 'var(--tg-theme-section-bg-color)',
          'section-header': 'var(--tg-theme-section-header-text-color)',
          accent: 'var(--tg-theme-accent-text-color)',
          destructive: 'var(--tg-theme-destructive-text-color)',
          subtitle: 'var(--tg-theme-subtitle-text-color)',
          separator: 'var(--tg-theme-section-separator-color)',
        },
      },
      borderRadius: {
        '4xl': '2rem',
        '5xl': '2.5rem',
      },
      boxShadow: {
        'premium': '0 4px 24px -4px rgba(0, 0, 0, 0.12)',
        'premium-lg': '0 8px 40px -8px rgba(0, 0, 0, 0.15)',
        'inner-light': 'inset 0 1px 0 0 rgba(255, 255, 255, 0.1)',
        'glow-sm': '0 0 20px -5px currentColor',
        'glow': '0 0 40px -10px currentColor',
      },
      animation: {
        'fade-in': 'fadeIn 0.3s ease-out forwards',
        'fade-in-up': 'fadeInUp 0.4s ease-out forwards',
        'scale-in': 'scaleIn 0.3s ease-out forwards',
        'slide-up': 'slideUp 0.3s ease-out forwards',
        'float': 'float 3s ease-in-out infinite',
        'pulse-glow': 'pulseGlow 2s ease-in-out infinite',
        'shimmer': 'shimmer 1.5s ease-in-out infinite',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        fadeInUp: {
          '0%': { opacity: '0', transform: 'translateY(16px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        scaleIn: {
          '0%': { opacity: '0', transform: 'scale(0.95)' },
          '100%': { opacity: '1', transform: 'scale(1)' },
        },
        slideUp: {
          '0%': { transform: 'translateY(100%)' },
          '100%': { transform: 'translateY(0)' },
        },
        float: {
          '0%, 100%': { transform: 'translateY(0)' },
          '50%': { transform: 'translateY(-6px)' },
        },
        pulseGlow: {
          '0%, 100%': { boxShadow: '0 0 0 0 currentColor' },
          '50%': { boxShadow: '0 0 20px -5px currentColor' },
        },
        shimmer: {
          '0%': { backgroundPosition: '200% 0' },
          '100%': { backgroundPosition: '-200% 0' },
        },
      },
      backdropBlur: {
        '3xl': '64px',
      },
      transitionTimingFunction: {
        'bounce-in': 'cubic-bezier(0.68, -0.55, 0.265, 1.55)',
      },
    },
  },
  plugins: [],
}
