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
        sans: ['Manrope', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'sans-serif'],
      },
      colors: {
        // Design system colors
        deep: {
          navy: '#0B1120',
          surface: '#1E2532',
          elevated: '#111827',
        },
        premium: {
          gold: {
            light: '#FCD34D',
            DEFAULT: '#F59E0B',
            dark: '#D97706',
          },
          purple: {
            light: '#C084FC',
            DEFAULT: '#8B5CF6',
            dark: '#7C3AED',
          },
        },
        // Legacy tg- colors for compatibility
        tg: {
          bg: 'var(--bg-deep)',
          text: 'var(--text-primary)',
          hint: 'var(--text-subtle)',
          link: 'var(--accent-primary)',
          button: 'var(--bg-surface)',
          'button-text': 'var(--text-primary)',
          'secondary-bg': 'var(--bg-elevated)',
          'header-bg': 'var(--bg-deep)',
          'section-bg': 'var(--bg-surface)',
          'section-header': 'var(--text-subtle)',
          accent: 'var(--accent-primary)',
          destructive: 'var(--error)',
          subtitle: 'var(--text-muted)',
          separator: 'var(--border-subtle)',
        },
      },
      borderRadius: {
        '4xl': '2rem',
        '5xl': '2.5rem',
      },
      boxShadow: {
        'premium': '0 4px 20px rgba(0, 0, 0, 0.25), inset 0 1px 0 0 rgba(255, 255, 255, 0.03)',
        'premium-lg': '0 8px 32px rgba(0, 0, 0, 0.35), inset 0 1px 0 0 rgba(255, 255, 255, 0.05)',
        'premium-glow': '0 0 30px -8px rgba(59, 130, 246, 0.25)',
        'gold-glow': '0 0 30px -8px rgba(245, 158, 11, 0.3)',
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
        'shimmer-gold': 'shimmerGold 3s ease-in-out infinite',
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
        shimmerGold: {
          '0%, 100%': { backgroundPosition: '0% 50%' },
          '50%': { backgroundPosition: '100% 50%' },
        },
      },
      backdropBlur: {
        '3xl': '64px',
      },
      transitionTimingFunction: {
        'bounce-in': 'cubic-bezier(0.68, -0.55, 0.265, 1.55)',
        'premium': 'cubic-bezier(0.4, 0, 0.2, 1)',
      },
      spacing: {
        '18': '4.5rem',
        '88': '22rem',
      },
    },
  },
  plugins: [],
}
