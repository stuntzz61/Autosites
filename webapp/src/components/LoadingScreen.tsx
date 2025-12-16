import { motion } from 'framer-motion'

export default function LoadingScreen() {
  return (
    <div
      className="flex items-center justify-center min-h-screen"
      style={{ background: 'var(--bg-deep)' }}
    >
      <motion.div
        className="flex flex-col items-center gap-5"
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.4 }}
      >
        {/* Premium Logo Container */}
        <motion.div
          className="relative"
          animate={{
            rotateY: [0, 5, -5, 0],
          }}
          transition={{
            duration: 3,
            repeat: Infinity,
            ease: 'easeInOut'
          }}
        >
          {/* Glow effect */}
          <div
            className="absolute inset-0 rounded-2xl blur-xl opacity-50"
            style={{ background: 'var(--accent-primary)' }}
          />

          {/* Logo container */}
          <div
            className="relative w-16 h-16 rounded-2xl flex items-center justify-center"
            style={{
              background: 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 100%)',
              boxShadow: '0 8px 32px -4px rgba(59, 130, 246, 0.5), inset 0 1px 0 0 rgba(255, 255, 255, 0.15)'
            }}
          >
            <svg
              className="w-8 h-8 text-white"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"
              />
            </svg>
          </div>
        </motion.div>

        {/* Premium loading dots */}
        <div className="flex gap-2">
          {[0, 1, 2].map((i) => (
            <motion.div
              key={i}
              className="w-2 h-2 rounded-full"
              style={{ background: 'var(--accent-primary)' }}
              animate={{
                scale: [1, 1.3, 1],
                opacity: [0.4, 1, 0.4],
              }}
              transition={{
                duration: 1,
                repeat: Infinity,
                delay: i * 0.15,
                ease: 'easeInOut'
              }}
            />
          ))}
        </div>

        {/* Loading text */}
        <motion.p
          className="text-sm font-medium"
          style={{ color: 'var(--text-subtle)' }}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.5 }}
        >
          Загрузка...
        </motion.p>
      </motion.div>
    </div>
  )
}
