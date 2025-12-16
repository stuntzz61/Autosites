import { useState, useRef, useEffect, ReactNode } from 'react'
import { motion, AnimatePresence } from 'framer-motion'

interface TooltipProps {
  content: string
  children: ReactNode
  position?: 'top' | 'bottom' | 'left' | 'right'
  delay?: number
  disabled?: boolean
}

export default function Tooltip({
  content,
  children,
  position = 'top',
  delay = 300,
  disabled = false
}: TooltipProps) {
  const [isVisible, setIsVisible] = useState(false)
  const [showDelayed, setShowDelayed] = useState(false)
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const hideTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    if (disabled) {
      setShowDelayed(false)
      return
    }

    if (isVisible) {
      // Clear any pending hide timeout
      if (hideTimeoutRef.current) {
        clearTimeout(hideTimeoutRef.current)
      }

      timeoutRef.current = setTimeout(() => {
        setShowDelayed(true)
      }, delay)
    } else {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current)
      }
      // Small delay before hiding to prevent flickering
      hideTimeoutRef.current = setTimeout(() => {
        setShowDelayed(false)
      }, 100)
    }

    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current)
      }
      if (hideTimeoutRef.current) {
        clearTimeout(hideTimeoutRef.current)
      }
    }
  }, [isVisible, delay, disabled])

  const positionClasses = {
    top: 'bottom-full left-1/2 -translate-x-1/2 mb-3',
    bottom: 'top-full left-1/2 -translate-x-1/2 mt-3',
    left: 'right-full top-1/2 -translate-y-1/2 mr-3',
    right: 'left-full top-1/2 -translate-y-1/2 ml-3',
  }

  const arrowClasses = {
    top: 'top-full left-1/2 -translate-x-1/2 border-t-zinc-800 dark:border-t-zinc-200 border-x-transparent border-b-transparent',
    bottom: 'bottom-full left-1/2 -translate-x-1/2 border-b-zinc-800 dark:border-b-zinc-200 border-x-transparent border-t-transparent',
    left: 'left-full top-1/2 -translate-y-1/2 border-l-zinc-800 dark:border-l-zinc-200 border-y-transparent border-r-transparent',
    right: 'right-full top-1/2 -translate-y-1/2 border-r-zinc-800 dark:border-r-zinc-200 border-y-transparent border-l-transparent',
  }

  if (disabled || !content) {
    return <>{children}</>
  }

  return (
    <div
      ref={containerRef}
      className="relative w-full"
      onMouseEnter={() => setIsVisible(true)}
      onMouseLeave={() => setIsVisible(false)}
      onFocus={() => setIsVisible(true)}
      onBlur={() => setIsVisible(false)}
      onTouchStart={() => {
        setIsVisible(true)
        // Auto-hide on mobile after 2 seconds
        setTimeout(() => setIsVisible(false), 2000)
      }}
    >
      {children}
      <AnimatePresence>
        {showDelayed && (
          <motion.div
            initial={{ opacity: 0, scale: 0.9, y: position === 'top' ? 4 : position === 'bottom' ? -4 : 0 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.9 }}
            transition={{ duration: 0.15, ease: 'easeOut' }}
            className={`absolute z-[9999] ${positionClasses[position]} pointer-events-none`}
          >
            <div className="bg-zinc-800 dark:bg-zinc-200 text-white dark:text-zinc-900 text-xs font-medium px-3 py-2 rounded-xl shadow-xl shadow-black/20 dark:shadow-black/10 whitespace-nowrap max-w-[250px] text-center">
              {content}
              <div
                className={`absolute w-0 h-0 border-[6px] ${arrowClasses[position]}`}
              />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

