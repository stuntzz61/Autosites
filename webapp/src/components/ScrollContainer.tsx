import { useState, useRef, useEffect, ReactNode } from 'react'
import { ChevronLeft, ChevronRight } from 'lucide-react'
import clsx from 'clsx'

interface ScrollContainerProps {
  children: ReactNode
  className?: string
  showArrowsOnDesktop?: boolean
}

export default function ScrollContainer({
  children,
  className = '',
  showArrowsOnDesktop = true
}: ScrollContainerProps) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [canScrollLeft, setCanScrollLeft] = useState(false)
  const [canScrollRight, setCanScrollRight] = useState(false)
  const [isDesktop, setIsDesktop] = useState(false)

  const checkScrollability = () => {
    const el = scrollRef.current
    if (!el) return

    const { scrollLeft, scrollWidth, clientWidth } = el
    setCanScrollLeft(scrollLeft > 0)
    setCanScrollRight(scrollLeft + clientWidth < scrollWidth - 1)
  }

  useEffect(() => {
    const checkDesktop = () => {
      setIsDesktop(window.innerWidth >= 768)
    }

    checkDesktop()
    window.addEventListener('resize', checkDesktop)
    return () => window.removeEventListener('resize', checkDesktop)
  }, [])

  useEffect(() => {
    const el = scrollRef.current
    if (!el) return

    checkScrollability()

    el.addEventListener('scroll', checkScrollability)

    // ResizeObserver to detect content changes
    const resizeObserver = new ResizeObserver(checkScrollability)
    resizeObserver.observe(el)

    return () => {
      el.removeEventListener('scroll', checkScrollability)
      resizeObserver.disconnect()
    }
  }, [children])

  const scroll = (direction: 'left' | 'right') => {
    const el = scrollRef.current
    if (!el) return

    const scrollAmount = el.clientWidth * 0.6
    el.scrollBy({
      left: direction === 'left' ? -scrollAmount : scrollAmount,
      behavior: 'smooth'
    })
  }

  const showLeftArrow = showArrowsOnDesktop && isDesktop && canScrollLeft
  const showRightArrow = showArrowsOnDesktop && isDesktop && canScrollRight

  return (
    <div className="relative group">
      {/* Left Arrow */}
      {showLeftArrow && (
        <button
          onClick={() => scroll('left')}
          className="absolute left-0 top-1/2 -translate-y-1/2 z-10 w-8 h-8 rounded-full flex items-center justify-center transition-all opacity-0 group-hover:opacity-100 hover:scale-110"
          style={{
            background: 'var(--surface-secondary)',
            border: '1px solid var(--border-subtle)',
            boxShadow: '0 4px 12px rgba(0, 0, 0, 0.3)',
            color: 'var(--tg-theme-text-color)'
          }}
          aria-label="Прокрутить влево"
        >
          <ChevronLeft className="w-5 h-5" />
        </button>
      )}

      {/* Scroll Container */}
      <div
        ref={scrollRef}
        className={clsx(
          'scroll-x-container',
          showLeftArrow && 'pl-2',
          showRightArrow && 'pr-2',
          className
        )}
      >
        {children}
      </div>

      {/* Right Arrow */}
      {showRightArrow && (
        <button
          onClick={() => scroll('right')}
          className="absolute right-0 top-1/2 -translate-y-1/2 z-10 w-8 h-8 rounded-full flex items-center justify-center transition-all opacity-0 group-hover:opacity-100 hover:scale-110"
          style={{
            background: 'var(--surface-secondary)',
            border: '1px solid var(--border-subtle)',
            boxShadow: '0 4px 12px rgba(0, 0, 0, 0.3)',
            color: 'var(--tg-theme-text-color)'
          }}
          aria-label="Прокрутить вправо"
        >
          <ChevronRight className="w-5 h-5" />
        </button>
      )}

      {/* Gradient Fades for scroll indication */}
      {showLeftArrow && (
        <div
          className="absolute left-0 top-0 bottom-0 w-8 pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity"
          style={{
            background: 'linear-gradient(to right, var(--tg-theme-bg-color), transparent)'
          }}
        />
      )}
      {showRightArrow && (
        <div
          className="absolute right-0 top-0 bottom-0 w-8 pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity"
          style={{
            background: 'linear-gradient(to left, var(--tg-theme-bg-color), transparent)'
          }}
        />
      )}
    </div>
  )
}




