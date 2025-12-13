import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Home, FileText, Archive, User, Shield, MessageSquare } from 'lucide-react'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { useEffect } from 'react'
import clsx from 'clsx'

const navItems = [
  { path: '/', icon: Home, label: 'Главная' },
  { path: '/requests', icon: FileText, label: 'Заявки' },
  { path: '/feedback', icon: MessageSquare, label: 'Связь' },
  { path: '/profile', icon: User, label: 'Профиль' },
]

export default function MainLayout() {
  const location = useLocation()
  const navigate = useNavigate()
  const { isAdmin } = useAuthStore()
  const { backButton, haptic } = useTelegram()

  // Handle back button
  useEffect(() => {
    if (!backButton) return

    const handleBack = () => {
      haptic?.impactOccurred('light')
      navigate(-1)
    }

    if (location.pathname !== '/') {
      backButton.show()
      backButton.onClick(handleBack)
    } else {
      backButton.hide()
    }

    return () => {
      backButton.offClick(handleBack)
    }
  }, [location.pathname, backButton, navigate, haptic])

  return (
    <div className="flex flex-col min-h-screen min-h-[100dvh] bg-tg-secondary-bg">
      {/* Content */}
      <main className="flex-1 overflow-auto main-content-with-nav">
        <motion.div
          key={location.pathname}
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -10 }}
          transition={{ duration: 0.2 }}
        >
          <Outlet />
        </motion.div>
      </main>

      {/* Bottom Navigation - Fixed with proper z-index and safe area */}
      <nav className="bottom-nav">
        <div className="flex items-center justify-around px-2 py-2">
          {navItems.map(({ path, icon: Icon, label }) => {
            const isActive = location.pathname === path ||
              (path !== '/' && location.pathname.startsWith(path))

            return (
              <button
                key={path}
                onClick={() => {
                  haptic?.selectionChanged()
                  navigate(path)
                }}
                className={clsx(
                  'flex flex-col items-center gap-0.5 px-3 py-1.5 rounded-xl transition-colors min-w-[64px]',
                  isActive ? 'text-tg-text' : 'text-tg-hint'
                )}
              >
                <Icon className="w-6 h-6" strokeWidth={isActive ? 2 : 1.5} />
                <span className="text-[10px] font-medium">{label}</span>
              </button>
            )
          })}

          {/* Admin button */}
          <button
            onClick={() => {
              haptic?.selectionChanged()
              navigate(isAdmin ? '/admin' : '/admin-login')
            }}
            className={clsx(
              'flex flex-col items-center gap-0.5 px-3 py-1.5 rounded-xl transition-colors min-w-[64px]',
              location.pathname.startsWith('/admin') ? 'text-tg-text' : 'text-tg-hint'
            )}
          >
            <Shield className="w-6 h-6" strokeWidth={location.pathname.startsWith('/admin') ? 2 : 1.5} />
            <span className="text-[10px] font-medium">Админ</span>
          </button>
        </div>
      </nav>
    </div>
  )
}
