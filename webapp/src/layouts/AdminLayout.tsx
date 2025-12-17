import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { LayoutDashboard, Users, FileText, BarChart3, Radio, ChevronLeft, MessageSquare, Globe, Link2, Users2, Shield, Crown } from 'lucide-react'
import { useTelegram } from '@/contexts/TelegramContext'
import { useEffect } from 'react'
import clsx from 'clsx'
import { useAuthStore, isOwnerRole, isDirectorRole } from '@/stores/authStore'

// Base nav items available to all supervisors
const baseNavItems = [
  { path: '/admin', icon: LayoutDashboard, label: 'Панель', exact: true },
  // Users management - grouped together
  { path: '/admin/managers', icon: Users, label: 'Менеджеры' },
  { path: '/admin/groups', icon: Users2, label: 'Группы' },
  { path: '/admin/invite-codes', icon: Link2, label: 'Инвайты' },
  // Content management
  { path: '/admin/requests', icon: FileText, label: 'Заявки' },
  { path: '/admin/sites', icon: Globe, label: 'Сайты' },
  // Communication & Analytics
  { path: '/admin/feedback', icon: MessageSquare, label: 'Обращения' },
  { path: '/admin/stats', icon: BarChart3, label: 'Статистика' },
  { path: '/admin/broadcast', icon: Radio, label: 'Рассылка' },
]

// Additional nav items for director (can manage supervisors) - inserted after managers
const directorNavItems = [
  { path: '/admin/supervisors', icon: Shield, label: 'Супервайзеры' },
]

// Additional nav items for owner (can manage directors and supervisors) - inserted after supervisors
const ownerNavItems = [
  { path: '/admin/directors', icon: Crown, label: 'Директоры' },
]

export default function AdminLayout() {
  const location = useLocation()
  const navigate = useNavigate()
  const { backButton, haptic } = useTelegram()
  const { user } = useAuthStore()

  // Get nav items based on role
  const getNavItems = () => {
    const items = [...baseNavItems]
    const managersIndex = items.findIndex(item => item.path === '/admin/managers')

    // Insert supervisors after managers (for director and owner)
    if (user && (isDirectorRole(user.role) || isOwnerRole(user.role))) {
      items.splice(managersIndex + 1, 0, ...directorNavItems)
    }

    // Insert directors after supervisors (for owner only)
    if (user && isOwnerRole(user.role)) {
      const supervisorsIndex = items.findIndex(item => item.path === '/admin/supervisors')
      if (supervisorsIndex !== -1) {
        items.splice(supervisorsIndex + 1, 0, ...ownerNavItems)
      } else {
        // If no supervisors, insert after managers
        items.splice(managersIndex + 1, 0, ...ownerNavItems)
      }
    }

    return items
  }

  const adminNavItems = getNavItems()

  useEffect(() => {
    if (!backButton) return

    const handleBack = () => {
      haptic?.impactOccurred('light')
      if (location.pathname === '/admin') {
        navigate('/')
      } else {
        navigate('/admin')
      }
    }

    backButton.show()
    backButton.onClick(handleBack)

    return () => {
      backButton.offClick(handleBack)
    }
  }, [location.pathname, backButton, navigate, haptic])

  return (
    <div className="flex flex-col min-h-screen bg-tg-secondary-bg">
      {/* Header */}
      <header className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator px-4 py-3 safe-top">
        <div className="flex items-center gap-3">
          <button
            onClick={() => navigate('/')}
            className="p-2 -ml-2 rounded-xl hover:bg-tg-secondary-bg"
          >
            <ChevronLeft className="w-5 h-5" />
          </button>
          <h1 className="text-lg font-semibold">Админ</h1>
        </div>
      </header>

      {/* Nav */}
      <div className="bg-tg-bg border-b border-tg-separator overflow-x-auto">
        <div className="flex items-center gap-1 px-4 py-2 min-w-max">
          {adminNavItems.map(({ path, icon: Icon, label, exact }) => {
            const isActive = exact ? location.pathname === path : location.pathname.startsWith(path)

            return (
              <button
                key={path}
                onClick={() => {
                  haptic?.selectionChanged()
                  navigate(path)
                }}
                className={clsx(
                  'flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-medium whitespace-nowrap',
                  isActive ? 'bg-black dark:bg-white text-white dark:text-black' : 'text-tg-hint'
                )}
              >
                <Icon className="w-4 h-4" />
                {label}
              </button>
            )
          })}
        </div>
      </div>

      <main className="flex-1 overflow-auto">
        <motion.div
          key={location.pathname}
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.2 }}
        >
          <Outlet />
        </motion.div>
      </main>
    </div>
  )
}
