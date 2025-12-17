import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Home, FileText, User, Shield, MessageSquare, LayoutDashboard, Users, BarChart3, Users2, LucideIcon } from 'lucide-react'
import { useAuthStore, isOwnerRole, isDirectorRole, isSupervisorRole } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { useEffect, useMemo } from 'react'

// Navigation for regular managers
const managerNavItems = [
  { path: '/', icon: Home, label: 'Главная' },
  { path: '/requests', icon: FileText, label: 'Заявки' },
  { path: '/feedback', icon: MessageSquare, label: 'Связь' },
  { path: '/profile', icon: User, label: 'Профиль' },
]

// Navigation for supervisors
const supervisorNavItems = [
  { path: '/', icon: Home, label: 'Главная' },
  { path: '/admin', icon: LayoutDashboard, label: 'Панель' },
  { path: '/admin/managers', icon: Users, label: 'Менеджеры' },
  { path: '/admin/stats', icon: BarChart3, label: 'Статистика' },
  { path: '/profile', icon: User, label: 'Профиль' },
]

// Navigation for directors and owners
const directorNavItems = [
  { path: '/admin', icon: LayoutDashboard, label: 'Дашборд' },
  { path: '/admin/overview', icon: FileText, label: 'Заявки' },
  { path: '/admin/managers', icon: Users, label: 'Менеджеры' },
  { path: '/admin/groups', icon: Users2, label: 'Группы' },
  { path: '/admin/stats', icon: BarChart3, label: 'Аналитика' },
]

export default function MainLayout() {
  const location = useLocation()
  const navigate = useNavigate()
  const { isAdmin, user } = useAuthStore()
  const { backButton, haptic } = useTelegram()

  const isOwner = !!(user && isOwnerRole(user.role))
  const isDirector = !!(user && isDirectorRole(user.role))
  const isSupervisor = !!(user && isSupervisorRole(user.role) && !isDirectorRole(user.role))

  // Get navigation items based on role
  const navItems = useMemo(() => {
    if (isOwner || isDirector) {
      return directorNavItems
    }
    if (isSupervisor) {
      return supervisorNavItems
    }
    return managerNavItems
  }, [isOwner, isDirector, isSupervisor])

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
    <div
      className="flex flex-col min-h-screen min-h-[100dvh] relative"
      style={{ background: 'var(--bg-deep)' }}
    >
      {/* Main Content */}
      <main className="flex-1 overflow-y-auto overflow-x-hidden main-content-with-nav">
        <motion.div
          key={location.pathname}
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.25, ease: [0.4, 0, 0.2, 1] }}
        >
          <Outlet />
        </motion.div>
      </main>

      {/* Premium Bottom Navigation */}
      <nav className="bottom-nav">
        <div className="flex items-center justify-around w-full px-2">
          {navItems.map(({ path, icon: Icon, label }) => {
            const isActive = path === '/'
              ? location.pathname === path
              : location.pathname === path || location.pathname.startsWith(path + '/')

            return (
              <NavButton
                key={path}
                onClick={() => {
                  haptic?.selectionChanged()
                  navigate(path)
                }}
                icon={Icon}
                label={label}
                isActive={isActive}
                isHighlighted={isOwner || isDirector}
                isOwner={isOwner}
              />
            )
          })}

          {/* Admin button - only for managers (not for owner/director/supervisor who have admin in nav) */}
          {!isOwner && !isDirector && !isSupervisor && (
            <NavButton
              onClick={() => {
                haptic?.selectionChanged()
                navigate(isAdmin ? '/admin' : '/admin-login')
              }}
              icon={Shield}
              label="Админ"
              isActive={location.pathname.startsWith('/admin')}
              isHighlighted={false}
            />
          )}
        </div>
      </nav>
    </div>
  )
}

interface NavButtonProps {
  onClick: () => void
  icon: LucideIcon
  label: string
  isActive: boolean
  isHighlighted?: boolean
  isOwner?: boolean
}

function NavButton({ onClick, icon: Icon, label, isActive, isHighlighted, isOwner }: NavButtonProps) {
  // Cold color for Owner, gold for Director
  const highlightColor = isOwner ? 'rgb(59, 130, 246)' : 'rgb(251, 191, 36)' // blue-500 for owner, yellow-400 for director
  const highlightColorAlpha = isOwner ? 'rgba(59, 130, 246, 0.7)' : 'rgba(251, 191, 36, 0.7)'
  const highlightGlow = isOwner ? 'rgba(59, 130, 246, 0.2)' : 'rgba(251, 191, 36, 0.2)'
  const highlightGlowAlpha = isOwner ? 'rgba(59, 130, 246, 0.8)' : 'rgba(251, 191, 36, 0.8)'

  return (
    <motion.button
      onClick={onClick}
      className="relative flex flex-col items-center gap-1 px-3 py-2 min-w-[60px] rounded-xl transition-colors"
      whileTap={{ scale: 0.92 }}
      style={{
        color: isActive
          ? (isHighlighted ? highlightColor : 'var(--accent-primary-light)')
          : (isHighlighted ? highlightColorAlpha : 'var(--text-subtle)')
      }}
    >
      {/* Active indicator glow */}
      {isActive && (
        <motion.div
          className="absolute inset-0 rounded-xl"
          layoutId="navIndicator"
          initial={false}
          transition={{ type: 'spring', stiffness: 500, damping: 35 }}
          style={{
            background: isHighlighted
              ? `radial-gradient(ellipse at center bottom, ${highlightGlow} 0%, transparent 70%)`
              : 'radial-gradient(ellipse at center bottom, rgba(59, 130, 246, 0.15) 0%, transparent 70%)'
          }}
        />
      )}

      <div className="relative">
        <Icon
          className="w-6 h-6 transition-all duration-200"
          strokeWidth={isActive ? 2.2 : 1.8}
        />
        {/* Active dot indicator */}
        {isActive && (
          <motion.div
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            className="absolute -bottom-1 left-1/2 -translate-x-1/2 w-1 h-1 rounded-full"
            style={{ background: isHighlighted ? highlightColor : 'var(--accent-primary)' }}
          />
        )}
      </div>

      <span
        className="text-[10px] font-semibold transition-colors truncate max-w-[48px]"
        style={{
          color: isActive
            ? (isHighlighted ? highlightColor : 'var(--accent-primary-light)')
            : (isHighlighted ? highlightGlowAlpha : 'var(--text-subtle)'),
          opacity: isActive ? 1 : 0.8
        }}
      >
        {label}
      </span>
    </motion.button>
  )
}
