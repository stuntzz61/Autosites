import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import {
  Plus, FileText, Archive, Clock, CheckCircle, ChevronRight,
  Sparkles, TrendingUp, Zap, ArrowRight
} from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { profileApi } from '@/api/client'

export default function HomePage() {
  const navigate = useNavigate()
  const { user } = useAuthStore()
  const { haptic } = useTelegram()

  const { data: stats } = useQuery({
    queryKey: ['profile-stats'],
    queryFn: () => profileApi.stats().then(res => res.data),
  })

  const getGreeting = () => {
    const hour = new Date().getHours()
    if (hour < 12) return 'Доброе утро'
    if (hour < 18) return 'Добрый день'
    return 'Добрый вечер'
  }

  return (
    <div className="min-h-screen" style={{ background: 'var(--tg-theme-bg-color)' }}>
      {/* Premium Header */}
      <div className="relative overflow-hidden px-5 pt-10 pb-24" style={{ background: 'var(--tg-theme-bg-color)' }}>
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4 }}
          className="relative z-10"
        >
          <p className="text-sm font-medium mb-1" style={{ color: 'var(--tg-theme-hint-color)' }}>
            {getGreeting()}
          </p>
          <h1 className="text-3xl font-bold mb-2" style={{ color: 'var(--tg-theme-text-color)' }}>
            {user?.first_name}
          </h1>
          {user?.role === 'admin' && (
            <motion.span
              className="inline-flex items-center gap-1.5 text-xs font-semibold px-2.5 py-1 rounded-lg border"
              style={{
                background: 'rgba(59, 130, 246, 0.15)',
                borderColor: 'rgba(59, 130, 246, 0.3)',
                color: 'var(--accent-blue-light)'
              }}
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: 0.2 }}
            >
              <Sparkles className="w-3 h-3" />
              Администратор
            </motion.span>
          )}
        </motion.div>
      </div>

      {/* Content */}
      <div className="px-4 -mt-16 space-y-5 pb-8 relative z-10">
        {/* Main Action Card */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <button
            onClick={() => {
              haptic?.impactOccurred('medium')
              navigate('/requests/new')
            }}
            className="w-full group"
          >
            <div className="relative overflow-hidden rounded-3xl p-6 shadow-2xl transition-all duration-300 hover:scale-[1.02] active:scale-[0.98] border" style={{
              background: 'var(--surface-secondary)',
              borderColor: 'var(--border-accent)',
              boxShadow: '0 4px 32px -4px rgba(59, 130, 246, 0.2), inset 0 1px 0 0 rgba(255, 255, 255, 0.03)'
            }}>
              <div className="relative flex items-center gap-4">
                <div className="w-16 h-16 rounded-2xl flex items-center justify-center" style={{
                  background: 'rgba(59, 130, 246, 0.15)',
                  border: '1px solid rgba(59, 130, 246, 0.25)'
                }}>
                  <Plus className="w-8 h-8" style={{ color: 'var(--accent-blue-light)' }} />
                </div>
                <div className="flex-1 text-left">
                  <p className="font-bold text-xl mb-0.5" style={{ color: 'var(--tg-theme-text-color)' }}>Новая заявка</p>
                  <p className="text-sm" style={{ color: 'var(--tg-theme-hint-color)' }}>Создать сайт для клиента</p>
                </div>
                <div className="w-10 h-10 rounded-xl flex items-center justify-center transition-colors" style={{
                  background: 'rgba(59, 130, 246, 0.15)',
                  border: '1px solid rgba(59, 130, 246, 0.25)'
                }}>
                  <ArrowRight className="w-5 h-5 group-hover:translate-x-0.5 transition-transform" style={{ color: 'var(--accent-blue-light)' }} />
                </div>
              </div>
            </div>
          </button>
        </motion.div>

        {/* Stats Grid */}
        <motion.div
          className="grid grid-cols-2 gap-3"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
        >
          <StatsCard
            icon={<FileText className="w-5 h-5" />}
            value={stats?.total_requests || 0}
            label="Всего заявок"
            onClick={() => navigate('/requests')}
          />
          <StatsCard
            icon={<Clock className="w-5 h-5" />}
            value={stats?.pending_requests || 0}
            label="В работе"
            onClick={() => navigate('/requests?status=generating')}
            accent="blue"
          />
          <StatsCard
            icon={<CheckCircle className="w-5 h-5" />}
            value={stats?.completed_requests || 0}
            label="Завершено"
            accent="emerald"
          />
          <StatsCard
            icon={<TrendingUp className="w-5 h-5" />}
            value={stats?.this_week || 0}
            label="За неделю"
            accent="amber"
          />
        </motion.div>

        {/* Quick Actions */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
        >
          <p className="section-header">Быстрые действия</p>
          <div className="rounded-3xl overflow-hidden border" style={{
            background: 'var(--surface-secondary)',
            borderColor: 'var(--border-subtle)',
            boxShadow: '0 4px 24px -4px rgba(0, 0, 0, 0.4), inset 0 1px 0 0 rgba(255, 255, 255, 0.02)'
          }}>
            <QuickAction
              icon={<FileText className="w-5 h-5" />}
              iconBg="transparent"
              iconColor="var(--tg-theme-subtitle-text-color)"
              title="Все заявки"
              subtitle={`${stats?.total_requests || 0} заявок`}
              onClick={() => navigate('/requests')}
            />
            <div className="h-px ml-[68px]" style={{ background: 'var(--border-subtle)' }} />
            <QuickAction
              icon={<Zap className="w-5 h-5" />}
              iconBg="transparent"
              iconColor="var(--accent-blue)"
              title="Активные"
              subtitle={`${stats?.pending_requests || 0} в работе`}
              onClick={() => navigate('/requests?status=generating')}
            />
            <div className="h-px ml-[68px]" style={{ background: 'var(--border-subtle)' }} />
            <QuickAction
              icon={<Archive className="w-5 h-5" />}
              iconBg="transparent"
              iconColor="var(--tg-theme-subtitle-text-color)"
              title="Архив"
              subtitle="Завершённые проекты"
              onClick={() => navigate('/archive')}
            />
          </div>
        </motion.div>

        {/* Tip Card */}
        <motion.div
          className="rounded-2xl p-4 border"
          style={{
            background: 'linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, var(--surface-secondary) 100%)',
            borderColor: 'var(--border-accent)',
            boxShadow: '0 4px 24px -4px rgba(59, 130, 246, 0.15), inset 0 1px 0 0 rgba(255, 255, 255, 0.02)'
          }}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
        >
          <div className="flex items-start gap-3">
            <div className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0" style={{
              background: 'rgba(59, 130, 246, 0.15)'
            }}>
              <Sparkles className="w-4 h-4" style={{ color: 'var(--accent-blue-light)' }} />
            </div>
            <div>
              <p className="font-semibold text-sm mb-0.5" style={{ color: 'var(--tg-theme-text-color)' }}>Совет дня</p>
              <p className="text-xs leading-relaxed" style={{ color: 'var(--tg-theme-subtitle-text-color)' }}>
                Добавляйте качественные фото для лучшего результата генерации сайта
              </p>
            </div>
          </div>
        </motion.div>
      </div>
    </div>
  )
}

function StatsCard({
  icon,
  value,
  label,
  onClick,
  accent,
}: {
  icon: React.ReactNode
  value: number
  label: string
  onClick?: () => void
  accent?: 'blue' | 'amber' | 'emerald'
}) {
  const colors = {
    blue: {
      bg: 'rgba(59, 130, 246, 0.1)',
      text: 'var(--accent-blue)',
      value: 'var(--accent-blue-light)',
      border: 'rgba(59, 130, 246, 0.25)',
    },
    amber: {
      bg: 'rgba(245, 158, 11, 0.1)',
      text: '#F59E0B',
      value: '#FBBF24',
      border: 'rgba(245, 158, 11, 0.25)',
    },
    emerald: {
      bg: 'rgba(16, 185, 129, 0.1)',
      text: '#10B981',
      value: '#34D399',
      border: 'rgba(16, 185, 129, 0.25)',
    },
  }

  const color = accent ? colors[accent] : {
    bg: 'transparent',
    text: 'var(--tg-theme-subtitle-text-color)',
    value: 'var(--tg-theme-text-color)',
    border: 'var(--border-subtle)',
  }

  return (
    <motion.button
      onClick={onClick}
      className="rounded-2xl p-4 text-left transition-all duration-300 hover:-translate-y-1 active:scale-[0.98] border"
      style={{
        background: 'var(--surface-secondary)',
        borderColor: color.border,
        boxShadow: '0 4px 24px -4px rgba(0, 0, 0, 0.4), inset 0 1px 0 0 rgba(255, 255, 255, 0.02)'
      }}
      whileTap={{ scale: 0.98 }}
    >
      <div className="inline-flex items-center justify-center w-10 h-10 rounded-xl mb-3" style={{
        color: color.text,
        background: color.bg
      }}>
        {icon}
      </div>
      <p className="text-3xl font-bold mb-0.5" style={{ color: color.value }}>
        {value}
      </p>
      <p className="text-xs font-medium" style={{ color: 'var(--tg-theme-hint-color)' }}>{label}</p>
    </motion.button>
  )
}

function QuickAction({
  icon,
  iconBg,
  iconColor,
  title,
  subtitle,
  onClick
}: {
  icon: React.ReactNode
  iconBg: string
  iconColor: string
  title: string
  subtitle: string
  onClick: () => void
}) {
  return (
    <button onClick={onClick} className="flex items-center gap-4 p-4 w-full text-left transition-colors group hover:bg-white/[0.02]" style={{
      background: 'transparent'
    }}>
      <div className="w-12 h-12 rounded-2xl flex items-center justify-center transition-transform group-hover:scale-105" style={{
        color: iconColor
      }}>
        {icon}
      </div>
      <div className="flex-1 min-w-0">
        <p className="font-semibold" style={{ color: 'var(--tg-theme-text-color)' }}>{title}</p>
        <p className="text-sm" style={{ color: 'var(--tg-theme-hint-color)' }}>{subtitle}</p>
      </div>
      <ChevronRight className="w-5 h-5 transition-all group-hover:translate-x-0.5" style={{ color: 'var(--tg-theme-hint-color)' }} />
    </button>
  )
}
