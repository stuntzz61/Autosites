import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import {
  Plus, FileText, Clock, CheckCircle, ChevronRight,
  Sparkles, TrendingUp, ArrowRight, Crown, Zap
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
    <div className="min-h-screen" style={{ background: 'var(--bg-deep)' }}>
      {/* Premium Header with subtle gradient */}
      <div
        className="relative overflow-hidden px-5 pt-12 pb-8"
        style={{
          background: 'linear-gradient(180deg, var(--bg-deep) 0%, var(--bg-elevated) 100%)'
        }}
      >
        {/* Decorative background elements */}
        <div
          className="absolute top-0 right-0 w-64 h-64 rounded-full blur-3xl opacity-20"
          style={{ background: 'var(--accent-primary)' }}
        />

        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, ease: [0.4, 0, 0.2, 1] }}
          className="relative z-10"
        >
          <p
            className="text-sm font-medium mb-1"
            style={{ color: 'var(--text-subtle)' }}
          >
            {getGreeting()}
          </p>
          <h1
            className="text-3xl font-bold tracking-tight mb-3"
            style={{ color: 'var(--text-primary)' }}
          >
            {user?.first_name}
          </h1>

          {user?.role === 'admin' && (
            <motion.span
              className="badge-premium inline-flex items-center gap-1.5"
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: 0.2 }}
            >
              <Crown className="w-3.5 h-3.5" />
              Администратор
            </motion.span>
          )}
        </motion.div>
      </div>

      {/* Content */}
      <div className="px-4 space-y-5 pb-8 relative z-10">
        {/* Main Action Card - Create New Request */}
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
            <div
              className="relative overflow-hidden rounded-2xl p-5 transition-all duration-300 hover:scale-[1.01] active:scale-[0.98]"
              style={{
                background: 'linear-gradient(135deg, var(--bg-surface) 0%, var(--bg-elevated) 100%)',
                border: '1px solid var(--border-accent)',
                boxShadow: '0 4px 24px -4px rgba(59, 130, 246, 0.2), inset 0 1px 0 0 rgba(255, 255, 255, 0.03)'
              }}
            >
              {/* Subtle glow effect */}
              <div
                className="absolute -top-20 -right-20 w-40 h-40 rounded-full blur-3xl opacity-30 group-hover:opacity-50 transition-opacity"
                style={{ background: 'var(--accent-primary)' }}
              />

              <div className="relative flex items-center gap-4">
                <div
                  className="w-14 h-14 rounded-xl flex items-center justify-center transition-transform group-hover:scale-105"
                  style={{
                    background: 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 100%)',
                    boxShadow: '0 4px 16px -4px rgba(59, 130, 246, 0.5)'
                  }}
                >
                  <Plus className="w-7 h-7 text-white" />
                </div>
                <div className="flex-1 text-left">
                  <p
                    className="font-bold text-lg mb-0.5"
                    style={{ color: 'var(--text-primary)' }}
                  >
                    Новая заявка
                  </p>
                  <p
                    className="text-sm"
                    style={{ color: 'var(--text-muted)' }}
                  >
                    Создать сайт для клиента
                  </p>
                </div>
                <div
                  className="w-10 h-10 rounded-xl flex items-center justify-center transition-all group-hover:translate-x-1"
                  style={{
                    background: 'rgba(59, 130, 246, 0.1)',
                    border: '1px solid var(--border-accent)'
                  }}
                >
                  <ArrowRight
                    className="w-5 h-5"
                    style={{ color: 'var(--accent-primary-light)' }}
                  />
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
            accent="gold"
          />
        </motion.div>

        {/* Quick Actions */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
        >
          <p className="section-header">Быстрые действия</p>
          <div
            className="rounded-2xl overflow-hidden"
            style={{
              background: 'var(--bg-surface)',
              border: '1px solid var(--border-default)',
              boxShadow: '0 4px 20px rgba(0, 0, 0, 0.2)'
            }}
          >
            <QuickAction
              icon={<FileText className="w-5 h-5" />}
              title="Все заявки"
              subtitle={`${stats?.total_requests || 0} заявок`}
              onClick={() => navigate('/requests')}
            />
            <div className="divider" />
            <QuickAction
              icon={<Zap className="w-5 h-5" />}
              iconAccent
              title="Активные"
              subtitle={`${stats?.pending_requests || 0} в работе`}
              onClick={() => navigate('/requests?status=generating')}
            />
          </div>
        </motion.div>

        {/* Tip Card */}
        <motion.div
          className="rounded-2xl p-4"
          style={{
            background: 'linear-gradient(135deg, rgba(59, 130, 246, 0.08) 0%, var(--bg-surface) 100%)',
            border: '1px solid var(--border-accent)',
          }}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
        >
          <div className="flex items-start gap-3">
            <div
              className="w-9 h-9 rounded-lg flex items-center justify-center flex-shrink-0"
              style={{
                background: 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 100%)'
              }}
            >
              <Sparkles className="w-4.5 h-4.5 text-white" />
            </div>
            <div>
              <p
                className="font-semibold text-sm mb-0.5"
                style={{ color: 'var(--text-primary)' }}
              >
                Совет дня
              </p>
              <p
                className="text-sm leading-relaxed"
                style={{ color: 'var(--text-muted)' }}
              >
                Добавляйте качественные фото для лучшего результата генерации сайта
              </p>
            </div>
          </div>
        </motion.div>
      </div>
    </div>
  )
}

interface StatsCardProps {
  icon: React.ReactNode
  value: number
  label: string
  onClick?: () => void
  accent?: 'blue' | 'gold' | 'emerald'
}

function StatsCard({ icon, value, label, onClick, accent }: StatsCardProps) {
  const colors = {
    blue: {
      iconBg: 'rgba(59, 130, 246, 0.12)',
      iconColor: 'var(--accent-primary)',
      valueColor: 'var(--accent-primary-light)',
      border: 'rgba(59, 130, 246, 0.2)',
    },
    gold: {
      iconBg: 'rgba(245, 158, 11, 0.12)',
      iconColor: 'var(--gold-primary)',
      valueColor: 'var(--gold-light)',
      border: 'rgba(245, 158, 11, 0.2)',
    },
    emerald: {
      iconBg: 'rgba(16, 185, 129, 0.12)',
      iconColor: 'var(--success)',
      valueColor: 'var(--success-light)',
      border: 'rgba(16, 185, 129, 0.2)',
    },
  }

  const color = accent ? colors[accent] : {
    iconBg: 'var(--bg-tertiary)',
    iconColor: 'var(--text-muted)',
    valueColor: 'var(--text-primary)',
    border: 'var(--border-default)',
  }

  return (
    <motion.button
      onClick={onClick}
      className="rounded-2xl p-4 text-left transition-all duration-200 hover:-translate-y-0.5 active:scale-[0.98]"
      style={{
        background: 'var(--bg-surface)',
        border: `1px solid ${color.border}`,
        boxShadow: '0 4px 16px rgba(0, 0, 0, 0.15)'
      }}
      whileTap={{ scale: 0.98 }}
    >
      <div
        className="inline-flex items-center justify-center w-10 h-10 rounded-xl mb-3"
        style={{
          color: color.iconColor,
          background: color.iconBg
        }}
      >
        {icon}
      </div>
      <p
        className="text-2xl font-bold mb-0.5 number-animate"
        style={{ color: color.valueColor }}
      >
        {value}
      </p>
      <p
        className="text-xs font-medium"
        style={{ color: 'var(--text-subtle)' }}
      >
        {label}
      </p>
    </motion.button>
  )
}

interface QuickActionProps {
  icon: React.ReactNode
  title: string
  subtitle: string
  onClick: () => void
  iconAccent?: boolean
}

function QuickAction({ icon, title, subtitle, onClick, iconAccent }: QuickActionProps) {
  return (
    <button
      onClick={onClick}
      className="flex items-center gap-4 p-4 w-full text-left transition-all group hover:bg-white/[0.02] active:scale-[0.99]"
    >
      <div
        className="w-11 h-11 rounded-xl flex items-center justify-center transition-transform group-hover:scale-105"
        style={{
          color: iconAccent ? 'var(--accent-primary)' : 'var(--text-muted)',
          background: iconAccent ? 'rgba(59, 130, 246, 0.1)' : 'var(--bg-tertiary)'
        }}
      >
        {icon}
      </div>
      <div className="flex-1 min-w-0">
        <p
          className="font-semibold text-[15px]"
          style={{ color: 'var(--text-primary)' }}
        >
          {title}
        </p>
        <p
          className="text-sm"
          style={{ color: 'var(--text-subtle)' }}
        >
          {subtitle}
        </p>
      </div>
      <ChevronRight
        className="w-5 h-5 transition-all group-hover:translate-x-0.5"
        style={{ color: 'var(--text-subtle)' }}
      />
    </button>
  )
}
