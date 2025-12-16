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
    <div className="min-h-screen bg-tg-bg">
      {/* Premium Header */}
      <div className="relative overflow-hidden px-5 pt-10 pb-24" style={{ background: 'linear-gradient(145deg, #1a1f2e 0%, #0f1419 100%)' }}>
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4 }}
          className="relative z-10"
        >
          <p className="text-slate-400 text-sm font-medium mb-1">
            {getGreeting()}
          </p>
          <h1 className="text-3xl font-bold text-slate-100 mb-2">
            {user?.first_name}
          </h1>
          {user?.role === 'admin' && (
            <motion.span
              className="inline-flex items-center gap-1.5 text-xs font-semibold text-slate-300 px-2.5 py-1 rounded-lg border"
              style={{
                background: 'rgba(100, 116, 139, 0.1)',
                borderColor: 'rgba(148, 163, 184, 0.2)'
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
              background: 'linear-gradient(145deg, #334155 0%, #1e293b 100%)',
              borderColor: 'rgba(148, 163, 184, 0.2)'
            }}>
              <div className="relative flex items-center gap-4">
                <div className="w-16 h-16 rounded-2xl flex items-center justify-center border" style={{
                  background: 'rgba(100, 116, 139, 0.1)',
                  borderColor: 'rgba(148, 163, 184, 0.2)'
                }}>
                  <Plus className="w-8 h-8 text-slate-200" />
                </div>
                <div className="flex-1 text-left">
                  <p className="font-bold text-xl text-slate-100 mb-0.5">Новая заявка</p>
                  <p className="text-sm text-slate-400">Создать сайт для клиента</p>
                </div>
                <div className="w-10 h-10 rounded-xl flex items-center justify-center transition-colors border" style={{
                  background: 'rgba(100, 116, 139, 0.1)',
                  borderColor: 'rgba(148, 163, 184, 0.2)'
                }}>
                  <ArrowRight className="w-5 h-5 text-slate-300 group-hover:translate-x-0.5 transition-transform" />
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
            accent="blue"
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
            background: 'linear-gradient(145deg, #1e293b 0%, #1a1f2e 100%)',
            borderColor: 'rgba(100, 116, 139, 0.15)'
          }}>
            <QuickAction
              icon={<FileText className="w-5 h-5" />}
              iconBg="rgba(148, 163, 184, 0.12)"
              iconColor="#cbd5e1"
              title="Все заявки"
              subtitle={`${stats?.total_requests || 0} заявок`}
              onClick={() => navigate('/requests')}
            />
            <div className="h-px ml-[68px]" style={{ background: 'linear-gradient(90deg, transparent 0%, rgba(100, 116, 139, 0.2) 50%, transparent 100%)' }} />
            <QuickAction
              icon={<Zap className="w-5 h-5" />}
              iconBg="rgba(203, 213, 225, 0.12)"
              iconColor="#e2e8f0"
              title="Активные"
              subtitle={`${stats?.pending_requests || 0} в работе`}
              onClick={() => navigate('/requests?status=generating')}
            />
            <div className="h-px ml-[68px]" style={{ background: 'linear-gradient(90deg, transparent 0%, rgba(100, 116, 139, 0.2) 50%, transparent 100%)' }} />
            <QuickAction
              icon={<Archive className="w-5 h-5" />}
              iconBg="rgba(100, 116, 139, 0.12)"
              iconColor="#94a3b8"
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
            background: 'linear-gradient(145deg, rgba(100, 116, 139, 0.08) 0%, rgba(71, 85, 105, 0.08) 100%)',
            borderColor: 'rgba(100, 116, 139, 0.2)'
          }}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
        >
          <div className="flex items-start gap-3">
            <div className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0" style={{
              background: 'rgba(148, 163, 184, 0.12)'
            }}>
              <Sparkles className="w-4 h-4" style={{ color: '#cbd5e1' }} />
            </div>
            <div>
              <p className="font-semibold text-sm text-slate-100 mb-0.5">Совет дня</p>
              <p className="text-xs text-slate-400 leading-relaxed">
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
  accent?: 'blue' | 'amber'
}) {
  const colors = {
    blue: {
      bg: 'rgba(148, 163, 184, 0.12)',
      text: '#cbd5e1',
      value: '#e2e8f0',
      border: 'rgba(148, 163, 184, 0.2)',
    },
    amber: {
      bg: 'rgba(245, 158, 11, 0.12)',
      text: '#fbbf24',
      value: '#fcd34d',
      border: 'rgba(245, 158, 11, 0.2)',
    },
  }

  const color = accent ? colors[accent] : {
    bg: 'rgba(100, 116, 139, 0.1)',
    text: '#94a3b8',
    value: '#cbd5e1',
    border: 'rgba(100, 116, 139, 0.2)',
  }

  return (
    <motion.button
      onClick={onClick}
      className="rounded-2xl p-4 text-left transition-all duration-300 hover:-translate-y-1 active:scale-[0.98] border"
      style={{
        background: 'linear-gradient(145deg, #1e293b 0%, #1a1f2e 100%)',
        borderColor: color.border
      }}
      whileTap={{ scale: 0.98 }}
    >
      <div className="inline-flex items-center justify-center w-10 h-10 rounded-xl mb-3" style={{
        background: color.bg,
        color: color.text
      }}>
        {icon}
      </div>
      <p className="text-3xl font-bold mb-0.5" style={{ color: color.value }}>
        {value}
      </p>
      <p className="text-xs text-slate-400 font-medium">{label}</p>
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
    <button onClick={onClick} className="flex items-center gap-4 p-4 w-full text-left hover:bg-slate-700/30 transition-colors group">
      <div className="w-12 h-12 rounded-2xl flex items-center justify-center transition-transform group-hover:scale-105" style={{
        background: iconBg,
        color: iconColor
      }}>
        {icon}
      </div>
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-slate-100">{title}</p>
        <p className="text-sm text-slate-400">{subtitle}</p>
      </div>
      <ChevronRight className="w-5 h-5 text-slate-600 group-hover:translate-x-0.5 group-hover:text-slate-400 transition-all" />
    </button>
  )
}
