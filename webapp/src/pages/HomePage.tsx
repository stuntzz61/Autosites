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
      <div className="relative overflow-hidden px-5 pt-10 pb-24" style={{ background: 'linear-gradient(145deg, #0f172a 0%, #0a0f1e 100%)' }}>
        {/* Decorative gradient orbs */}
        <div className="absolute top-0 right-0 w-64 h-64 bg-blue-500/10 rounded-full blur-3xl" />
        <div className="absolute bottom-0 left-0 w-48 h-48 bg-blue-600/10 rounded-full blur-3xl" />

        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4 }}
          className="relative z-10"
        >
          <p className="text-blue-300/70 text-sm font-medium mb-1">
            {getGreeting()}
          </p>
          <h1 className="text-3xl font-bold text-white mb-2">
            {user?.first_name}
          </h1>
          {user?.role === 'admin' && (
            <motion.span
              className="inline-flex items-center gap-1.5 text-xs font-semibold text-blue-300 bg-blue-500/20 px-2.5 py-1 rounded-lg border border-blue-500/30"
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
            <div className="relative overflow-hidden rounded-3xl p-6 shadow-2xl transition-all duration-300 hover:scale-[1.02] active:scale-[0.98]" style={{ background: 'linear-gradient(145deg, #1e3a8a 0%, #1e40af 50%, #1d4ed8 100%)' }}>
              {/* Glow effect */}
              <div className="absolute top-0 right-0 w-40 h-40 bg-gradient-to-br from-blue-400/30 to-cyan-400/20 rounded-full blur-3xl" />
              <div className="absolute bottom-0 left-0 w-32 h-32 bg-gradient-to-tr from-blue-300/20 via-sky-400/20 to-cyan-300/20 rounded-full blur-3xl" />

              <div className="relative flex items-center gap-4">
                <div className="w-16 h-16 rounded-2xl bg-white/15 backdrop-blur-sm flex items-center justify-center border border-white/20">
                  <Plus className="w-8 h-8 text-white" />
                </div>
                <div className="flex-1 text-left">
                  <p className="font-bold text-xl text-white mb-0.5">Новая заявка</p>
                  <p className="text-sm text-blue-200/80">Создать сайт для клиента</p>
                </div>
                <div className="w-10 h-10 rounded-xl bg-white/15 flex items-center justify-center group-hover:bg-white/25 transition-colors">
                  <ArrowRight className="w-5 h-5 text-white group-hover:translate-x-0.5 transition-transform" />
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
          <div className="rounded-3xl overflow-hidden border border-blue-500/10" style={{ background: 'linear-gradient(145deg, #0f172a 0%, #0d1424 100%)' }}>
            <QuickAction
              icon={<FileText className="w-5 h-5" />}
              iconBg="bg-blue-500/20"
              iconColor="text-blue-400"
              title="Все заявки"
              subtitle={`${stats?.total_requests || 0} заявок`}
              onClick={() => navigate('/requests')}
            />
            <div className="h-px ml-[68px]" style={{ background: 'linear-gradient(90deg, transparent 0%, rgba(59, 130, 246, 0.2) 50%, transparent 100%)' }} />
            <QuickAction
              icon={<Zap className="w-5 h-5" />}
              iconBg="bg-cyan-500/20"
              iconColor="text-cyan-400"
              title="Активные"
              subtitle={`${stats?.pending_requests || 0} в работе`}
              onClick={() => navigate('/requests?status=generating')}
            />
            <div className="h-px ml-[68px]" style={{ background: 'linear-gradient(90deg, transparent 0%, rgba(59, 130, 246, 0.2) 50%, transparent 100%)' }} />
            <QuickAction
              icon={<Archive className="w-5 h-5" />}
              iconBg="bg-slate-600/30"
              iconColor="text-slate-400"
              title="Архив"
              subtitle="Завершённые проекты"
              onClick={() => navigate('/archive')}
            />
          </div>
        </motion.div>

        {/* Tip Card */}
        <motion.div
          className="rounded-2xl p-4 border border-blue-500/20"
          style={{ background: 'linear-gradient(145deg, rgba(59, 130, 246, 0.1) 0%, rgba(30, 58, 138, 0.1) 100%)' }}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
        >
          <div className="flex items-start gap-3">
            <div className="w-8 h-8 rounded-lg bg-blue-500/20 flex items-center justify-center flex-shrink-0">
              <Sparkles className="w-4 h-4 text-blue-400" />
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
      bg: 'bg-blue-500/15',
      text: 'text-blue-400',
      value: 'text-blue-300',
      border: 'border-blue-500/20',
    },
    amber: {
      bg: 'bg-amber-500/15',
      text: 'text-amber-400',
      value: 'text-amber-300',
      border: 'border-amber-500/20',
    },
  }

  const color = accent ? colors[accent] : {
    bg: 'bg-slate-700/50',
    text: 'text-slate-400',
    value: 'text-slate-200',
    border: 'border-slate-700/50',
  }

  return (
    <motion.button
      onClick={onClick}
      className={`rounded-2xl p-4 text-left transition-all duration-300 hover:-translate-y-1 active:scale-[0.98] border ${color.border}`}
      style={{ background: 'linear-gradient(145deg, #1e293b 0%, #0f172a 100%)' }}
      whileTap={{ scale: 0.98 }}
    >
      <div className={`inline-flex items-center justify-center w-10 h-10 rounded-xl mb-3 ${color.bg} ${color.text}`}>
        {icon}
      </div>
      <p className={`text-3xl font-bold mb-0.5 ${color.value}`}>
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
    <button onClick={onClick} className="flex items-center gap-4 p-4 w-full text-left hover:bg-blue-500/5 transition-colors group">
      <div className={`w-12 h-12 rounded-2xl ${iconBg} ${iconColor} flex items-center justify-center transition-transform group-hover:scale-105`}>
        {icon}
      </div>
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-slate-100">{title}</p>
        <p className="text-sm text-slate-400">{subtitle}</p>
      </div>
      <ChevronRight className="w-5 h-5 text-slate-600 group-hover:translate-x-0.5 group-hover:text-blue-400 transition-all" />
    </button>
  )
}
