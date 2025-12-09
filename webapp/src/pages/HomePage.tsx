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
      <div className="bg-zinc-900 dark:bg-zinc-100 px-5 pt-10 pb-24">
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4 }}
        >
          <p className="text-zinc-500 text-sm font-medium mb-1">
            {getGreeting()} 👋
          </p>
          <h1 className="text-3xl font-bold text-white dark:text-zinc-900 mb-2">
            {user?.first_name}
          </h1>
          {user?.role === 'admin' && (
            <motion.span
              className="inline-flex items-center gap-1.5 text-xs font-semibold text-amber-400 bg-amber-500/20 px-2.5 py-1 rounded-lg"
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
            <div className="relative overflow-hidden bg-gradient-to-br from-zinc-800 to-zinc-900 dark:from-white dark:to-zinc-100 rounded-3xl p-6 shadow-2xl transition-all duration-300 hover:scale-[1.02] active:scale-[0.98]">
              {/* Glow effect */}
              <div className="absolute top-0 right-0 w-40 h-40 bg-gradient-to-br from-blue-500/30 to-purple-500/30 rounded-full blur-3xl" />
              <div className="absolute bottom-0 left-0 w-32 h-32 bg-gradient-to-tr from-emerald-500/20 to-cyan-500/20 rounded-full blur-2xl" />

              <div className="relative flex items-center gap-4">
                <div className="w-16 h-16 rounded-2xl bg-white/10 dark:bg-black/5 backdrop-blur-sm flex items-center justify-center border border-white/20 dark:border-black/10">
                  <Plus className="w-8 h-8 text-white dark:text-zinc-900" />
                </div>
                <div className="flex-1 text-left">
                  <p className="font-bold text-xl text-white dark:text-zinc-900 mb-0.5">Новая заявка</p>
                  <p className="text-sm text-zinc-400 dark:text-zinc-600">Создать сайт для клиента</p>
                </div>
                <div className="w-10 h-10 rounded-xl bg-white/10 dark:bg-black/5 flex items-center justify-center group-hover:bg-white/20 dark:group-hover:bg-black/10 transition-colors">
                  <ArrowRight className="w-5 h-5 text-white dark:text-zinc-900 group-hover:translate-x-0.5 transition-transform" />
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
          <div className="bg-white dark:bg-zinc-900 rounded-3xl overflow-hidden border border-zinc-100 dark:border-zinc-800 shadow-lg">
            <QuickAction
              icon={<FileText className="w-5 h-5" />}
              iconBg="bg-blue-100 dark:bg-blue-900/30"
              iconColor="text-blue-600 dark:text-blue-400"
              title="Все заявки"
              subtitle={`${stats?.total_requests || 0} заявок`}
              onClick={() => navigate('/requests')}
            />
            <div className="h-px bg-zinc-100 dark:bg-zinc-800 ml-[68px]" />
            <QuickAction
              icon={<Zap className="w-5 h-5" />}
              iconBg="bg-amber-100 dark:bg-amber-900/30"
              iconColor="text-amber-600 dark:text-amber-400"
              title="Активные"
              subtitle={`${stats?.pending_requests || 0} в работе`}
              onClick={() => navigate('/requests?status=generating')}
            />
            <div className="h-px bg-zinc-100 dark:bg-zinc-800 ml-[68px]" />
            <QuickAction
              icon={<Archive className="w-5 h-5" />}
              iconBg="bg-zinc-100 dark:bg-zinc-800"
              iconColor="text-zinc-600 dark:text-zinc-400"
              title="Архив"
              subtitle="Завершённые проекты"
              onClick={() => navigate('/archive')}
            />
          </div>
        </motion.div>

        {/* Tip Card */}
        <motion.div
          className="bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-blue-900/20 dark:to-indigo-900/20 rounded-2xl p-4 border border-blue-100 dark:border-blue-800/30"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
        >
          <div className="flex items-start gap-3">
            <div className="w-8 h-8 rounded-lg bg-blue-500/10 flex items-center justify-center flex-shrink-0">
              <Sparkles className="w-4 h-4 text-blue-500" />
            </div>
            <div>
              <p className="font-semibold text-sm text-zinc-900 dark:text-zinc-100 mb-0.5">Совет дня</p>
              <p className="text-xs text-zinc-600 dark:text-zinc-400 leading-relaxed">
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
  accent?: 'blue' | 'emerald' | 'amber'
}) {
  const colors = {
    blue: {
      bg: 'bg-blue-50 dark:bg-blue-900/20',
      text: 'text-blue-600 dark:text-blue-400',
      value: 'text-blue-600 dark:text-blue-400',
    },
    emerald: {
      bg: 'bg-emerald-50 dark:bg-emerald-900/20',
      text: 'text-emerald-600 dark:text-emerald-400',
      value: 'text-emerald-600 dark:text-emerald-400',
    },
    amber: {
      bg: 'bg-amber-50 dark:bg-amber-900/20',
      text: 'text-amber-600 dark:text-amber-400',
      value: 'text-amber-600 dark:text-amber-400',
    },
  }

  const color = accent ? colors[accent] : null

  return (
    <motion.button
      onClick={onClick}
      className="bg-white dark:bg-zinc-900 border border-zinc-100 dark:border-zinc-800 rounded-2xl p-4 text-left shadow-sm hover:shadow-lg transition-all duration-300 hover:-translate-y-1 active:scale-[0.98]"
      whileTap={{ scale: 0.98 }}
    >
      <div className={`inline-flex items-center justify-center w-10 h-10 rounded-xl mb-3 ${color?.bg || 'bg-zinc-100 dark:bg-zinc-800'} ${color?.text || 'text-zinc-500'}`}>
        {icon}
      </div>
      <p className={`text-3xl font-bold mb-0.5 ${color?.value || 'text-tg-text'}`}>
        {value}
      </p>
      <p className="text-xs text-tg-hint font-medium">{label}</p>
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
    <button onClick={onClick} className="flex items-center gap-4 p-4 w-full text-left hover:bg-zinc-50 dark:hover:bg-zinc-800/50 transition-colors group">
      <div className={`w-12 h-12 rounded-2xl ${iconBg} ${iconColor} flex items-center justify-center transition-transform group-hover:scale-105`}>
        {icon}
      </div>
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-tg-text">{title}</p>
        <p className="text-sm text-tg-hint">{subtitle}</p>
      </div>
      <ChevronRight className="w-5 h-5 text-zinc-300 dark:text-zinc-600 group-hover:translate-x-0.5 transition-transform" />
    </button>
  )
}
