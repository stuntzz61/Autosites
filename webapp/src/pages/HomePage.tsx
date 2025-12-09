import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { Plus, FileText, Archive, Clock, CheckCircle, ChevronRight, Sparkles, TrendingUp } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { profileApi } from '@/api/client'

const containerVariants = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.1 }
  }
}

const itemVariants = {
  hidden: { opacity: 0, y: 20 },
  show: { opacity: 1, y: 0 }
}

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
      {/* Header with gradient accent */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-br from-zinc-900 via-zinc-800 to-zinc-900 dark:from-zinc-100 dark:via-zinc-200 dark:to-zinc-100" />
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top_right,_var(--tw-gradient-stops))] from-blue-500/20 via-transparent to-purple-500/20" />

        <div className="relative px-5 pt-10 pb-8">
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.4 }}
          >
            <p className="text-zinc-400 dark:text-zinc-600 text-sm font-medium mb-1">
              {getGreeting()} 👋
            </p>
            <h1 className="text-3xl font-bold text-white dark:text-zinc-900 mb-1">
              {user?.first_name}
            </h1>
            {user?.role === 'admin' && (
              <span className="inline-flex items-center gap-1 text-xs font-semibold text-blue-400 dark:text-blue-600 bg-blue-500/20 px-2 py-0.5 rounded-full">
                <Sparkles className="w-3 h-3" />
                Администратор
              </span>
            )}
          </motion.div>
        </div>
      </div>

      <motion.div
        className="px-5 -mt-4 space-y-5 pb-8"
        variants={containerVariants}
        initial="hidden"
        animate="show"
      >
        {/* Main Action Card */}
        <motion.div variants={itemVariants}>
          <button
            onClick={() => {
              haptic?.impactOccurred('medium')
              navigate('/requests/new')
            }}
            className="w-full group"
          >
            <div className="relative overflow-hidden bg-white dark:bg-zinc-900 rounded-2xl p-5 border border-zinc-200 dark:border-zinc-800 shadow-xl shadow-zinc-900/5 dark:shadow-black/20 transition-all duration-300 hover:shadow-2xl hover:-translate-y-0.5 active:scale-[0.98]">
              <div className="absolute top-0 right-0 w-32 h-32 bg-gradient-to-br from-blue-500/10 to-purple-500/10 rounded-full blur-2xl transform translate-x-8 -translate-y-8" />

              <div className="relative flex items-center gap-4">
                <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-zinc-900 to-zinc-700 dark:from-white dark:to-zinc-200 flex items-center justify-center shadow-lg">
                  <Plus className="w-7 h-7 text-white dark:text-zinc-900" />
                </div>
                <div className="flex-1 text-left">
                  <p className="font-bold text-lg text-tg-text">Новая заявка</p>
                  <p className="text-sm text-tg-hint">Создать сайт для клиента</p>
                </div>
                <ChevronRight className="w-5 h-5 text-tg-hint group-hover:translate-x-1 transition-transform" />
              </div>
            </div>
          </button>
        </motion.div>

        {/* Stats Grid */}
        <motion.div variants={itemVariants} className="grid grid-cols-2 gap-3">
          <StatsCard
            icon={<FileText className="w-5 h-5" />}
            value={stats?.total_requests || 0}
            label="Всего заявок"
            onClick={() => navigate('/requests')}
            delay={0}
          />
          <StatsCard
            icon={<Clock className="w-5 h-5" />}
            value={stats?.pending_requests || 0}
            label="В работе"
            onClick={() => navigate('/requests?status=pending')}
            accent="blue"
            delay={1}
          />
          <StatsCard
            icon={<CheckCircle className="w-5 h-5" />}
            value={stats?.completed_requests || 0}
            label="Завершено"
            accent="green"
            delay={2}
          />
          <StatsCard
            icon={<TrendingUp className="w-5 h-5" />}
            value={stats?.this_week || 0}
            label="За неделю"
            delay={3}
          />
        </motion.div>

        {/* Quick Links */}
        <motion.div variants={itemVariants}>
          <p className="section-header">Меню</p>
          <div className="bg-white dark:bg-zinc-900 rounded-2xl overflow-hidden border border-zinc-200 dark:border-zinc-800 shadow-sm">
            <QuickLink
              icon={<FileText className="w-5 h-5" />}
              title="Мои заявки"
              subtitle={`${stats?.pending_requests || 0} активных`}
              onClick={() => navigate('/requests')}
            />
            <div className="divider" />
            <QuickLink
              icon={<Archive className="w-5 h-5" />}
              title="Архив"
              subtitle="Завершённые проекты"
              onClick={() => navigate('/archive')}
            />
          </div>
        </motion.div>
      </motion.div>
    </div>
  )
}

function StatsCard({
  icon,
  value,
  label,
  onClick,
  accent,
  delay = 0
}: {
  icon: React.ReactNode
  value: number
  label: string
  onClick?: () => void
  accent?: 'blue' | 'green' | 'orange'
  delay?: number
}) {
  const accentClasses = {
    blue: 'text-blue-500',
    green: 'text-emerald-500',
    orange: 'text-orange-500',
  }

  return (
    <motion.button
      onClick={onClick}
      className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 rounded-2xl p-4 text-left shadow-sm hover:shadow-md transition-all duration-200 hover:-translate-y-0.5 active:scale-[0.98]"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: delay * 0.05 + 0.2, duration: 0.3 }}
    >
      <div className={`mb-3 ${accent ? accentClasses[accent] : 'text-tg-hint'}`}>
        {icon}
      </div>
      <p className={`text-3xl font-bold mb-0.5 ${accent ? accentClasses[accent] : 'text-tg-text'}`}>
        {value}
      </p>
      <p className="text-xs text-tg-hint font-medium">{label}</p>
    </motion.button>
  )
}

function QuickLink({ icon, title, subtitle, onClick }: {
  icon: React.ReactNode
  title: string
  subtitle: string
  onClick: () => void
}) {
  return (
    <button onClick={onClick} className="list-item w-full text-left group">
      <div className="w-10 h-10 rounded-xl bg-zinc-100 dark:bg-zinc-800 flex items-center justify-center text-tg-hint group-hover:bg-zinc-200 dark:group-hover:bg-zinc-700 transition-colors">
        {icon}
      </div>
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-tg-text">{title}</p>
        <p className="text-xs text-tg-hint">{subtitle}</p>
      </div>
      <ChevronRight className="w-5 h-5 text-tg-hint group-hover:translate-x-0.5 transition-transform" />
    </button>
  )
}
