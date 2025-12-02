import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { Plus, FileText, Archive, TrendingUp, Clock, CheckCircle, ChevronRight, Sparkles } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { profileApi } from '@/api/client'

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.08 },
  },
}

const item = {
  hidden: { opacity: 0, y: 20 },
  show: { opacity: 1, y: 0 },
}

export default function HomePage() {
  const navigate = useNavigate()
  const { user } = useAuthStore()
  const { haptic } = useTelegram()

  const { data: stats } = useQuery({
    queryKey: ['profile-stats'],
    queryFn: () => profileApi.stats().then(res => res.data),
  })

  const handleNewRequest = () => {
    haptic?.impactOccurred('medium')
    navigate('/requests/new')
  }

  return (
    <motion.div
      className="min-h-screen"
      variants={container}
      initial="hidden"
      animate="show"
    >
      {/* Hero Section with gradient */}
      <motion.div
        variants={item}
        className="relative overflow-hidden bg-gradient-to-br from-orange-500 via-orange-600 to-amber-600 px-6 pt-8 pb-12"
      >
        {/* Decorative elements */}
        <div className="absolute top-0 right-0 w-64 h-64 bg-white/10 rounded-full blur-3xl -translate-y-1/2 translate-x-1/2" />
        <div className="absolute bottom-0 left-0 w-48 h-48 bg-amber-400/20 rounded-full blur-3xl translate-y-1/2 -translate-x-1/2" />

        <div className="relative z-10">
          <div className="flex items-center gap-2 text-white/80 text-sm mb-2">
            <Sparkles className="w-4 h-4" />
            <span>AutoSites</span>
          </div>
          <h1 className="text-3xl font-bold text-white mb-1">
            Привет, {user?.first_name}!
          </h1>
          <p className="text-white/80">
            Создавайте сайты в один клик
          </p>
        </div>
      </motion.div>

      {/* Content */}
      <div className="px-4 -mt-6 space-y-5 pb-6">
        {/* Quick Action Card */}
        <motion.div variants={item}>
          <button
            onClick={handleNewRequest}
            className="w-full bg-white dark:bg-slate-800 rounded-3xl p-5 shadow-xl shadow-black/5 flex items-center gap-4 active:scale-[0.98] transition-transform border border-black/5 dark:border-white/5"
          >
            <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-orange-500 to-amber-500 flex items-center justify-center shadow-lg shadow-orange-500/25">
              <Plus className="w-7 h-7 text-white" />
            </div>
            <div className="flex-1 text-left">
              <p className="font-bold text-tg-text text-lg">Создать заявку</p>
              <p className="text-sm text-tg-hint">Новый сайт за минуту</p>
            </div>
            <ChevronRight className="w-5 h-5 text-tg-hint" />
          </button>
        </motion.div>

        {/* Stats Grid */}
        <motion.div variants={item} className="grid grid-cols-2 gap-3">
          <StatsCard
            icon={<FileText className="w-5 h-5" />}
            label="Всего заявок"
            value={stats?.total_requests || 0}
            color="orange"
            onClick={() => navigate('/requests')}
          />
          <StatsCard
            icon={<Clock className="w-5 h-5" />}
            label="В работе"
            value={stats?.pending_requests || 0}
            color="teal"
            onClick={() => navigate('/requests?status=pending')}
          />
          <StatsCard
            icon={<CheckCircle className="w-5 h-5" />}
            label="Завершено"
            value={stats?.completed_requests || 0}
            color="emerald"
          />
          <StatsCard
            icon={<TrendingUp className="w-5 h-5" />}
            label="За неделю"
            value={stats?.this_week || 0}
            color="violet"
          />
        </motion.div>

        {/* Quick Links */}
        <motion.div variants={item} className="space-y-2">
          <p className="section-header">Быстрый доступ</p>
          <div className="bg-tg-section rounded-3xl overflow-hidden border border-black/5 dark:border-white/5">
            <QuickLink
              icon={<FileText className="w-5 h-5 text-orange-500" />}
              title="Мои заявки"
              subtitle={`${stats?.pending_requests || 0} активных`}
              onClick={() => navigate('/requests')}
            />
            <div className="divider" />
            <QuickLink
              icon={<Archive className="w-5 h-5 text-violet-500" />}
              title="Архив"
              subtitle="Завершённые заявки"
              onClick={() => navigate('/archive')}
            />
          </div>
        </motion.div>

        {/* Today's Activity */}
        <motion.div variants={item} className="space-y-2">
          <p className="section-header">Активность</p>
          <div className="bg-tg-section rounded-3xl p-5 border border-black/5 dark:border-white/5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-tg-hint">Сегодня создано</p>
                <p className="text-3xl font-bold text-tg-text">{stats?.today || 0}</p>
              </div>
              <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-teal-400/20 to-teal-500/20 flex items-center justify-center">
                <Sparkles className="w-6 h-6 text-teal-500" />
              </div>
            </div>
          </div>
        </motion.div>
      </div>
    </motion.div>
  )
}

interface StatsCardProps {
  icon: React.ReactNode
  label: string
  value: number
  color: 'orange' | 'emerald' | 'teal' | 'violet'
  onClick?: () => void
}

function StatsCard({ icon, label, value, color, onClick }: StatsCardProps) {
  const colors = {
    orange: 'bg-orange-500/10 text-orange-500',
    emerald: 'bg-emerald-500/10 text-emerald-500',
    teal: 'bg-teal-500/10 text-teal-500',
    violet: 'bg-violet-500/10 text-violet-500',
  }

  return (
    <motion.button
      onClick={onClick}
      className="bg-tg-section rounded-3xl p-4 text-left active:scale-[0.97] transition-all border border-black/5 dark:border-white/5"
      whileTap={{ scale: 0.97 }}
    >
      <div className={`inline-flex p-2 rounded-xl ${colors[color]} mb-2`}>
        {icon}
      </div>
      <p className="text-2xl font-bold text-tg-text">{value}</p>
      <p className="text-xs text-tg-hint">{label}</p>
    </motion.button>
  )
}

interface QuickLinkProps {
  icon: React.ReactNode
  title: string
  subtitle: string
  onClick: () => void
}

function QuickLink({ icon, title, subtitle, onClick }: QuickLinkProps) {
  return (
    <button onClick={onClick} className="list-item w-full text-left">
      <div className="flex-shrink-0">{icon}</div>
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-tg-text">{title}</p>
        <p className="text-xs text-tg-hint">{subtitle}</p>
      </div>
      <ChevronRight className="w-5 h-5 text-tg-hint" />
    </button>
  )
}
