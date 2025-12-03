import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { Plus, FileText, Archive, TrendingUp, Clock, CheckCircle, ChevronRight, Globe } from 'lucide-react'
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
        className="relative overflow-hidden bg-gradient-to-br from-[#1877f2] via-[#166fe5] to-[#0d65d9] dark:from-[#242526] dark:via-[#18191a] dark:to-[#242526] px-6 pt-8 pb-12"
      >
        {/* Decorative elements */}
        <div className="absolute top-0 right-0 w-64 h-64 bg-white/10 rounded-full blur-3xl -translate-y-1/2 translate-x-1/2" />
        <div className="absolute bottom-0 left-0 w-48 h-48 bg-[#42b72a]/20 dark:bg-white/5 rounded-full blur-3xl translate-y-1/2 -translate-x-1/2" />

        <div className="relative z-10">
          <div className="flex items-center gap-2 text-white/80 text-sm mb-2">
            <Globe className="w-4 h-4" />
            <span>Webly</span>
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
            className="w-full bg-white dark:bg-[#242526] rounded-2xl p-5 shadow-xl shadow-black/5 dark:shadow-black/20 flex items-center gap-4 active:scale-[0.98] transition-transform border border-[#e4e6eb] dark:border-[#3e4042]"
          >
            <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-[#1877f2] to-[#0d65d9] dark:from-[#e4e6eb] dark:to-white flex items-center justify-center shadow-lg shadow-[#1877f2]/25 dark:shadow-white/10">
              <Plus className="w-7 h-7 text-white dark:text-[#18191a]" />
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
            color="blue"
            onClick={() => navigate('/requests')}
          />
          <StatsCard
            icon={<Clock className="w-5 h-5" />}
            label="В работе"
            value={stats?.pending_requests || 0}
            color="slate"
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
            color="indigo"
          />
        </motion.div>

        {/* Quick Links */}
        <motion.div variants={item} className="space-y-2">
          <p className="section-header">Быстрый доступ</p>
          <div className="bg-tg-section rounded-2xl overflow-hidden border border-[#e4e6eb] dark:border-[#3e4042]">
            <QuickLink
              icon={<FileText className="w-5 h-5 text-[#1877f2] dark:text-white" />}
              title="Мои заявки"
              subtitle={`${stats?.pending_requests || 0} активных`}
              onClick={() => navigate('/requests')}
            />
            <div className="divider" />
            <QuickLink
              icon={<Archive className="w-5 h-5 text-[#65676b] dark:text-[#b0b3b8]" />}
              title="Архив"
              subtitle="Завершённые заявки"
              onClick={() => navigate('/archive')}
            />
          </div>
        </motion.div>

        {/* Today's Activity */}
        <motion.div variants={item} className="space-y-2">
          <p className="section-header">Активность</p>
          <div className="bg-tg-section rounded-2xl p-5 border border-[#e4e6eb] dark:border-[#3e4042]">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-tg-hint">Сегодня создано</p>
                <p className="text-3xl font-bold text-tg-text">{stats?.today || 0}</p>
              </div>
              <div className="w-12 h-12 rounded-2xl bg-[#e7f3ff] dark:bg-[#3e4042] flex items-center justify-center">
                <Globe className="w-6 h-6 text-[#1877f2] dark:text-white" />
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
  color: 'blue' | 'emerald' | 'slate' | 'indigo'
  onClick?: () => void
}

function StatsCard({ icon, label, value, color, onClick }: StatsCardProps) {
  const colors = {
    blue: 'bg-[#e7f3ff] text-[#1877f2] dark:bg-[#3e4042] dark:text-white',
    emerald: 'bg-emerald-50 text-emerald-600 dark:bg-[#3e4042] dark:text-emerald-400',
    slate: 'bg-[#f0f2f5] text-[#65676b] dark:bg-[#3e4042] dark:text-[#b0b3b8]',
    indigo: 'bg-indigo-50 text-indigo-600 dark:bg-[#3e4042] dark:text-indigo-400',
  }

  return (
    <motion.button
      onClick={onClick}
      className="bg-tg-section rounded-2xl p-4 text-left active:scale-[0.97] transition-all border border-[#e4e6eb] dark:border-[#3e4042]"
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
