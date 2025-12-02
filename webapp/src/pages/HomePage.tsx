import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { Plus, FileText, Archive, TrendingUp, Clock, CheckCircle, ChevronRight } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { profileApi } from '@/api/client'

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.1 },
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
      className="p-4 space-y-6"
      variants={container}
      initial="hidden"
      animate="show"
    >
      {/* Welcome Section */}
      <motion.div variants={item} className="text-center py-6">
        <motion.div
          className="inline-flex items-center justify-center w-20 h-20 rounded-3xl bg-gradient-to-br from-brand-500 to-brand-600 shadow-lg mb-4"
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
        >
          <svg
            className="w-10 h-10 text-white"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={1.5}
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"
            />
          </svg>
        </motion.div>
        <h1 className="text-2xl font-bold text-tg-text mb-1">
          Привет, {user?.first_name}!
        </h1>
        <p className="text-tg-hint">
          Создавайте сайты в один клик
        </p>
      </motion.div>

      {/* Quick Actions */}
      <motion.div variants={item}>
        <button
          onClick={handleNewRequest}
          className="w-full btn btn-primary text-lg py-4 shadow-lg shadow-brand-500/25"
        >
          <Plus className="w-6 h-6" />
          Создать заявку
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
          color="amber"
          onClick={() => navigate('/requests?status=pending')}
        />
        <StatsCard
          icon={<CheckCircle className="w-5 h-5" />}
          label="Завершено"
          value={stats?.completed_requests || 0}
          color="green"
        />
        <StatsCard
          icon={<TrendingUp className="w-5 h-5" />}
          label="За неделю"
          value={stats?.this_week || 0}
          color="purple"
        />
      </motion.div>

      {/* Quick Links */}
      <motion.div variants={item} className="space-y-2">
        <p className="section-header">Быстрый доступ</p>
        <div className="bg-tg-section rounded-2xl overflow-hidden">
          <QuickLink
            icon={<FileText className="w-5 h-5 text-brand-500" />}
            title="Мои заявки"
            subtitle={`${stats?.pending_requests || 0} активных`}
            onClick={() => navigate('/requests')}
          />
          <div className="divider" />
          <QuickLink
            icon={<Archive className="w-5 h-5 text-purple-500" />}
            title="Архив"
            subtitle="Завершённые заявки"
            onClick={() => navigate('/archive')}
          />
        </div>
      </motion.div>

      {/* Recent Activity */}
      <motion.div variants={item} className="space-y-2">
        <p className="section-header">Сегодня</p>
        <div className="bg-tg-section rounded-2xl p-4">
          {stats?.today ? (
            <p className="text-tg-text">
              Создано заявок: <span className="font-semibold">{stats.today}</span>
            </p>
          ) : (
            <p className="text-tg-hint text-center py-4">
              Пока нет активности
            </p>
          )}
        </div>
      </motion.div>
    </motion.div>
  )
}

interface StatsCardProps {
  icon: React.ReactNode
  label: string
  value: number
  color: 'blue' | 'green' | 'amber' | 'purple'
  onClick?: () => void
}

function StatsCard({ icon, label, value, color, onClick }: StatsCardProps) {
  const colors = {
    blue: 'bg-blue-500/10 text-blue-500',
    green: 'bg-green-500/10 text-green-500',
    amber: 'bg-amber-500/10 text-amber-500',
    purple: 'bg-purple-500/10 text-purple-500',
  }

  return (
    <motion.button
      onClick={onClick}
      className="bg-tg-section rounded-2xl p-4 text-left active:scale-[0.98] transition-transform"
      whileTap={{ scale: 0.98 }}
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
        <p className="font-medium text-tg-text">{title}</p>
        <p className="text-xs text-tg-hint">{subtitle}</p>
      </div>
      <ChevronRight className="w-5 h-5 text-tg-hint" />
    </button>
  )
}
