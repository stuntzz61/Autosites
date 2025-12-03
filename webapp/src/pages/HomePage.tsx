import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { Plus, FileText, Archive, Clock, CheckCircle, ChevronRight } from 'lucide-react'
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

  return (
    <div className="min-h-screen bg-tg-bg">
      {/* Header */}
      <div className="px-5 pt-8 pb-6">
        <p className="text-tg-hint text-sm mb-1">Добро пожаловать</p>
        <h1 className="text-2xl font-bold text-tg-text">
          {user?.first_name}
        </h1>
      </div>

      {/* Main Action */}
      <div className="px-5 mb-6">
        <motion.button
          onClick={() => {
            haptic?.impactOccurred('medium')
            navigate('/requests/new')
          }}
          className="w-full bg-black dark:bg-white text-white dark:text-black rounded-2xl p-5 flex items-center gap-4 active:scale-[0.98] transition-transform"
          whileTap={{ scale: 0.98 }}
        >
          <div className="w-12 h-12 rounded-xl bg-white/20 dark:bg-black/20 flex items-center justify-center">
            <Plus className="w-6 h-6" />
          </div>
          <div className="flex-1 text-left">
            <p className="font-semibold text-lg">Новая заявка</p>
            <p className="text-sm opacity-70">Создать сайт</p>
          </div>
          <ChevronRight className="w-5 h-5 opacity-50" />
        </motion.button>
      </div>

      {/* Stats */}
      <div className="px-5 mb-6">
        <div className="grid grid-cols-2 gap-3">
          <motion.button
            onClick={() => navigate('/requests')}
            className="bg-tg-section border border-tg-separator rounded-2xl p-4 text-left"
            whileTap={{ scale: 0.97 }}
          >
            <FileText className="w-5 h-5 text-tg-hint mb-3" />
            <p className="text-2xl font-bold text-tg-text">{stats?.total_requests || 0}</p>
            <p className="text-xs text-tg-hint">Всего заявок</p>
          </motion.button>

          <motion.button
            onClick={() => navigate('/requests?status=pending')}
            className="bg-tg-section border border-tg-separator rounded-2xl p-4 text-left"
            whileTap={{ scale: 0.97 }}
          >
            <Clock className="w-5 h-5 text-tg-hint mb-3" />
            <p className="text-2xl font-bold text-tg-text">{stats?.pending_requests || 0}</p>
            <p className="text-xs text-tg-hint">В работе</p>
          </motion.button>

          <motion.button
            className="bg-tg-section border border-tg-separator rounded-2xl p-4 text-left"
            whileTap={{ scale: 0.97 }}
          >
            <CheckCircle className="w-5 h-5 text-tg-hint mb-3" />
            <p className="text-2xl font-bold text-tg-text">{stats?.completed_requests || 0}</p>
            <p className="text-xs text-tg-hint">Завершено</p>
          </motion.button>

          <motion.button
            className="bg-tg-section border border-tg-separator rounded-2xl p-4 text-left"
            whileTap={{ scale: 0.97 }}
          >
            <p className="text-xs text-tg-hint mb-3">За неделю</p>
            <p className="text-2xl font-bold text-tg-text">{stats?.this_week || 0}</p>
            <p className="text-xs text-tg-hint">заявок</p>
          </motion.button>
        </div>
      </div>

      {/* Quick Links */}
      <div className="px-5">
        <p className="section-header">Меню</p>
        <div className="bg-tg-section rounded-2xl overflow-hidden border border-tg-separator">
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
            subtitle="Завершённые"
            onClick={() => navigate('/archive')}
          />
        </div>
      </div>
    </div>
  )
}

function QuickLink({ icon, title, subtitle, onClick }: {
  icon: React.ReactNode
  title: string
  subtitle: string
  onClick: () => void
}) {
  return (
    <button onClick={onClick} className="list-item w-full text-left">
      <div className="text-tg-hint">{icon}</div>
      <div className="flex-1 min-w-0">
        <p className="font-medium text-tg-text">{title}</p>
        <p className="text-xs text-tg-hint">{subtitle}</p>
      </div>
      <ChevronRight className="w-5 h-5 text-tg-hint" />
    </button>
  )
}
