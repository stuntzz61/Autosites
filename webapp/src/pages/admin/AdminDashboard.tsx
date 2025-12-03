import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import {
  Users, FileText, CheckCircle, Clock,
  UserPlus, ChevronRight
} from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { useTelegram } from '@/contexts/TelegramContext'

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

export default function AdminDashboard() {
  const navigate = useNavigate()
  const { haptic } = useTelegram()

  const { data: dashboard, isLoading } = useQuery({
    queryKey: ['admin-dashboard'],
    queryFn: () => adminApi.dashboard().then(res => res.data),
  })

  const { data: pending } = useQuery({
    queryKey: ['admin-pending'],
    queryFn: () => adminApi.pending.list().then(res => res.data),
  })

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        <div className="skeleton h-32 rounded-2xl" />
        <div className="grid grid-cols-2 gap-3">
          {[1, 2, 3, 4].map(i => (
            <div key={i} className="skeleton h-24 rounded-2xl" />
          ))}
        </div>
      </div>
    )
  }

  const stats = dashboard?.stats || {}
  const pendingCount = pending?.length || 0

  return (
    <motion.div
      className="p-4 space-y-6"
      variants={container}
      initial="hidden"
      animate="show"
    >
      {/* Pending Registrations Alert */}
      {pendingCount > 0 && (
        <motion.button
          variants={item}
          onClick={() => {
            haptic?.impactOccurred('medium')
            navigate('/admin/managers')
          }}
          className="w-full bg-amber-500/10 border border-amber-500/20 rounded-2xl p-4 text-left"
        >
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-amber-500/20">
              <UserPlus className="w-6 h-6 text-amber-600" />
            </div>
            <div className="flex-1">
              <p className="font-semibold text-amber-700 dark:text-amber-500">Новые регистрации</p>
              <p className="text-sm text-amber-600 dark:text-amber-400">{pendingCount} ожидают подтверждения</p>
            </div>
            <ChevronRight className="w-5 h-5 text-amber-600" />
          </div>
        </motion.button>
      )}

      {/* Stats Grid */}
      <motion.div variants={item} className="grid grid-cols-2 gap-3">
        <StatCard
          icon={<Users className="w-5 h-5" />}
          value={stats.total_managers || 0}
          label="Менеджеров"
          color="blue"
          onClick={() => navigate('/admin/managers')}
        />
        <StatCard
          icon={<FileText className="w-5 h-5" />}
          value={stats.total_requests || 0}
          label="Заявок"
          color="purple"
          onClick={() => navigate('/admin/requests')}
        />
        <StatCard
          icon={<CheckCircle className="w-5 h-5" />}
          value={stats.completed_requests || 0}
          label="Завершено"
          color="green"
        />
        <StatCard
          icon={<Clock className="w-5 h-5" />}
          value={stats.pending_requests || 0}
          label="В работе"
          color="amber"
        />
      </motion.div>

      {/* Quick Stats */}
      <motion.div variants={item}>
        <p className="section-header">Статистика за сегодня</p>
        <div className="bg-tg-section rounded-2xl p-4 border border-[#e4e6eb] dark:border-[#3e4042]">
          <div className="grid grid-cols-3 gap-4 text-center">
            <div>
              <p className="text-2xl font-bold text-tg-text">{stats.today_requests || 0}</p>
              <p className="text-xs text-tg-hint">Новых заявок</p>
            </div>
            <div>
              <p className="text-2xl font-bold text-tg-text">{stats.today_generated || 0}</p>
              <p className="text-xs text-tg-hint">Завершено</p>
            </div>
            <div>
              <p className="text-2xl font-bold text-tg-text">{stats.today_archived || 0}</p>
              <p className="text-xs text-tg-hint">Архивировано</p>
            </div>
          </div>
        </div>
      </motion.div>

      {/* Top Managers */}
      {dashboard?.top_managers?.length > 0 && (
        <motion.div variants={item}>
          <p className="section-header">Топ менеджеров</p>
          <div className="bg-tg-section rounded-2xl overflow-hidden border border-[#e4e6eb] dark:border-[#3e4042]">
            {dashboard.top_managers.slice(0, 5).map((manager: any, i: number) => (
              <div key={manager.id}>
                {i > 0 && <div className="divider" />}
                <div className="list-item">
                  <div className="w-8 h-8 rounded-full bg-gradient-to-br from-[#1877f2] to-[#0d65d9] flex items-center justify-center text-white font-bold text-sm">
                    {i + 1}
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="font-medium text-tg-text truncate">
                      {manager.first_name} {manager.last_name}
                    </p>
                    {manager.username && (
                      <p className="text-xs text-tg-hint">@{manager.username}</p>
                    )}
                  </div>
                  <div className="text-right">
                    <p className="font-semibold text-tg-text">{manager.request_count}</p>
                    <p className="text-xs text-tg-hint">заявок</p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </motion.div>
      )}

      {/* Status Distribution */}
      <motion.div variants={item}>
        <p className="section-header">По статусам</p>
        <div className="bg-tg-section rounded-2xl p-4 space-y-3 border border-[#e4e6eb] dark:border-[#3e4042]">
          <StatusBar label="Черновики" count={stats.draft_count || 0} total={stats.total_requests || 1} color="bg-[#65676b]" />
          <StatusBar label="Готовы к разработке" count={stats.ready_count || 0} total={stats.total_requests || 1} color="bg-[#42b72a]" />
          <StatusBar label="В работе" count={stats.generating_count || 0} total={stats.total_requests || 1} color="bg-[#1877f2]" />
          <StatusBar label="Завершено" count={stats.success_count || 0} total={stats.total_requests || 1} color="bg-emerald-500" />
          <StatusBar label="Ошибки" count={stats.error_count || 0} total={stats.total_requests || 1} color="bg-red-500" />
        </div>
      </motion.div>
    </motion.div>
  )
}

interface StatCardProps {
  icon: React.ReactNode
  value: number
  label: string
  color: 'blue' | 'green' | 'amber' | 'purple'
  onClick?: () => void
}

function StatCard({ icon, value, label, color, onClick }: StatCardProps) {
  const colors = {
    blue: 'bg-[#e7f3ff] text-[#1877f2] dark:bg-[#3e4042] dark:text-[#e4e6eb]',
    green: 'bg-emerald-500/10 text-emerald-500',
    amber: 'bg-orange-500/10 text-orange-500',
    purple: 'bg-violet-500/10 text-violet-500',
  }

  return (
    <motion.button
      onClick={onClick}
      className="bg-tg-section rounded-3xl p-4 text-left active:scale-[0.97] transition-all border border-[#e4e6eb] dark:border-[#3e4042]"
      whileTap={{ scale: 0.97 }}
    >
      <div className={`inline-flex p-2.5 rounded-xl ${colors[color]} mb-2`}>
        {icon}
      </div>
      <p className="text-2xl font-bold text-tg-text">{value}</p>
      <p className="text-xs text-tg-hint">{label}</p>
    </motion.button>
  )
}

function StatusBar({
  label,
  count,
  total,
  color,
}: {
  label: string
  count: number
  total: number
  color: string
}) {
  const percentage = total > 0 ? (count / total) * 100 : 0

  return (
    <div>
      <div className="flex justify-between text-sm mb-1">
        <span className="text-tg-text">{label}</span>
        <span className="text-tg-hint">{count}</span>
      </div>
      <div className="h-2 bg-tg-secondary-bg rounded-full overflow-hidden">
        <motion.div
          className={`h-full ${color} rounded-full`}
          initial={{ width: 0 }}
          animate={{ width: `${percentage}%` }}
          transition={{ duration: 0.5, ease: 'easeOut' }}
        />
      </div>
    </div>
  )
}
