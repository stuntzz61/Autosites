import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { Users, FileText, CheckCircle, Clock, UserPlus, ChevronRight } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { useTelegram } from '@/contexts/TelegramContext'

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
        <div className="skeleton h-24 rounded-2xl" />
        <div className="grid grid-cols-2 gap-3">
          {[1, 2, 3, 4].map(i => <div key={i} className="skeleton h-24 rounded-2xl" />)}
        </div>
      </div>
    )
  }

  const stats = dashboard?.stats || {}
  const pendingCount = pending?.length || 0

  return (
    <div className="p-4 space-y-6">
      {/* Pending Alert */}
      {pendingCount > 0 && (
        <motion.button
          onClick={() => {
            haptic?.impactOccurred('medium')
            navigate('/admin/managers')
          }}
          className="w-full bg-amber-500/10 border border-amber-500/20 rounded-2xl p-4 text-left"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
        >
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-amber-500/20">
              <UserPlus className="w-6 h-6 text-amber-600" />
            </div>
            <div className="flex-1">
              <p className="font-semibold text-amber-700 dark:text-amber-500">Новые регистрации</p>
              <p className="text-sm text-amber-600 dark:text-amber-400">{pendingCount} ожидают</p>
            </div>
            <ChevronRight className="w-5 h-5 text-amber-600" />
          </div>
        </motion.button>
      )}

      {/* Stats */}
      <div className="grid grid-cols-2 gap-3">
        <StatCard
          icon={<Users className="w-5 h-5" />}
          value={stats.total_managers || 0}
          label="Менеджеров"
          onClick={() => navigate('/admin/managers')}
        />
        <StatCard
          icon={<FileText className="w-5 h-5" />}
          value={stats.total_requests || 0}
          label="Заявок"
          onClick={() => navigate('/admin/requests')}
        />
        <StatCard
          icon={<CheckCircle className="w-5 h-5" />}
          value={stats.completed_requests || 0}
          label="Завершено"
        />
        <StatCard
          icon={<Clock className="w-5 h-5" />}
          value={stats.pending_requests || 0}
          label="В работе"
        />
      </div>

      {/* Today */}
      <div>
        <p className="section-header">Сегодня</p>
        <div className="bg-tg-section rounded-2xl p-4 border border-tg-separator">
          <div className="grid grid-cols-3 gap-4 text-center">
            <div>
              <p className="text-2xl font-bold">{stats.today_requests || 0}</p>
              <p className="text-xs text-tg-hint">Новых</p>
            </div>
            <div>
              <p className="text-2xl font-bold">{stats.today_generated || 0}</p>
              <p className="text-xs text-tg-hint">Готово</p>
            </div>
            <div>
              <p className="text-2xl font-bold">{stats.today_archived || 0}</p>
              <p className="text-xs text-tg-hint">Архив</p>
            </div>
          </div>
        </div>
      </div>

      {/* Top Managers */}
      {dashboard?.top_managers?.length > 0 && (
        <div>
          <p className="section-header">Топ менеджеров</p>
          <div className="bg-tg-section rounded-2xl border border-tg-separator">
            {dashboard.top_managers.slice(0, 5).map((manager: any, i: number) => (
              <div key={manager.id}>
                {i > 0 && <div className="divider" />}
                <div className="list-item">
                  <div className="w-8 h-8 rounded-full bg-black dark:bg-white text-white dark:text-black flex items-center justify-center font-bold text-sm">
                    {i + 1}
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="font-medium truncate">{manager.first_name} {manager.last_name}</p>
                    {manager.username && <p className="text-xs text-tg-hint">@{manager.username}</p>}
                  </div>
                  <div className="text-right">
                    <p className="font-semibold">{manager.request_count}</p>
                    <p className="text-xs text-tg-hint">заявок</p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* By Status */}
      <div>
        <p className="section-header">По статусам</p>
        <div className="bg-tg-section rounded-2xl p-4 space-y-3 border border-tg-separator">
          <StatusBar label="Черновики" count={stats.draft_count || 0} total={stats.total_requests || 1} />
          <StatusBar label="Готовы" count={stats.ready_count || 0} total={stats.total_requests || 1} />
          <StatusBar label="В работе" count={stats.generating_count || 0} total={stats.total_requests || 1} />
          <StatusBar label="Завершено" count={stats.success_count || 0} total={stats.total_requests || 1} />
          <StatusBar label="Ошибки" count={stats.error_count || 0} total={stats.total_requests || 1} />
        </div>
      </div>
    </div>
  )
}

function StatCard({ icon, value, label, onClick }: {
  icon: React.ReactNode
  value: number
  label: string
  onClick?: () => void
}) {
  return (
    <motion.button
      onClick={onClick}
      className="bg-tg-section rounded-2xl p-4 text-left border border-tg-separator active:scale-[0.97]"
      whileTap={{ scale: 0.97 }}
    >
      <div className="text-tg-hint mb-2">{icon}</div>
      <p className="text-2xl font-bold">{value}</p>
      <p className="text-xs text-tg-hint">{label}</p>
    </motion.button>
  )
}

function StatusBar({ label, count, total }: { label: string; count: number; total: number }) {
  const pct = total > 0 ? (count / total) * 100 : 0

  return (
    <div>
      <div className="flex justify-between text-sm mb-1">
        <span>{label}</span>
        <span className="text-tg-hint">{count}</span>
      </div>
      <div className="h-2 bg-tg-secondary-bg rounded-full overflow-hidden">
        <motion.div
          className="h-full bg-black dark:bg-white rounded-full"
          initial={{ width: 0 }}
          animate={{ width: `${pct}%` }}
        />
      </div>
    </div>
  )
}
