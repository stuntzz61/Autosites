import { motion } from 'framer-motion'
import {
  BarChart3, TrendingUp, Users, FileText, Download,
  Calendar, Award
} from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { useTelegram } from '@/contexts/TelegramContext'
import toast from 'react-hot-toast'

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

export default function AdminStats() {
  const { haptic } = useTelegram()

  const { data: overview } = useQuery({
    queryKey: ['admin-stats-overview'],
    queryFn: () => adminApi.stats.overview().then(res => res.data),
  })

  const { data: byStatus } = useQuery({
    queryKey: ['admin-stats-by-status'],
    queryFn: () => adminApi.stats.byStatus().then(res => res.data),
  })

  const { data: byDay } = useQuery({
    queryKey: ['admin-stats-by-day'],
    queryFn: () => adminApi.stats.byDay(7).then(res => res.data),
  })

  const { data: managers } = useQuery({
    queryKey: ['admin-stats-managers'],
    queryFn: () => adminApi.stats.managers().then(res => res.data),
  })

  const handleExportExcel = async () => {
    try {
      haptic?.impactOccurred('medium')
      const response = await adminApi.export.excel()
      const url = window.URL.createObjectURL(new Blob([response.data]))
      const link = document.createElement('a')
      link.href = url
      link.setAttribute('download', `stats_${new Date().toISOString().split('T')[0]}.xlsx`)
      document.body.appendChild(link)
      link.click()
      link.remove()
      toast.success('Файл загружен')
    } catch {
      toast.error('Ошибка экспорта')
    }
  }

  const handleExportPdf = async () => {
    try {
      haptic?.impactOccurred('medium')
      const response = await adminApi.export.pdf()
      const url = window.URL.createObjectURL(new Blob([response.data]))
      const link = document.createElement('a')
      link.href = url
      link.setAttribute('download', `stats_${new Date().toISOString().split('T')[0]}.pdf`)
      document.body.appendChild(link)
      link.click()
      link.remove()
      toast.success('Файл загружен')
    } catch {
      toast.error('Ошибка экспорта')
    }
  }

  const maxDayValue = Math.max(...(byDay?.map((d: any) => d.count) || [1]))

  return (
    <motion.div
      className="p-4 space-y-6"
      variants={container}
      initial="hidden"
      animate="show"
    >
      {/* Export Buttons */}
      <motion.div variants={item} className="flex gap-3">
        <button onClick={handleExportExcel} className="btn btn-secondary flex-1">
          <Download className="w-5 h-5" />
          Excel
        </button>
        <button onClick={handleExportPdf} className="btn btn-secondary flex-1">
          <Download className="w-5 h-5" />
          PDF
        </button>
      </motion.div>

      {/* Overview Stats */}
      <motion.div variants={item} className="grid grid-cols-2 gap-3">
        <StatCard
          icon={<FileText className="w-5 h-5" />}
          value={overview?.total_requests || 0}
          label="Всего заявок"
          color="blue"
        />
        <StatCard
          icon={<Users className="w-5 h-5" />}
          value={overview?.total_managers || 0}
          label="Менеджеров"
          color="purple"
        />
        <StatCard
          icon={<TrendingUp className="w-5 h-5" />}
          value={overview?.this_month || 0}
          label="За месяц"
          color="green"
        />
        <StatCard
          icon={<BarChart3 className="w-5 h-5" />}
          value={overview?.avg_per_day?.toFixed(1) || '0'}
          label="В среднем/день"
          color="amber"
        />
      </motion.div>

      {/* By Day Chart */}
      <motion.div variants={item}>
        <p className="section-header">За последние 7 дней</p>
        <div className="bg-tg-section rounded-2xl p-4">
          <div className="flex items-end justify-between gap-2 h-32">
            {byDay?.map((day: any, i: number) => {
              const height = maxDayValue > 0 ? (day.count / maxDayValue) * 100 : 0
              return (
                <div key={i} className="flex-1 flex flex-col items-center gap-2">
                  <motion.div
                    className="w-full bg-gradient-to-t from-brand-500 to-brand-400 rounded-t-lg"
                    initial={{ height: 0 }}
                    animate={{ height: `${Math.max(height, 4)}%` }}
                    transition={{ delay: i * 0.1, duration: 0.5 }}
                  />
                  <span className="text-[10px] text-tg-hint">
                    {new Date(day.date).toLocaleDateString('ru-RU', { weekday: 'short' })}
                  </span>
                </div>
              )
            })}
          </div>
          <div className="flex justify-between mt-2 text-xs text-tg-hint">
            <span>Мин: {Math.min(...(byDay?.map((d: any) => d.count) || [0]))}</span>
            <span>Макс: {maxDayValue}</span>
          </div>
        </div>
      </motion.div>

      {/* By Status */}
      <motion.div variants={item}>
        <p className="section-header">По статусам</p>
        <div className="bg-tg-section rounded-2xl p-4 space-y-3">
          {byStatus?.map((s: any) => (
            <StatusRow key={s.status} status={s.status} count={s.count} total={overview?.total_requests || 1} />
          ))}
        </div>
      </motion.div>

      {/* Top Managers */}
      <motion.div variants={item}>
        <p className="section-header">Топ менеджеров</p>
        <div className="bg-tg-section rounded-2xl overflow-hidden">
          {managers?.slice(0, 10).map((manager: any, i: number) => (
            <div key={manager.id}>
              {i > 0 && <div className="divider" />}
              <div className="list-item">
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-white font-bold text-sm ${
                  i === 0 ? 'bg-amber-500' : i === 1 ? 'bg-gray-400' : i === 2 ? 'bg-amber-700' : 'bg-tg-hint'
                }`}>
                  {i < 3 ? <Award className="w-4 h-4" /> : i + 1}
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
    </motion.div>
  )
}

function StatCard({
  icon,
  value,
  label,
  color,
}: {
  icon: React.ReactNode
  value: number | string
  label: string
  color: 'blue' | 'green' | 'amber' | 'purple'
}) {
  const colors = {
    blue: 'bg-blue-500/10 text-blue-500',
    green: 'bg-green-500/10 text-green-500',
    amber: 'bg-amber-500/10 text-amber-500',
    purple: 'bg-purple-500/10 text-purple-500',
  }

  return (
    <div className="bg-tg-section rounded-2xl p-4">
      <div className={`inline-flex p-2 rounded-xl ${colors[color]} mb-2`}>
        {icon}
      </div>
      <p className="text-2xl font-bold text-tg-text">{value}</p>
      <p className="text-xs text-tg-hint">{label}</p>
    </div>
  )
}

const statusColors: Record<string, string> = {
  draft: 'bg-gray-400',
  collecting_info: 'bg-amber-400',
  collecting_photos: 'bg-amber-500',
  ready_to_generate: 'bg-green-500',
  generating: 'bg-blue-500',
  in_queue: 'bg-blue-400',
  success: 'bg-emerald-500',
  error: 'bg-red-500',
  archived: 'bg-purple-500',
  closed: 'bg-gray-600',
}

const statusLabels: Record<string, string> = {
  draft: 'Черновики',
  collecting_info: 'Сбор данных',
  collecting_photos: 'Сбор фото',
  ready_to_generate: 'Готовы',
  generating: 'Генерация',
  in_queue: 'В очереди',
  success: 'Успешно',
  error: 'Ошибки',
  archived: 'Архив',
  closed: 'Закрыто',
}

function StatusRow({ status, count, total }: { status: string; count: number; total: number }) {
  const percentage = total > 0 ? (count / total) * 100 : 0

  return (
    <div>
      <div className="flex justify-between text-sm mb-1">
        <span className="text-tg-text">{statusLabels[status] || status}</span>
        <span className="text-tg-hint">{count}</span>
      </div>
      <div className="h-2 bg-tg-secondary-bg rounded-full overflow-hidden">
        <motion.div
          className={`h-full ${statusColors[status] || 'bg-gray-400'} rounded-full`}
          initial={{ width: 0 }}
          animate={{ width: `${percentage}%` }}
          transition={{ duration: 0.5, ease: 'easeOut' }}
        />
      </div>
    </div>
  )
}
