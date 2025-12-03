import { motion } from 'framer-motion'
import { BarChart3, TrendingUp, Users, FileText, Download, Award } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { useTelegram } from '@/contexts/TelegramContext'
import toast from 'react-hot-toast'

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
      toast.success('Загружено')
    } catch {
      toast.error('Ошибка')
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
      toast.success('Загружено')
    } catch {
      toast.error('Ошибка экспорта PDF')
    }
  }

  const maxDayValue = Math.max(...(byDay?.map((d: any) => d.count) || [1]))

  const statusLabels: Record<string, string> = {
    draft: 'Черновики',
    collecting_info: 'Сбор данных',
    collecting_photos: 'Сбор фото',
    ready_to_generate: 'Готовы',
    generating: 'В работе',
    in_queue: 'В очереди',
    success: 'Завершено',
    error: 'Ошибки',
    archived: 'Архив',
    closed: 'Закрыто',
  }

  return (
    <div className="p-4 space-y-6">
      {/* Export */}
      <div className="flex gap-3">
        <button onClick={handleExportExcel} className="btn btn-secondary flex-1">
          <Download className="w-5 h-5" /> Excel
        </button>
        <button onClick={handleExportPdf} className="btn btn-secondary flex-1">
          <Download className="w-5 h-5" /> PDF
        </button>
      </div>

      {/* Overview */}
      <div className="grid grid-cols-2 gap-3">
        <StatCard icon={<FileText className="w-5 h-5" />} value={overview?.total_requests || 0} label="Всего заявок" />
        <StatCard icon={<Users className="w-5 h-5" />} value={overview?.total_managers || 0} label="Менеджеров" />
        <StatCard icon={<TrendingUp className="w-5 h-5" />} value={overview?.this_month || 0} label="За месяц" />
        <StatCard icon={<BarChart3 className="w-5 h-5" />} value={overview?.avg_per_day?.toFixed(1) || '0'} label="В среднем/день" />
      </div>

      {/* Chart */}
      <div>
        <p className="section-header">7 дней</p>
        <div className="bg-tg-section rounded-2xl p-4 border border-tg-separator">
          <div className="flex items-end justify-between gap-2 h-32">
            {byDay?.map((day: any, i: number) => {
              const h = maxDayValue > 0 ? (day.count / maxDayValue) * 100 : 0
              return (
                <div key={i} className="flex-1 flex flex-col items-center gap-2">
                  <motion.div
                    className="w-full bg-black dark:bg-white rounded-t-lg"
                    initial={{ height: 0 }}
                    animate={{ height: `${Math.max(h, 4)}%` }}
                    transition={{ delay: i * 0.1, duration: 0.5 }}
                  />
                  <span className="text-[10px] text-tg-hint">
                    {new Date(day.date).toLocaleDateString('ru-RU', { weekday: 'short' })}
                  </span>
                </div>
              )
            })}
          </div>
        </div>
      </div>

      {/* By Status */}
      <div>
        <p className="section-header">По статусам</p>
        <div className="bg-tg-section rounded-2xl p-4 space-y-3 border border-tg-separator">
          {byStatus?.map((s: any) => (
            <StatusRow key={s.status} label={statusLabels[s.status] || s.status} count={s.count} total={overview?.total_requests || 1} />
          ))}
        </div>
      </div>

      {/* Top Managers */}
      <div>
        <p className="section-header">Топ менеджеров</p>
        <div className="bg-tg-section rounded-2xl border border-tg-separator">
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
    </div>
  )
}

function StatCard({ icon, value, label }: { icon: React.ReactNode; value: number | string; label: string }) {
  return (
    <div className="bg-tg-section rounded-2xl p-4 border border-tg-separator">
      <div className="text-tg-hint mb-2">{icon}</div>
      <p className="text-2xl font-bold">{value}</p>
      <p className="text-xs text-tg-hint">{label}</p>
    </div>
  )
}

function StatusRow({ label, count, total }: { label: string; count: number; total: number }) {
  const pct = total > 0 ? (count / total) * 100 : 0

  return (
    <div>
      <div className="flex justify-between text-sm mb-1">
        <span>{label}</span>
        <span className="text-tg-hint">{count}</span>
      </div>
      <div className="h-2 bg-tg-secondary-bg rounded-full overflow-hidden">
        <motion.div className="h-full bg-black dark:bg-white rounded-full" initial={{ width: 0 }} animate={{ width: `${pct}%` }} />
      </div>
    </div>
  )
}
