import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { BarChart3, PieChart, TrendingUp, Download, FileSpreadsheet, FileText, Loader2 } from 'lucide-react'
import toast from 'react-hot-toast'

export default function AdminStats() {
  const [exporting, setExporting] = useState<'excel' | 'pdf' | null>(null)

  const { data: overview, isLoading: loadingOverview } = useQuery({
    queryKey: ['admin', 'stats', 'overview'],
    queryFn: () => adminApi.stats.overview().then(res => res.data),
  })

  const { data: byStatus } = useQuery({
    queryKey: ['admin', 'stats', 'byStatus'],
    queryFn: () => adminApi.stats.byStatus().then(res => res.data),
  })

  const { data: managers } = useQuery({
    queryKey: ['admin', 'stats', 'managers'],
    queryFn: () => adminApi.stats.managers().then(res => res.data),
  })

  const handleExportExcel = async () => {
    setExporting('excel')
    try {
      const response = await adminApi.export.excel()
      const blob = new Blob([response.data], {
        type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      })
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `stats_${new Date().toISOString().split('T')[0]}.xlsx`
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      document.body.removeChild(a)
      toast.success('Excel файл скачан')
    } catch (error) {
      toast.error('Ошибка экспорта')
    } finally {
      setExporting(null)
    }
  }

  const handleExportPDF = async () => {
    setExporting('pdf')
    try {
      const response = await adminApi.export.pdf()
      const blob = new Blob([response.data], { type: 'application/pdf' })
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `stats_${new Date().toISOString().split('T')[0]}.pdf`
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      document.body.removeChild(a)
      toast.success('PDF файл скачан')
    } catch (error) {
      toast.error('Ошибка экспорта')
    } finally {
      setExporting(null)
    }
  }

  if (loadingOverview) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(3)].map((_, i) => (
          <div key={i} className="skeleton h-32 rounded-2xl" />
        ))}
      </div>
    )
  }

  const statusLabels: Record<string, string> = {
    draft: 'Черновик',
    collecting_info: 'Сбор данных',
    collecting_photos: 'Сбор фото',
    ready_to_generate: 'Готов',
    queued: 'В очереди',
    in_queue: 'В очереди',
    generating: 'Генерация',
    success: 'Успешно',
    generated_ok: 'Успешно',
    error: 'Ошибка',
    archived: 'Архив',
  }

  return (
    <div className="p-4 space-y-4 pb-24">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-tg-text">Статистика</h1>

        {/* Export buttons */}
        <div className="flex gap-2">
          <button
            onClick={handleExportExcel}
            disabled={exporting !== null}
            className="p-2 rounded-xl bg-green-500/20 text-green-600 disabled:opacity-50"
            title="Скачать Excel"
          >
            {exporting === 'excel' ? (
              <Loader2 className="w-5 h-5 animate-spin" />
            ) : (
              <FileSpreadsheet className="w-5 h-5" />
            )}
          </button>
          <button
            onClick={handleExportPDF}
            disabled={exporting !== null}
            className="p-2 rounded-xl bg-red-500/20 text-red-600 disabled:opacity-50"
            title="Скачать PDF"
          >
            {exporting === 'pdf' ? (
              <Loader2 className="w-5 h-5 animate-spin" />
            ) : (
              <FileText className="w-5 h-5" />
            )}
          </button>
        </div>
      </div>

      {/* Overview */}
      <div className="bg-tg-secondary-bg rounded-2xl p-4">
        <div className="flex items-center gap-2 mb-4">
          <TrendingUp className="w-5 h-5 text-tg-accent" />
          <h2 className="font-semibold text-tg-text">Обзор</h2>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <p className="text-2xl font-bold text-tg-text">{overview?.total_requests || 0}</p>
            <p className="text-sm text-tg-hint">Всего заявок</p>
          </div>
          <div>
            <p className="text-2xl font-bold text-tg-text">{overview?.total_managers || 0}</p>
            <p className="text-sm text-tg-hint">Менеджеров</p>
          </div>
          <div>
            <p className="text-2xl font-bold text-green-500">{overview?.completed_today || 0}</p>
            <p className="text-sm text-tg-hint">Завершено сегодня</p>
          </div>
          <div>
            <p className="text-2xl font-bold text-orange-500">{overview?.pending_generation || 0}</p>
            <p className="text-sm text-tg-hint">В работе</p>
          </div>
        </div>
      </div>

      {/* By period */}
      <div className="bg-tg-secondary-bg rounded-2xl p-4">
        <div className="flex items-center gap-2 mb-4">
          <BarChart3 className="w-5 h-5 text-tg-accent" />
          <h2 className="font-semibold text-tg-text">По периодам</h2>
        </div>
        <div className="space-y-3">
          <div className="flex justify-between">
            <span className="text-tg-hint">Сегодня</span>
            <span className="font-medium text-tg-text">{overview?.requests_today || 0}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-tg-hint">За неделю</span>
            <span className="font-medium text-tg-text">{overview?.requests_this_week || 0}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-tg-hint">За месяц</span>
            <span className="font-medium text-tg-text">{overview?.requests_this_month || 0}</span>
          </div>
        </div>
      </div>

      {/* By status */}
      <div className="bg-tg-secondary-bg rounded-2xl p-4">
        <div className="flex items-center gap-2 mb-4">
          <PieChart className="w-5 h-5 text-tg-accent" />
          <h2 className="font-semibold text-tg-text">По статусам</h2>
        </div>
        <div className="space-y-2">
          {byStatus?.map((item: any) => (
            <div key={item.status} className="flex justify-between items-center">
              <span className="text-tg-hint">
                {statusLabels[item.status] || item.status}
              </span>
              <span className="font-medium text-tg-text">{item.count}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Top Managers */}
      {managers && managers.length > 0 && (
        <div className="bg-tg-secondary-bg rounded-2xl p-4">
          <div className="flex items-center gap-2 mb-4">
            <Download className="w-5 h-5 text-tg-accent" />
            <h2 className="font-semibold text-tg-text">Топ менеджеров</h2>
          </div>
          <div className="space-y-3">
            {managers.slice(0, 10).map((manager: any, index: number) => (
              <div key={manager.id} className="flex items-center gap-3">
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${
                  index === 0 ? 'bg-yellow-500/20 text-yellow-600' :
                  index === 1 ? 'bg-gray-300/20 text-gray-500' :
                  index === 2 ? 'bg-orange-500/20 text-orange-600' :
                  'bg-tg-bg text-tg-hint'
                }`}>
                  {index + 1}
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
                  <p className="font-bold text-tg-text">{manager.request_count || 0}</p>
                  <p className="text-xs text-tg-hint">заявок</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
