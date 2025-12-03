import { useQuery } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { BarChart3, PieChart, TrendingUp } from 'lucide-react'

export default function AdminStats() {
  const { data: overview, isLoading: loadingOverview } = useQuery({
    queryKey: ['admin', 'stats', 'overview'],
    queryFn: () => adminApi.stats.overview().then(res => res.data),
  })

  const { data: byStatus } = useQuery({
    queryKey: ['admin', 'stats', 'byStatus'],
    queryFn: () => adminApi.stats.byStatus().then(res => res.data),
  })

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
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold text-tg-text">Статистика</h1>

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
    </div>
  )
}

