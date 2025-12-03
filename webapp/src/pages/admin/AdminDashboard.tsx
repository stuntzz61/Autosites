import { useQuery } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { Users, FileText, Clock, CheckCircle2, TrendingUp } from 'lucide-react'

export default function AdminDashboard() {
  const { data: stats, isLoading } = useQuery({
    queryKey: ['admin', 'stats'],
    queryFn: () => adminApi.stats.overview().then(res => res.data),
  })

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="skeleton h-24 rounded-2xl" />
        ))}
      </div>
    )
  }

  const cards = [
    {
      title: 'Всего менеджеров',
      value: stats?.total_managers || 0,
      icon: <Users className="w-6 h-6" />,
      color: 'bg-blue-500',
    },
    {
      title: 'Всего заявок',
      value: stats?.total_requests || 0,
      icon: <FileText className="w-6 h-6" />,
      color: 'bg-purple-500',
    },
    {
      title: 'В очереди',
      value: stats?.pending_generation || 0,
      icon: <Clock className="w-6 h-6" />,
      color: 'bg-orange-500',
    },
    {
      title: 'Завершено сегодня',
      value: stats?.completed_today || 0,
      icon: <CheckCircle2 className="w-6 h-6" />,
      color: 'bg-green-500',
    },
  ]

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold text-tg-text">Панель управления</h1>

      <div className="grid grid-cols-2 gap-3">
        {cards.map((card, i) => (
          <div
            key={i}
            className="bg-tg-secondary-bg rounded-2xl p-4"
          >
            <div className={`w-10 h-10 ${card.color} rounded-xl flex items-center justify-center text-white mb-3`}>
              {card.icon}
            </div>
            <p className="text-2xl font-bold text-tg-text">{card.value}</p>
            <p className="text-sm text-tg-hint">{card.title}</p>
          </div>
        ))}
      </div>

      {/* Recent activity */}
      <div className="bg-tg-secondary-bg rounded-2xl p-4">
        <div className="flex items-center gap-2 mb-4">
          <TrendingUp className="w-5 h-5 text-tg-accent" />
          <h2 className="font-semibold text-tg-text">Активность</h2>
        </div>
        <div className="space-y-2 text-sm text-tg-hint">
          <p>Заявок сегодня: {stats?.requests_today || 0}</p>
          <p>Заявок за неделю: {stats?.requests_this_week || 0}</p>
          <p>Заявок за месяц: {stats?.requests_this_month || 0}</p>
        </div>
      </div>
    </div>
  )
}

