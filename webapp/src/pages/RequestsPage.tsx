import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { Plus, Search, Filter, ChevronRight, Globe, Clock, CheckCircle2, AlertCircle, Loader2 } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import clsx from 'clsx'

const statusFilters = [
  { value: 'all', label: 'Все' },
  { value: 'draft', label: 'Черновики' },
  { value: 'ready_to_generate', label: 'Готовы' },
  { value: 'generating', label: 'Генерация' },
  { value: 'success', label: 'Готово' },
  { value: 'error', label: 'Ошибки' },
]

const statusConfig: Record<string, { icon: React.ReactNode; color: string; label: string }> = {
  draft: { icon: <Clock className="w-4 h-4" />, color: 'text-gray-500', label: 'Черновик' },
  collecting_info: { icon: <Clock className="w-4 h-4" />, color: 'text-amber-500', label: 'Сбор данных' },
  collecting_photos: { icon: <Clock className="w-4 h-4" />, color: 'text-amber-500', label: 'Сбор фото' },
  ready_to_generate: { icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-green-500', label: 'Готов' },
  generating: { icon: <Loader2 className="w-4 h-4 animate-spin" />, color: 'text-blue-500', label: 'Генерация' },
  in_queue: { icon: <Clock className="w-4 h-4" />, color: 'text-blue-500', label: 'В очереди' },
  success: { icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-emerald-500', label: 'Готово' },
  error: { icon: <AlertCircle className="w-4 h-4" />, color: 'text-red-500', label: 'Ошибка' },
}

interface Request {
  id: string
  company_name: string
  client_name: string
  status: string
  created_at: string
  payload?: {
    site?: {
      meta?: {
        status?: string
      }
    }
  }
}

export default function RequestsPage() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const { haptic } = useTelegram()
  const [searchQuery, setSearchQuery] = useState('')

  const statusFilter = searchParams.get('status') || 'all'

  const { data, isLoading } = useQuery({
    queryKey: ['requests', statusFilter],
    queryFn: () => requestsApi.list({ status: statusFilter !== 'all' ? statusFilter : undefined }).then(res => res.data),
  })

  const requests: Request[] = data?.items || []

  const filteredRequests = requests.filter(req => {
    if (!searchQuery) return true
    const search = searchQuery.toLowerCase()
    return (
      req.company_name?.toLowerCase().includes(search) ||
      req.client_name?.toLowerCase().includes(search)
    )
  })

  const getStatus = (req: Request) => {
    return req.payload?.site?.meta?.status || req.status || 'draft'
  }

  return (
    <div className="min-h-screen">
      {/* Header */}
      <div className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator">
        <div className="p-4 space-y-3">
          <div className="flex items-center justify-between">
            <h1 className="text-xl font-bold text-tg-text">Заявки</h1>
            <button
              onClick={() => {
                haptic?.impactOccurred('medium')
                navigate('/requests/new')
              }}
              className="p-2 rounded-xl bg-tg-button text-tg-button-text"
            >
              <Plus className="w-5 h-5" />
            </button>
          </div>

          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
            <input
              type="text"
              placeholder="Поиск по названию или клиенту..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="input pl-10"
            />
          </div>

          {/* Filters */}
          <div className="flex gap-2 overflow-x-auto pb-1 -mx-4 px-4">
            {statusFilters.map(filter => (
              <button
                key={filter.value}
                onClick={() => {
                  haptic?.selectionChanged()
                  setSearchParams(filter.value === 'all' ? {} : { status: filter.value })
                }}
                className={clsx(
                  'px-3 py-1.5 rounded-full text-sm font-medium whitespace-nowrap transition-colors',
                  statusFilter === filter.value
                    ? 'bg-tg-button text-tg-button-text'
                    : 'bg-tg-secondary-bg text-tg-hint'
                )}
              >
                {filter.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="p-4">
        {isLoading ? (
          <div className="space-y-3">
            {[1, 2, 3].map(i => (
              <div key={i} className="skeleton h-20 rounded-2xl" />
            ))}
          </div>
        ) : filteredRequests.length === 0 ? (
          <motion.div
            className="text-center py-12"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
          >
            <div className="w-16 h-16 rounded-2xl bg-tg-secondary-bg mx-auto mb-4 flex items-center justify-center">
              <Globe className="w-8 h-8 text-tg-hint" />
            </div>
            <p className="text-tg-text font-medium mb-1">Нет заявок</p>
            <p className="text-sm text-tg-hint mb-4">
              {searchQuery ? 'Попробуйте другой запрос' : 'Создайте первую заявку'}
            </p>
            {!searchQuery && (
              <button
                onClick={() => navigate('/requests/new')}
                className="btn btn-primary"
              >
                <Plus className="w-5 h-5" />
                Создать заявку
              </button>
            )}
          </motion.div>
        ) : (
          <AnimatePresence mode="popLayout">
            <div className="space-y-3">
              {filteredRequests.map((request, index) => {
                const status = getStatus(request)
                const config = statusConfig[status] || statusConfig.draft

                return (
                  <motion.button
                    key={request.id}
                    onClick={() => {
                      haptic?.impactOccurred('light')
                      navigate(`/requests/${request.id}`)
                    }}
                    className="w-full bg-tg-section rounded-2xl p-4 text-left active:scale-[0.98] transition-transform"
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95 }}
                    transition={{ delay: index * 0.05 }}
                    layout
                  >
                    <div className="flex items-start gap-3">
                      <div className="flex-shrink-0 w-10 h-10 rounded-xl bg-gradient-to-br from-brand-500 to-brand-600 flex items-center justify-center text-white font-bold text-lg">
                        {request.company_name?.[0]?.toUpperCase() || '?'}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="font-semibold text-tg-text truncate">
                          {request.company_name || 'Без названия'}
                        </p>
                        <p className="text-sm text-tg-hint truncate">
                          {request.client_name || 'Без клиента'}
                        </p>
                        <div className={`inline-flex items-center gap-1 mt-1 ${config.color}`}>
                          {config.icon}
                          <span className="text-xs font-medium">{config.label}</span>
                        </div>
                      </div>
                      <ChevronRight className="w-5 h-5 text-tg-hint flex-shrink-0" />
                    </div>
                  </motion.button>
                )
              })}
            </div>
          </AnimatePresence>
        )}
      </div>
    </div>
  )
}
