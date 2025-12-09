import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { Plus, Search, ChevronRight, Globe, Clock, CheckCircle2, AlertCircle, Loader2, X, Sparkles } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import clsx from 'clsx'

const statusFilters = [
  { value: 'all', label: 'Все', icon: null },
  { value: 'draft', label: 'Черновик', icon: '📝' },
  { value: 'ready_to_generate', label: 'Готов', icon: '✅' },
  { value: 'generating', label: 'Генерация', icon: '⚡' },
  { value: 'success', label: 'Готово', icon: '🎉' },
]

const statusConfig: Record<string, { icon: React.ReactNode; label: string; color: string }> = {
  draft: { icon: <Clock className="w-4 h-4" />, label: 'Черновик', color: 'text-zinc-500' },
  collecting_info: { icon: <Clock className="w-4 h-4" />, label: 'Сбор данных', color: 'text-zinc-500' },
  collecting_photos: { icon: <Clock className="w-4 h-4" />, label: 'Сбор фото', color: 'text-zinc-500' },
  awaiting_photos: { icon: <Clock className="w-4 h-4" />, label: 'Ожидание фото', color: 'text-amber-500' },
  ready_to_generate: { icon: <CheckCircle2 className="w-4 h-4" />, label: 'Готов к отправке', color: 'text-emerald-500' },
  generating: { icon: <Loader2 className="w-4 h-4 animate-spin" />, label: 'Генерация...', color: 'text-blue-500' },
  in_queue: { icon: <Clock className="w-4 h-4" />, label: 'В очереди', color: 'text-blue-500' },
  success: { icon: <Sparkles className="w-4 h-4" />, label: 'Сайт готов!', color: 'text-emerald-500' },
  error: { icon: <AlertCircle className="w-4 h-4" />, label: 'Ошибка', color: 'text-red-500' },
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
  const [isSearchFocused, setIsSearchFocused] = useState(false)

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
    const raw = req.payload?.site?.meta?.status || req.status || 'draft'
    const map: Record<string, string> = {
      'generated_ok': 'success',
      'generated_error': 'error',
      'ready': 'ready_to_generate',
    }
    return map[raw] || raw
  }

  return (
    <div className="min-h-screen bg-tg-bg">
      {/* Header */}
      <div className="sticky top-0 z-10 bg-tg-bg/80 backdrop-blur-xl border-b border-tg-separator">
        <div className="p-4 space-y-3">
          <div className="flex items-center justify-between">
            <h1 className="text-2xl font-bold text-tg-text">Заявки</h1>
            <motion.button
              onClick={() => {
                haptic?.impactOccurred('medium')
                navigate('/requests/new')
              }}
              className="p-2.5 rounded-xl bg-zinc-900 dark:bg-white text-white dark:text-zinc-900 shadow-lg shadow-zinc-900/20 dark:shadow-white/20"
              whileTap={{ scale: 0.95 }}
            >
              <Plus className="w-5 h-5" />
            </motion.button>
          </div>

          {/* Search */}
          <div className="relative">
            <Search className={clsx(
              "absolute left-3.5 top-1/2 -translate-y-1/2 w-5 h-5 transition-colors",
              isSearchFocused ? 'text-tg-text' : 'text-tg-hint'
            )} />
            <input
              type="text"
              placeholder="Поиск по компании или клиенту..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onFocus={() => setIsSearchFocused(true)}
              onBlur={() => setIsSearchFocused(false)}
              className="input pl-11 pr-10"
            />
            {searchQuery && (
              <button
                onClick={() => setSearchQuery('')}
                className="absolute right-3 top-1/2 -translate-y-1/2 p-1 rounded-full bg-tg-hint/20 text-tg-hint"
              >
                <X className="w-4 h-4" />
              </button>
            )}
          </div>

          {/* Filters */}
          <div className="flex gap-2 overflow-x-auto pb-1 -mx-4 px-4 scrollbar-hide">
            {statusFilters.map(filter => (
              <button
                key={filter.value}
                onClick={() => {
                  haptic?.selectionChanged()
                  setSearchParams(filter.value === 'all' ? {} : { status: filter.value })
                }}
                className={clsx(
                  'px-4 py-2 rounded-full text-sm font-semibold whitespace-nowrap transition-all duration-200',
                  statusFilter === filter.value
                    ? 'bg-zinc-900 dark:bg-white text-white dark:text-zinc-900 shadow-md'
                    : 'bg-zinc-100 dark:bg-zinc-800 text-tg-hint hover:bg-zinc-200 dark:hover:bg-zinc-700'
                )}
              >
                {filter.icon && <span className="mr-1">{filter.icon}</span>}
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
              <div key={i} className="skeleton h-24 rounded-2xl" style={{ animationDelay: `${i * 100}ms` }} />
            ))}
          </div>
        ) : filteredRequests.length === 0 ? (
          <motion.div
            className="text-center py-16"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
          >
            <div className="w-20 h-20 rounded-3xl bg-gradient-to-br from-zinc-100 to-zinc-200 dark:from-zinc-800 dark:to-zinc-700 mx-auto mb-5 flex items-center justify-center">
              <Globe className="w-10 h-10 text-tg-hint" />
            </div>
            <h3 className="text-lg font-bold text-tg-text mb-1">
              {searchQuery ? 'Ничего не найдено' : 'Нет заявок'}
            </h3>
            <p className="text-sm text-tg-hint mb-5">
              {searchQuery ? 'Попробуйте другой запрос' : 'Создайте первую заявку для клиента'}
            </p>
            {!searchQuery && (
              <motion.button
                onClick={() => navigate('/requests/new')}
                className="btn btn-primary"
                whileTap={{ scale: 0.97 }}
              >
                <Plus className="w-5 h-5" />
                Создать заявку
              </motion.button>
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
                    className="w-full bg-white dark:bg-zinc-900 rounded-2xl p-4 text-left active:scale-[0.98] transition-all duration-200 border border-zinc-200 dark:border-zinc-800 shadow-sm hover:shadow-md hover:-translate-y-0.5"
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95 }}
                    transition={{ delay: index * 0.03, duration: 0.2 }}
                    layout
                  >
                    <div className="flex items-start gap-3">
                      <div className="flex-shrink-0 w-12 h-12 rounded-xl bg-gradient-to-br from-zinc-900 to-zinc-700 dark:from-zinc-100 dark:to-zinc-300 flex items-center justify-center text-white dark:text-zinc-900 font-bold text-lg shadow-sm">
                        {request.company_name?.[0]?.toUpperCase() || '?'}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="font-bold text-tg-text truncate mb-0.5">
                          {request.company_name || 'Без названия'}
                        </p>
                        <p className="text-sm text-tg-hint truncate mb-2">
                          {request.client_name || 'Без клиента'}
                        </p>
                        <div className={clsx(
                          "inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold",
                          config.color,
                          status === 'success' ? 'bg-emerald-100 dark:bg-emerald-900/30' :
                          status === 'error' ? 'bg-red-100 dark:bg-red-900/30' :
                          status === 'generating' || status === 'in_queue' ? 'bg-blue-100 dark:bg-blue-900/30' :
                          'bg-zinc-100 dark:bg-zinc-800'
                        )}>
                          {config.icon}
                          <span>{config.label}</span>
                        </div>
                      </div>
                      <ChevronRight className="w-5 h-5 text-tg-hint flex-shrink-0 mt-3" />
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
