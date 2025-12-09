import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useNavigate, useSearchParams } from 'react-router-dom'
import {
  Plus, Search, ChevronRight, Globe, Clock, CheckCircle2,
  AlertCircle, Loader2, X, Sparkles, Calendar, Building2,
  ArrowUpRight
} from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import clsx from 'clsx'

const statusFilters = [
  { value: 'all', label: 'Все', count: null },
  { value: 'draft', label: 'Черновик', emoji: '📝' },
  { value: 'ready_to_generate', label: 'Готов', emoji: '✅' },
  { value: 'generating', label: 'Генерация', emoji: '⚡' },
  { value: 'success', label: 'Готово', emoji: '🎉' },
]

const statusConfig: Record<string, {
  icon: React.ReactNode
  label: string
  bg: string
  text: string
  glow?: string
}> = {
  draft: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Черновик',
    bg: 'bg-zinc-100 dark:bg-zinc-800',
    text: 'text-zinc-600 dark:text-zinc-400'
  },
  collecting_info: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Сбор данных',
    bg: 'bg-zinc-100 dark:bg-zinc-800',
    text: 'text-zinc-600 dark:text-zinc-400'
  },
  collecting_photos: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Сбор фото',
    bg: 'bg-amber-50 dark:bg-amber-900/20',
    text: 'text-amber-600 dark:text-amber-400'
  },
  awaiting_photos: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Ожидание фото',
    bg: 'bg-amber-50 dark:bg-amber-900/20',
    text: 'text-amber-600 dark:text-amber-400'
  },
  ready_to_generate: {
    icon: <CheckCircle2 className="w-3.5 h-3.5" />,
    label: 'Готов',
    bg: 'bg-emerald-50 dark:bg-emerald-900/20',
    text: 'text-emerald-600 dark:text-emerald-400'
  },
  generating: {
    icon: <Loader2 className="w-3.5 h-3.5 animate-spin" />,
    label: 'Генерация...',
    bg: 'bg-blue-50 dark:bg-blue-900/20',
    text: 'text-blue-600 dark:text-blue-400',
    glow: 'shadow-blue-500/20'
  },
  in_queue: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'В очереди',
    bg: 'bg-blue-50 dark:bg-blue-900/20',
    text: 'text-blue-600 dark:text-blue-400'
  },
  success: {
    icon: <Sparkles className="w-3.5 h-3.5" />,
    label: 'Готов!',
    bg: 'bg-emerald-50 dark:bg-emerald-900/20',
    text: 'text-emerald-600 dark:text-emerald-400',
    glow: 'shadow-emerald-500/20'
  },
  error: {
    icon: <AlertCircle className="w-3.5 h-3.5" />,
    label: 'Ошибка',
    bg: 'bg-red-50 dark:bg-red-900/20',
    text: 'text-red-600 dark:text-red-400'
  },
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

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr)
    const now = new Date()
    const diffMs = now.getTime() - date.getTime()
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))

    if (diffDays === 0) return 'Сегодня'
    if (diffDays === 1) return 'Вчера'
    if (diffDays < 7) return `${diffDays} дн. назад`

    return date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })
  }

  return (
    <div className="min-h-screen bg-tg-bg">
      {/* Premium Header */}
      <div className="bg-zinc-900 dark:bg-zinc-100 pt-8 pb-20 px-5">
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          className="flex items-center justify-between mb-6"
        >
          <div>
            <h1 className="text-2xl font-bold text-white dark:text-zinc-900">Заявки</h1>
            <p className="text-zinc-400 dark:text-zinc-600 text-sm mt-0.5">
              {requests.length} {requests.length === 1 ? 'заявка' : requests.length < 5 ? 'заявки' : 'заявок'}
            </p>
          </div>
          <motion.button
            onClick={() => {
              haptic?.impactOccurred('medium')
              navigate('/requests/new')
            }}
            className="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-white dark:bg-zinc-900 text-zinc-900 dark:text-white font-semibold text-sm shadow-lg"
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            <Plus className="w-4 h-4" />
            Новая
          </motion.button>
        </motion.div>

        {/* Search in header */}
        <motion.div
          className="relative"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <Search className={clsx(
            "absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 transition-colors duration-200",
            isSearchFocused ? 'text-zinc-900 dark:text-zinc-100' : 'text-zinc-400 dark:text-zinc-500'
          )} />
          <input
            type="text"
            placeholder="Найти заявку..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onFocus={() => setIsSearchFocused(true)}
            onBlur={() => setIsSearchFocused(false)}
            className="w-full pl-12 pr-12 py-3.5 rounded-2xl bg-white dark:bg-zinc-900 text-zinc-900 dark:text-zinc-100 placeholder:text-zinc-400 dark:placeholder:text-zinc-500 outline-none border-2 border-transparent focus:border-zinc-300 dark:focus:border-zinc-700 transition-all shadow-lg"
          />
          <AnimatePresence>
            {searchQuery && (
              <motion.button
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.8 }}
                onClick={() => setSearchQuery('')}
                className="absolute right-4 top-1/2 -translate-y-1/2 p-1.5 rounded-full bg-zinc-100 dark:bg-zinc-800 text-zinc-500"
              >
                <X className="w-3.5 h-3.5" />
              </motion.button>
            )}
          </AnimatePresence>
        </motion.div>
      </div>

      {/* Filters - Floating */}
      <div className="px-4 -mt-8 mb-4 relative z-10">
        <motion.div
          className="flex gap-2 overflow-x-auto pb-2 scrollbar-hide"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
        >
          {statusFilters.map((filter, idx) => (
            <motion.button
              key={filter.value}
              onClick={() => {
                haptic?.selectionChanged()
                setSearchParams(filter.value === 'all' ? {} : { status: filter.value })
              }}
              className={clsx(
                'flex items-center gap-2 px-4 py-2.5 rounded-2xl text-sm font-semibold whitespace-nowrap transition-all duration-300 shadow-lg',
                statusFilter === filter.value
                  ? 'bg-zinc-900 dark:bg-white text-white dark:text-zinc-900 scale-105'
                  : 'bg-white dark:bg-zinc-800 text-zinc-600 dark:text-zinc-300 hover:bg-zinc-50 dark:hover:bg-zinc-700'
              )}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.1 + idx * 0.05 }}
              whileTap={{ scale: 0.95 }}
            >
              {filter.emoji && <span>{filter.emoji}</span>}
              {filter.label}
            </motion.button>
          ))}
        </motion.div>
      </div>

      {/* Content */}
      <div className="px-4 pb-8">
        {isLoading ? (
          <div className="space-y-3">
            {[1, 2, 3].map(i => (
              <div
                key={i}
                className="skeleton h-28 rounded-3xl"
                style={{ animationDelay: `${i * 100}ms` }}
              />
            ))}
          </div>
        ) : filteredRequests.length === 0 ? (
          <motion.div
            className="text-center py-16"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
          >
            <motion.div
              className="w-24 h-24 rounded-3xl bg-gradient-to-br from-zinc-100 to-zinc-200 dark:from-zinc-800 dark:to-zinc-700 mx-auto mb-6 flex items-center justify-center shadow-xl"
              animate={{ y: [0, -8, 0] }}
              transition={{ repeat: Infinity, duration: 3, ease: "easeInOut" }}
            >
              <Globe className="w-12 h-12 text-zinc-400 dark:text-zinc-500" />
            </motion.div>
            <h3 className="text-xl font-bold text-tg-text mb-2">
              {searchQuery ? 'Ничего не найдено' : 'Пока нет заявок'}
            </h3>
            <p className="text-tg-hint mb-6 max-w-xs mx-auto">
              {searchQuery ? 'Попробуйте изменить поисковый запрос' : 'Создайте первую заявку и начните работу с клиентами'}
            </p>
            {!searchQuery && (
              <motion.button
                onClick={() => navigate('/requests/new')}
                className="inline-flex items-center gap-2 px-6 py-3.5 bg-zinc-900 dark:bg-white text-white dark:text-zinc-900 rounded-2xl font-semibold shadow-xl"
                whileHover={{ scale: 1.02, y: -2 }}
                whileTap={{ scale: 0.98 }}
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
                    className="w-full text-left group"
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95 }}
                    transition={{ delay: index * 0.04, duration: 0.3 }}
                    layout
                  >
                    <div className={clsx(
                      "bg-white dark:bg-zinc-900 rounded-3xl p-4 border border-zinc-100 dark:border-zinc-800 transition-all duration-300",
                      "hover:shadow-xl hover:-translate-y-1 hover:border-zinc-200 dark:hover:border-zinc-700",
                      config.glow && `shadow-lg ${config.glow}`
                    )}>
                      <div className="flex items-start gap-4">
                        {/* Avatar */}
                        <div className="relative flex-shrink-0">
                          <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-zinc-900 to-zinc-700 dark:from-zinc-200 dark:to-zinc-400 flex items-center justify-center text-white dark:text-zinc-900 font-bold text-xl shadow-lg">
                            {request.company_name?.[0]?.toUpperCase() || '?'}
                          </div>
                          {status === 'success' && (
                            <div className="absolute -bottom-1 -right-1 w-5 h-5 bg-emerald-500 rounded-lg flex items-center justify-center shadow-lg">
                              <CheckCircle2 className="w-3 h-3 text-white" />
                            </div>
                          )}
                        </div>

                        {/* Content */}
                        <div className="flex-1 min-w-0">
                          <div className="flex items-start justify-between gap-2 mb-1">
                            <h3 className="font-bold text-tg-text truncate text-[15px]">
                              {request.company_name || 'Без названия'}
                            </h3>
                            <ArrowUpRight className="w-4 h-4 text-zinc-300 dark:text-zinc-600 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity" />
                          </div>

                          <div className="flex items-center gap-3 text-sm text-tg-hint mb-3">
                            <span className="flex items-center gap-1 truncate">
                              <Building2 className="w-3.5 h-3.5 flex-shrink-0" />
                              {request.client_name || 'Без клиента'}
                            </span>
                            <span className="flex items-center gap-1 flex-shrink-0">
                              <Calendar className="w-3.5 h-3.5" />
                              {formatDate(request.created_at)}
                            </span>
                          </div>

                          {/* Status Badge */}
                          <div className={clsx(
                            "inline-flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-semibold",
                            config.bg,
                            config.text
                          )}>
                            {config.icon}
                            <span>{config.label}</span>
                          </div>
                        </div>
                      </div>
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
