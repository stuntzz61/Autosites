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
import Tooltip from '@/components/Tooltip'
import clsx from 'clsx'

const statusFilters = [
  { value: 'all', label: 'Все', count: null },
  { value: 'draft', label: 'Черновик' },
  { value: 'ready_to_generate', label: 'Готов' },
  { value: 'generating', label: 'Генерация' },
  { value: 'success', label: 'Готово' },
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
    bg: 'bg-slate-700/50',
    text: 'text-slate-400'
  },
  collecting_info: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Сбор данных',
    bg: 'bg-slate-700/50',
    text: 'text-slate-400'
  },
  collecting_photos: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Сбор фото',
    bg: 'bg-amber-500/15',
    text: 'text-amber-400'
  },
  awaiting_photos: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Ожидание фото',
    bg: 'bg-amber-500/15',
    text: 'text-amber-400'
  },
  ready_to_generate: {
    icon: <CheckCircle2 className="w-3.5 h-3.5" />,
    label: 'Готов',
    bg: 'bg-blue-500/20',
    text: 'text-blue-300'
  },
  generating: {
    icon: <Loader2 className="w-3.5 h-3.5 animate-spin" />,
    label: 'Генерация...',
    bg: 'bg-cyan-500/20',
    text: 'text-cyan-300',
    glow: 'shadow-cyan-500/30'
  },
  in_queue: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'В очереди',
    bg: 'bg-blue-500/15',
    text: 'text-blue-400'
  },
  success: {
    icon: <Sparkles className="w-3.5 h-3.5" />,
    label: 'Готов!',
    bg: 'bg-blue-500/25',
    text: 'text-blue-200',
    glow: 'shadow-blue-500/30'
  },
  error: {
    icon: <AlertCircle className="w-3.5 h-3.5" />,
    label: 'Ошибка',
    bg: 'bg-red-500/20',
    text: 'text-red-400'
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
      <div className="relative overflow-hidden pt-8 pb-20 px-5" style={{ background: 'linear-gradient(145deg, #0f172a 0%, #0a0f1e 100%)' }}>
        {/* Decorative gradient orbs */}
        <div className="absolute top-0 right-0 w-48 h-48 bg-blue-500/10 rounded-full blur-3xl" />
        <div className="absolute bottom-0 left-0 w-32 h-32 bg-blue-600/10 rounded-full blur-3xl" />

        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          className="flex items-center justify-between mb-6 relative z-10"
        >
          <div>
            <h1 className="text-2xl font-bold text-white">Заявки</h1>
            <p className="text-blue-300/70 text-sm mt-0.5">
              {requests.length} {requests.length === 1 ? 'заявка' : requests.length < 5 ? 'заявки' : 'заявок'}
            </p>
          </div>
          <Tooltip content="Создать новую заявку" position="bottom">
            <motion.button
              onClick={() => {
                haptic?.impactOccurred('medium')
                navigate('/requests/new')
              }}
              className="flex items-center gap-2 px-4 py-2.5 rounded-xl font-semibold text-sm shadow-lg text-white border border-blue-500/30"
              style={{ background: 'linear-gradient(145deg, #2563eb 0%, #1d4ed8 100%)' }}
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              <Plus className="w-4 h-4" />
              Новая
            </motion.button>
          </Tooltip>
        </motion.div>

        {/* Search in header */}
        <motion.div
          className="relative z-10"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <Search className={clsx(
            "absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 transition-colors duration-200",
            isSearchFocused ? 'text-blue-400' : 'text-slate-500'
          )} />
          <input
            type="text"
            placeholder="Найти заявку..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onFocus={() => setIsSearchFocused(true)}
            onBlur={() => setIsSearchFocused(false)}
            className="w-full pl-12 pr-12 py-3.5 rounded-2xl text-slate-100 placeholder:text-slate-500 outline-none border-2 transition-all shadow-lg"
            style={{
              background: 'linear-gradient(145deg, #1e293b 0%, #0f172a 100%)',
              borderColor: isSearchFocused ? 'rgba(59, 130, 246, 0.5)' : 'rgba(148, 163, 184, 0.1)'
            }}
          />
          <AnimatePresence>
            {searchQuery && (
              <motion.button
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.8 }}
                onClick={() => setSearchQuery('')}
                className="absolute right-4 top-1/2 -translate-y-1/2 p-1.5 rounded-full bg-slate-700 text-slate-400"
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
          className="flex gap-2 pb-2 scroll-x-container"
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
                'flex items-center gap-2 px-4 py-2.5 rounded-2xl text-sm font-semibold whitespace-nowrap transition-all duration-300 shadow-lg border',
                statusFilter === filter.value
                  ? 'text-white border-blue-500/50 scale-105'
                  : 'text-slate-300 border-slate-700/50 hover:border-blue-500/30'
              )}
              style={{
                background: statusFilter === filter.value
                  ? 'linear-gradient(145deg, #2563eb 0%, #1d4ed8 100%)'
                  : 'linear-gradient(145deg, #1e293b 0%, #0f172a 100%)'
              }}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.1 + idx * 0.05 }}
              whileTap={{ scale: 0.95 }}
            >
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
              className="w-24 h-24 rounded-3xl mx-auto mb-6 flex items-center justify-center shadow-xl border border-blue-500/20"
              style={{ background: 'linear-gradient(145deg, #1e3a8a 0%, #1e40af 100%)' }}
              animate={{ y: [0, -8, 0] }}
              transition={{ repeat: Infinity, duration: 3, ease: "easeInOut" }}
            >
              <Globe className="w-12 h-12 text-blue-200" />
            </motion.div>
            <h3 className="text-xl font-bold text-slate-100 mb-2">
              {searchQuery ? 'Ничего не найдено' : 'Пока нет заявок'}
            </h3>
            <p className="text-slate-400 mb-6 max-w-xs mx-auto">
              {searchQuery ? 'Попробуйте изменить поисковый запрос' : 'Создайте первую заявку и начните работу с клиентами'}
            </p>
            {!searchQuery && (
              <motion.button
                onClick={() => navigate('/requests/new')}
                className="inline-flex items-center gap-2 px-6 py-3.5 text-white rounded-2xl font-semibold shadow-xl border border-blue-500/30"
                style={{ background: 'linear-gradient(145deg, #2563eb 0%, #1d4ed8 100%)' }}
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
                    <div
                      className={clsx(
                        "rounded-3xl p-4 border transition-all duration-300",
                        "hover:shadow-xl hover:-translate-y-1",
                        config.glow && `shadow-lg ${config.glow}`
                      )}
                      style={{
                        background: 'linear-gradient(145deg, #0f172a 0%, #0d1424 100%)',
                        borderColor: 'rgba(59, 130, 246, 0.1)'
                      }}
                    >
                      <div className="flex items-start gap-4">
                        {/* Avatar */}
                        <div className="relative flex-shrink-0">
                          <div className="w-14 h-14 rounded-2xl flex items-center justify-center text-white font-bold text-xl shadow-lg" style={{ background: 'linear-gradient(145deg, #1e3a8a 0%, #1d4ed8 100%)' }}>
                            {request.company_name?.[0]?.toUpperCase() || '?'}
                          </div>
                          {status === 'success' && (
                            <div className="absolute -bottom-1 -right-1 w-5 h-5 bg-blue-500 rounded-lg flex items-center justify-center shadow-lg">
                              <CheckCircle2 className="w-3 h-3 text-white" />
                            </div>
                          )}
                        </div>

                        {/* Content */}
                        <div className="flex-1 min-w-0">
                          <div className="flex items-start justify-between gap-2 mb-1">
                            <h3 className="font-bold text-slate-100 truncate text-[15px]">
                              {request.company_name || 'Без названия'}
                            </h3>
                            <ArrowUpRight className="w-4 h-4 text-slate-600 flex-shrink-0 opacity-0 group-hover:opacity-100 group-hover:text-blue-400 transition-all" />
                          </div>

                          <div className="flex items-center gap-3 text-sm text-slate-400 mb-3">
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
