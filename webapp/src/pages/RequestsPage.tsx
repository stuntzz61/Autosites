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
    bg: 'rgba(100, 116, 139, 0.15)',
    text: '#94a3b8'
  },
  collecting_info: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Сбор данных',
    bg: 'rgba(59, 130, 246, 0.15)',
    text: '#60a5fa'
  },
  collecting_photos: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Сбор фото',
    bg: 'rgba(245, 158, 11, 0.15)',
    text: '#fbbf24'
  },
  awaiting_photos: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Ожидание фото',
    bg: 'rgba(245, 158, 11, 0.15)',
    text: '#fbbf24'
  },
  ready_to_generate: {
    icon: <CheckCircle2 className="w-3.5 h-3.5" />,
    label: 'Готов',
    bg: 'rgba(16, 185, 129, 0.15)',
    text: '#34d399'
  },
  generating: {
    icon: <Loader2 className="w-3.5 h-3.5 animate-spin" />,
    label: 'Генерация...',
    bg: 'rgba(139, 92, 246, 0.15)',
    text: '#a78bfa',
    glow: 'shadow-purple-500/20'
  },
  in_queue: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'В очереди',
    bg: 'rgba(139, 92, 246, 0.15)',
    text: '#a78bfa'
  },
  success: {
    icon: <Sparkles className="w-3.5 h-3.5" />,
    label: 'Готов!',
    bg: 'rgba(16, 185, 129, 0.15)',
    text: '#34d399',
    glow: 'shadow-emerald-500/20'
  },
  error: {
    icon: <AlertCircle className="w-3.5 h-3.5" />,
    label: 'Ошибка',
    bg: 'rgba(239, 68, 68, 0.15)',
    text: '#f87171'
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
    <div className="min-h-screen" style={{ background: 'var(--tg-theme-bg-color)' }}>
      {/* Premium Header */}
      <div className="relative overflow-hidden pt-8 pb-20 px-5" style={{ background: 'var(--tg-theme-bg-color)' }}>
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          className="flex items-center justify-between mb-6 relative z-10"
        >
          <div>
            <h1 className="text-2xl font-bold" style={{ color: 'var(--tg-theme-text-color)' }}>Заявки</h1>
            <p className="text-sm mt-0.5" style={{ color: 'var(--tg-theme-hint-color)' }}>
              {requests.length} {requests.length === 1 ? 'заявка' : requests.length < 5 ? 'заявки' : 'заявок'}
            </p>
          </div>
          <Tooltip content="Создать новую заявку" position="bottom">
            <motion.button
              onClick={() => {
                haptic?.impactOccurred('medium')
                navigate('/requests/new')
              }}
              className="flex items-center gap-2 px-4 py-2.5 rounded-xl font-semibold text-sm text-white"
              style={{
                background: 'linear-gradient(135deg, var(--accent-blue) 0%, var(--accent-blue-dark) 100%)',
                boxShadow: '0 4px 16px -4px rgba(59, 130, 246, 0.5)'
              }}
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
          <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 transition-colors duration-200"
            style={{ color: isSearchFocused ? 'var(--accent-blue-light)' : 'var(--tg-theme-hint-color)' }}
          />
          <input
            type="text"
            placeholder="Найти заявку..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onFocus={() => setIsSearchFocused(true)}
            onBlur={() => setIsSearchFocused(false)}
            className="w-full pl-12 pr-12 py-3.5 rounded-2xl outline-none transition-all"
            style={{
              background: 'var(--surface-secondary)',
              borderColor: isSearchFocused ? 'var(--border-accent)' : 'var(--border-subtle)',
              border: '1px solid',
              color: 'var(--tg-theme-text-color)',
              boxShadow: isSearchFocused ? '0 0 0 3px var(--accent-blue-glow)' : 'none'
            }}
          />
          <AnimatePresence>
            {searchQuery && (
              <motion.button
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.8 }}
                onClick={() => setSearchQuery('')}
                className="absolute right-4 top-1/2 -translate-y-1/2 p-1.5 rounded-full"
                style={{ background: 'var(--surface-tertiary)', color: 'var(--tg-theme-hint-color)' }}
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
              className="flex items-center gap-2 px-4 py-2.5 rounded-2xl text-sm font-semibold whitespace-nowrap transition-all duration-300"
              style={{
                background: statusFilter === filter.value
                  ? 'var(--accent-blue)'
                  : 'var(--surface-secondary)',
                border: `1px solid ${statusFilter === filter.value ? 'var(--accent-blue)' : 'var(--border-subtle)'}`,
                color: statusFilter === filter.value
                  ? 'white'
                  : 'var(--tg-theme-subtitle-text-color)',
                boxShadow: statusFilter === filter.value
                  ? '0 4px 16px -4px rgba(59, 130, 246, 0.4)'
                  : 'none'
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
              className="w-24 h-24 rounded-3xl mx-auto mb-6 flex items-center justify-center shadow-xl"
              style={{
                background: 'var(--surface-secondary)',
                border: '1px solid var(--border-accent)'
              }}
              animate={{ y: [0, -8, 0] }}
              transition={{ repeat: Infinity, duration: 3, ease: "easeInOut" }}
            >
              <Globe className="w-12 h-12" style={{ color: 'var(--accent-blue-light)' }} />
            </motion.div>
            <h3 className="text-xl font-bold mb-2" style={{ color: 'var(--tg-theme-text-color)' }}>
              {searchQuery ? 'Ничего не найдено' : 'Пока нет заявок'}
            </h3>
            <p className="mb-6 max-w-xs mx-auto" style={{ color: 'var(--tg-theme-hint-color)' }}>
              {searchQuery ? 'Попробуйте изменить поисковый запрос' : 'Создайте первую заявку и начните работу с клиентами'}
            </p>
            {!searchQuery && (
              <motion.button
                onClick={() => navigate('/requests/new')}
                className="inline-flex items-center gap-2 px-6 py-3.5 rounded-2xl font-semibold text-white"
                style={{
                  background: 'linear-gradient(135deg, var(--accent-blue) 0%, var(--accent-blue-dark) 100%)',
                  boxShadow: '0 4px 16px -4px rgba(59, 130, 246, 0.5)'
                }}
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
                        "rounded-3xl p-4 border transition-all duration-300 relative overflow-hidden",
                        "hover:shadow-xl hover:-translate-y-1",
                        config.glow && `shadow-lg ${config.glow}`
                      )}
                      style={{
                        background: request.tariff === 'premium'
                          ? 'linear-gradient(135deg, rgba(139, 92, 246, 0.15) 0%, rgba(59, 130, 246, 0.1) 50%, rgba(139, 92, 246, 0.15) 100%)'
                          : 'var(--surface-secondary)',
                        borderColor: request.tariff === 'premium'
                          ? 'rgba(139, 92, 246, 0.3)'
                          : 'var(--border-subtle)'
                      }}
                    >
                      {request.tariff === 'premium' && (
                        <div className="absolute top-0 right-0 w-32 h-32 bg-gradient-to-br from-purple-500/10 to-blue-500/10 rounded-full blur-2xl" />
                      )}
                      <div className="flex items-start gap-4">
                        {/* Avatar */}
                        <div className="relative flex-shrink-0">
                          <div className="w-14 h-14 rounded-2xl flex items-center justify-center font-bold text-xl shadow-lg" style={{
                            background: 'rgba(59, 130, 246, 0.15)',
                            border: '1px solid var(--border-accent)',
                            color: 'var(--accent-blue-light)'
                          }}>
                            {request.company_name?.[0]?.toUpperCase() || '?'}
                          </div>
                          {status === 'success' && (
                            <div className="absolute -bottom-1 -right-1 w-5 h-5 rounded-lg flex items-center justify-center shadow-lg" style={{
                              background: 'rgba(16, 185, 129, 0.2)',
                              border: '1px solid rgba(16, 185, 129, 0.3)'
                            }}>
                              <CheckCircle2 className="w-3 h-3 text-emerald-400" />
                            </div>
                          )}
                        </div>

                        {/* Content */}
                        <div className="flex-1 min-w-0 relative z-10">
                          <div className="flex items-start justify-between gap-2 mb-1">
                            <div className="flex items-center gap-2 flex-wrap min-w-0">
                              <h3 className="font-bold truncate text-[15px]" style={{ color: 'var(--tg-theme-text-color)' }}>
                                {request.company_name || 'Без названия'}
                              </h3>
                              {request.tariff === 'premium' && (
                                <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[9px] font-bold uppercase tracking-wide text-white flex-shrink-0" style={{
                                  background: 'linear-gradient(135deg, #8b5cf6 0%, #3b82f6 100%)',
                                  boxShadow: '0 2px 8px -2px rgba(139, 92, 246, 0.4)'
                                }}>
                                  <Sparkles className="w-2.5 h-2.5" />
                                  PRO
                                </span>
                              )}
                            </div>
                            <ArrowUpRight className="w-4 h-4 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-all" style={{ color: 'var(--accent-blue-light)' }} />
                          </div>

                          <div className="flex items-center gap-3 text-sm mb-3" style={{ color: 'var(--tg-theme-hint-color)' }}>
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
                          <div
                            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-semibold border"
                            style={{
                              background: config.bg,
                              color: config.text,
                              borderColor: config.text + '30'
                            }}
                          >
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
