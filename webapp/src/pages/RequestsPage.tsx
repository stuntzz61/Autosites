import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useNavigate, useSearchParams } from 'react-router-dom'
import {
  Plus, Search, ChevronRight, Globe, Clock, CheckCircle2,
  AlertCircle, Loader2, X, Sparkles, Calendar, Building2,
  Crown
} from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import Tooltip from '@/components/Tooltip'

const statusFilters = [
  { value: 'all', label: 'Все' },
  { value: 'draft', label: 'Черновик' },
  { value: 'ready_to_generate', label: 'Готов' },
  { value: 'generating', label: 'Генерация' },
  { value: 'success', label: 'Готово' },
]

const statusConfig: Record<string, {
  icon: React.ReactNode
  label: string
  bg: string
  color: string
  border: string
}> = {
  draft: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Черновик',
    bg: 'rgba(100, 116, 139, 0.12)',
    color: 'var(--text-muted)',
    border: 'rgba(100, 116, 139, 0.2)'
  },
  collecting_info: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Сбор данных',
    bg: 'rgba(59, 130, 246, 0.12)',
    color: 'var(--accent-primary-light)',
    border: 'rgba(59, 130, 246, 0.25)'
  },
  collecting_photos: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Сбор фото',
    bg: 'var(--warning-bg)',
    color: 'var(--warning-light)',
    border: 'var(--warning-border)'
  },
  awaiting_photos: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'Ожидание фото',
    bg: 'var(--warning-bg)',
    color: 'var(--warning-light)',
    border: 'var(--warning-border)'
  },
  ready_to_generate: {
    icon: <CheckCircle2 className="w-3.5 h-3.5" />,
    label: 'Готов',
    bg: 'var(--success-bg)',
    color: 'var(--success-light)',
    border: 'var(--success-border)'
  },
  generating: {
    icon: <Loader2 className="w-3.5 h-3.5 animate-spin" />,
    label: 'Генерация...',
    bg: 'var(--info-bg)',
    color: 'var(--info-light)',
    border: 'var(--info-border)'
  },
  in_queue: {
    icon: <Clock className="w-3.5 h-3.5" />,
    label: 'В очереди',
    bg: 'var(--info-bg)',
    color: 'var(--info-light)',
    border: 'var(--info-border)'
  },
  success: {
    icon: <Sparkles className="w-3.5 h-3.5" />,
    label: 'Готов!',
    bg: 'var(--success-bg)',
    color: 'var(--success-light)',
    border: 'var(--success-border)'
  },
  error: {
    icon: <AlertCircle className="w-3.5 h-3.5" />,
    label: 'Ошибка',
    bg: 'var(--error-bg)',
    color: 'var(--error-light)',
    border: 'var(--error-border)'
  },
}

interface Request {
  id: string
  company_name: string
  client_name: string
  status: string
  tariff?: string
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
    <div className="min-h-screen" style={{ background: 'var(--bg-deep)' }}>
      {/* Header */}
      <div
        className="relative overflow-hidden pt-10 pb-6 px-5"
        style={{ background: 'linear-gradient(180deg, var(--bg-deep) 0%, var(--bg-elevated) 100%)' }}
      >
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          className="flex items-start justify-between mb-6 relative z-10 gap-4"
        >
          <div className="flex-1 min-w-0">
            <h1 className="text-2xl font-bold tracking-tight" style={{ color: 'var(--text-primary)' }}>
              Заявки
            </h1>
            <p className="text-sm mt-1" style={{ color: 'var(--text-subtle)' }}>
              {requests.length} {requests.length === 1 ? 'заявка' : requests.length < 5 ? 'заявки' : 'заявок'}
            </p>
          </div>
          <Tooltip content="Создать новую заявку" position="bottom">
            <motion.button
              onClick={() => {
                haptic?.impactOccurred('medium')
                navigate('/requests/new')
              }}
              className="flex items-center gap-2 px-4 py-2.5 rounded-xl font-semibold text-sm text-white flex-shrink-0"
              style={{
                background: 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 100%)',
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

        {/* Search */}
        <motion.div
          className="relative z-10"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <Search
            className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 transition-colors duration-200"
            style={{ color: isSearchFocused ? 'var(--accent-primary-light)' : 'var(--text-subtle)' }}
          />
          <input
            type="text"
            placeholder="Найти заявку..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onFocus={() => setIsSearchFocused(true)}
            onBlur={() => setIsSearchFocused(false)}
            className="input pl-12 pr-12"
          />
          <AnimatePresence>
            {searchQuery && (
              <motion.button
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.8 }}
                onClick={() => setSearchQuery('')}
                className="absolute right-4 top-1/2 -translate-y-1/2 p-1.5 rounded-full transition-colors"
                style={{ background: 'var(--bg-tertiary)', color: 'var(--text-muted)' }}
              >
                <X className="w-3.5 h-3.5" />
              </motion.button>
            )}
          </AnimatePresence>
        </motion.div>
      </div>

      {/* Filters - Horizontal Scroll */}
      <div className="mb-4 relative z-10">
        <motion.div
          className="flex gap-2 px-4 pb-2 overflow-x-auto scrollbar-hide"
          style={{
            scrollSnapType: 'x proximity',
            WebkitOverflowScrolling: 'touch',
            scrollbarWidth: 'none',
            msOverflowStyle: 'none'
          }}
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
              className="px-4 py-2.5 rounded-xl text-sm font-semibold whitespace-nowrap transition-all duration-200 flex-shrink-0"
              style={{
                background: statusFilter === filter.value
                  ? 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 100%)'
                  : 'var(--bg-surface)',
                border: `1px solid ${statusFilter === filter.value ? 'var(--accent-primary)' : 'var(--border-default)'}`,
                color: statusFilter === filter.value ? 'white' : 'var(--text-muted)',
                boxShadow: statusFilter === filter.value
                  ? '0 4px 16px -4px rgba(59, 130, 246, 0.4)'
                  : 'none'
              }}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.1 + idx * 0.03 }}
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
                className="skeleton h-28 rounded-2xl"
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
              className="w-24 h-24 rounded-2xl mx-auto mb-6 flex items-center justify-center"
              style={{
                background: 'var(--bg-surface)',
                border: '1px solid var(--border-accent)',
                boxShadow: '0 4px 24px rgba(0, 0, 0, 0.2)'
              }}
              animate={{ y: [0, -8, 0] }}
              transition={{ repeat: Infinity, duration: 3, ease: "easeInOut" }}
            >
              <Globe className="w-12 h-12" style={{ color: 'var(--accent-primary-light)' }} />
            </motion.div>
            <h3 className="text-xl font-bold mb-2" style={{ color: 'var(--text-primary)' }}>
              {searchQuery ? 'Ничего не найдено' : 'Пока нет заявок'}
            </h3>
            <p className="mb-6 max-w-xs mx-auto" style={{ color: 'var(--text-muted)' }}>
              {searchQuery ? 'Попробуйте изменить поисковый запрос' : 'Создайте первую заявку и начните работу с клиентами'}
            </p>
            {!searchQuery && (
              <motion.button
                onClick={() => navigate('/requests/new')}
                className="btn btn-primary"
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
                const isPremium = request.tariff === 'premium'

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
                    transition={{ delay: index * 0.03, duration: 0.25 }}
                    layout
                  >
                    <div
                      className="rounded-2xl p-4 transition-all duration-200 relative overflow-hidden hover:-translate-y-0.5 active:scale-[0.99]"
                      style={{
                        background: isPremium
                          ? 'linear-gradient(135deg, rgba(139, 92, 246, 0.08) 0%, var(--bg-surface) 50%, rgba(59, 130, 246, 0.08) 100%)'
                          : 'var(--bg-surface)',
                        border: `1px solid ${isPremium ? 'rgba(139, 92, 246, 0.25)' : 'var(--border-default)'}`,
                        boxShadow: isPremium
                          ? '0 4px 24px rgba(139, 92, 246, 0.1)'
                          : '0 4px 16px rgba(0, 0, 0, 0.15)'
                      }}
                    >
                      {/* Premium glow effect */}
                      {isPremium && (
                        <div
                          className="absolute top-0 right-0 w-32 h-32 rounded-full blur-3xl opacity-20 pointer-events-none"
                          style={{ background: 'var(--info)' }}
                        />
                      )}

                      <div className="flex items-start gap-4 relative z-10">
                        {/* Avatar */}
                        <div className="relative flex-shrink-0">
                          <div
                            className="w-14 h-14 rounded-xl flex items-center justify-center font-bold text-xl"
                            style={{
                              background: isPremium
                                ? 'linear-gradient(135deg, var(--info-bg) 0%, rgba(139, 92, 246, 0.2) 100%)'
                                : 'rgba(59, 130, 246, 0.12)',
                              border: `1px solid ${isPremium ? 'var(--info-border)' : 'var(--border-accent)'}`,
                              color: isPremium ? 'var(--info-light)' : 'var(--accent-primary-light)'
                            }}
                          >
                            {request.company_name?.[0]?.toUpperCase() || '?'}
                          </div>
                          {status === 'success' && (
                            <div
                              className="absolute -bottom-1 -right-1 w-5 h-5 rounded-md flex items-center justify-center"
                              style={{
                                background: 'var(--success-bg)',
                                border: '1px solid var(--success-border)'
                              }}
                            >
                              <CheckCircle2 className="w-3 h-3" style={{ color: 'var(--success-light)' }} />
                            </div>
                          )}
                        </div>

                        {/* Content */}
                        <div className="flex-1 min-w-0">
                          <div className="flex items-start justify-between gap-2 mb-1">
                            <div className="flex items-center gap-2 flex-wrap min-w-0">
                              <h3
                                className="font-bold truncate text-[15px]"
                                style={{ color: 'var(--text-primary)' }}
                              >
                                {request.company_name || 'Без названия'}
                              </h3>
                              {isPremium && (
                                <span className="badge-premium inline-flex items-center gap-1 text-[9px] px-2 py-1">
                                  <Crown className="w-2.5 h-2.5" />
                                  PREMIUM
                                </span>
                              )}
                            </div>
                            <ChevronRight
                              className="w-4 h-4 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-all group-hover:translate-x-0.5"
                              style={{ color: 'var(--accent-primary-light)' }}
                            />
                          </div>

                          <div
                            className="flex items-center gap-3 text-sm mb-3"
                            style={{ color: 'var(--text-subtle)' }}
                          >
                            <span className="flex items-center gap-1.5 truncate">
                              <Building2 className="w-3.5 h-3.5 flex-shrink-0" />
                              {request.client_name || 'Без клиента'}
                            </span>
                            <span className="flex items-center gap-1.5 flex-shrink-0">
                              <Calendar className="w-3.5 h-3.5" />
                              {formatDate(request.created_at)}
                            </span>
                          </div>

                          {/* Status Badge */}
                          <div
                            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold"
                            style={{
                              background: config.bg,
                              color: config.color,
                              border: `1px solid ${config.border}`
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
