import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { adminApi } from '@/api/client'
import {
  Search, X, ChevronRight, Calendar, Building2, User, Users2,
  SlidersHorizontal
} from 'lucide-react'
import { motion, AnimatePresence } from 'framer-motion'
import clsx from 'clsx'

const statusConfig: Record<string, {
  label: string
  color: string
  bg: string
}> = {
  draft: { label: 'Черновик', color: 'text-gray-500', bg: 'bg-gray-500/10' },
  collecting_info: { label: 'Сбор данных', color: 'text-blue-500', bg: 'bg-blue-500/10' },
  collecting_photos: { label: 'Сбор фото', color: 'text-yellow-500', bg: 'bg-yellow-500/10' },
  awaiting_photos: { label: 'Ожидание фото', color: 'text-yellow-500', bg: 'bg-yellow-500/10' },
  ready_to_generate: { label: 'Готов к генерации', color: 'text-green-500', bg: 'bg-green-500/10' },
  generating: { label: 'Генерация', color: 'text-purple-500', bg: 'bg-purple-500/10' },
  in_queue: { label: 'В очереди', color: 'text-blue-500', bg: 'bg-blue-500/10' },
  success: { label: 'Завершён', color: 'text-green-500', bg: 'bg-green-500/10' },
  error: { label: 'Ошибка', color: 'text-red-500', bg: 'bg-red-500/10' },
}

const statusOptions = [
  { value: '', label: 'Все статусы' },
  { value: 'draft', label: 'Черновик' },
  { value: 'collecting_info', label: 'Сбор данных' },
  { value: 'ready_to_generate', label: 'Готов к генерации' },
  { value: 'generating', label: 'Генерация' },
  { value: 'success', label: 'Завершён' },
  { value: 'error', label: 'Ошибка' },
]

interface Request {
  id: string
  company_name: string
  client_name: string
  status: string
  created_at: string
  manager_id?: string
  manager_first_name?: string
  manager_last_name?: string
  manager_username?: string
  group_id?: string
  group_name?: string
  tariff?: string
}

interface Manager {
  id: string
  first_name: string
  last_name?: string
  username?: string
}

interface Group {
  id: string
  name: string
}

export default function AdminOverview() {
  const navigate = useNavigate()
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [managerFilter, setManagerFilter] = useState('')
  const [groupFilter, setGroupFilter] = useState('')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [showFilters, setShowFilters] = useState(false)
  const [sortBy, setSortBy] = useState<'date' | 'company' | 'status'>('date')
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc')

  // Fetch all data
  const { data: requestsData, isLoading: requestsLoading } = useQuery({
    queryKey: ['admin', 'requests', 'all'],
    queryFn: () => adminApi.requests.list({ limit: 500 }).then(res => res.data),
  })

  const { data: managers = [] } = useQuery({
    queryKey: ['admin', 'managers'],
    queryFn: () => adminApi.managers.list().then(res => res.data),
  })

  const { data: groups = [] } = useQuery({
    queryKey: ['admin', 'groups'],
    queryFn: () => adminApi.groups.list().then(res => res.data),
  })

  const requests: Request[] = requestsData?.items || []

  // Filter and sort requests
  const filteredRequests = useMemo(() => {
    let result = [...requests]

    // Search filter
    if (search) {
      const searchLower = search.toLowerCase()
      result = result.filter(req =>
        req.company_name?.toLowerCase().includes(searchLower) ||
        req.client_name?.toLowerCase().includes(searchLower) ||
        req.manager_first_name?.toLowerCase().includes(searchLower) ||
        req.manager_username?.toLowerCase().includes(searchLower)
      )
    }

    // Status filter
    if (statusFilter) {
      result = result.filter(req => req.status === statusFilter)
    }

    // Manager filter
    if (managerFilter) {
      result = result.filter(req => req.manager_id === managerFilter)
    }

    // Group filter
    if (groupFilter) {
      result = result.filter(req => req.group_id === groupFilter)
    }

    // Date filters
    if (dateFrom) {
      const fromDate = new Date(dateFrom)
      result = result.filter(req => new Date(req.created_at) >= fromDate)
    }
    if (dateTo) {
      const toDate = new Date(dateTo)
      toDate.setHours(23, 59, 59, 999)
      result = result.filter(req => new Date(req.created_at) <= toDate)
    }

    // Sorting
    result.sort((a, b) => {
      let comparison = 0
      switch (sortBy) {
        case 'date':
          comparison = new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
          break
        case 'company':
          comparison = (a.company_name || '').localeCompare(b.company_name || '')
          break
        case 'status':
          comparison = (a.status || '').localeCompare(b.status || '')
          break
      }
      return sortOrder === 'asc' ? comparison : -comparison
    })

    return result
  }, [requests, search, statusFilter, managerFilter, groupFilter, dateFrom, dateTo, sortBy, sortOrder])

  const activeFiltersCount = [statusFilter, managerFilter, groupFilter, dateFrom, dateTo].filter(Boolean).length

  const clearFilters = () => {
    setStatusFilter('')
    setManagerFilter('')
    setGroupFilter('')
    setDateFrom('')
    setDateTo('')
  }

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('ru-RU', {
      day: 'numeric',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  const getStatusConfig = (status: string) => {
    return statusConfig[status] || { label: status, color: 'text-gray-500', bg: 'bg-gray-500/10' }
  }

  if (requestsLoading) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="skeleton h-24 rounded-2xl" />
        ))}
      </div>
    )
  }

  return (
    <div className="min-h-screen" style={{ background: 'var(--bg-deep)' }}>
      {/* Header */}
      <div className="px-4 pt-4 pb-2">
        <h1 className="text-2xl font-bold" style={{ color: 'var(--text-primary)' }}>
          Обзор заявок
        </h1>
        <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
          {filteredRequests.length} из {requests.length} заявок
        </p>
      </div>

      {/* Search and Filter Bar */}
      <div className="px-4 pb-3 space-y-3">
        <div className="flex gap-2">
          {/* Search */}
          <div className="relative flex-1">
            <Search
              className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5"
              style={{ color: 'var(--text-subtle)' }}
            />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Поиск по названию, клиенту, менеджеру..."
              className="w-full pl-10 pr-4 py-3 rounded-xl text-sm"
              style={{
                background: 'var(--bg-surface)',
                border: '1px solid var(--border-default)',
                color: 'var(--text-primary)',
              }}
            />
            {search && (
              <button
                onClick={() => setSearch('')}
                className="absolute right-3 top-1/2 -translate-y-1/2"
              >
                <X className="w-4 h-4" style={{ color: 'var(--text-subtle)' }} />
              </button>
            )}
          </div>

          {/* Filter Toggle */}
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={clsx(
              'px-4 py-3 rounded-xl flex items-center gap-2 transition-colors',
              showFilters || activeFiltersCount > 0
                ? 'bg-blue-500 text-white'
                : ''
            )}
            style={!showFilters && activeFiltersCount === 0 ? {
              background: 'var(--bg-surface)',
              border: '1px solid var(--border-default)',
              color: 'var(--text-primary)',
            } : undefined}
          >
            <SlidersHorizontal className="w-5 h-5" />
            {activeFiltersCount > 0 && (
              <span className="text-sm font-medium">{activeFiltersCount}</span>
            )}
          </button>
        </div>

        {/* Expanded Filters */}
        <AnimatePresence>
          {showFilters && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              className="overflow-hidden"
            >
              <div
                className="p-4 rounded-xl space-y-3"
                style={{
                  background: 'var(--bg-surface)',
                  border: '1px solid var(--border-default)',
                }}
              >
                {/* Status */}
                <div>
                  <label className="text-xs font-medium mb-1 block" style={{ color: 'var(--text-subtle)' }}>
                    Статус
                  </label>
                  <select
                    value={statusFilter}
                    onChange={(e) => setStatusFilter(e.target.value)}
                    className="w-full px-3 py-2 rounded-lg text-sm"
                    style={{
                      background: 'var(--bg-elevated)',
                      border: '1px solid var(--border-default)',
                      color: 'var(--text-primary)',
                    }}
                  >
                    {statusOptions.map(opt => (
                      <option key={opt.value} value={opt.value}>{opt.label}</option>
                    ))}
                  </select>
                </div>

                {/* Manager */}
                <div>
                  <label className="text-xs font-medium mb-1 block" style={{ color: 'var(--text-subtle)' }}>
                    Менеджер
                  </label>
                  <select
                    value={managerFilter}
                    onChange={(e) => setManagerFilter(e.target.value)}
                    className="w-full px-3 py-2 rounded-lg text-sm"
                    style={{
                      background: 'var(--bg-elevated)',
                      border: '1px solid var(--border-default)',
                      color: 'var(--text-primary)',
                    }}
                  >
                    <option value="">Все менеджеры</option>
                    {managers.map((m: Manager) => (
                      <option key={m.id} value={m.id}>
                        {m.first_name} {m.last_name || ''} {m.username ? `(@${m.username})` : ''}
                      </option>
                    ))}
                  </select>
                </div>

                {/* Group */}
                <div>
                  <label className="text-xs font-medium mb-1 block" style={{ color: 'var(--text-subtle)' }}>
                    Группа
                  </label>
                  <select
                    value={groupFilter}
                    onChange={(e) => setGroupFilter(e.target.value)}
                    className="w-full px-3 py-2 rounded-lg text-sm"
                    style={{
                      background: 'var(--bg-elevated)',
                      border: '1px solid var(--border-default)',
                      color: 'var(--text-primary)',
                    }}
                  >
                    <option value="">Все группы</option>
                    {groups.map((g: Group) => (
                      <option key={g.id} value={g.id}>{g.name}</option>
                    ))}
                  </select>
                </div>

                {/* Date Range */}
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="text-xs font-medium mb-1 block" style={{ color: 'var(--text-subtle)' }}>
                      Дата от
                    </label>
                    <input
                      type="date"
                      value={dateFrom}
                      onChange={(e) => setDateFrom(e.target.value)}
                      className="w-full px-3 py-2 rounded-lg text-sm"
                      style={{
                        background: 'var(--bg-elevated)',
                        border: '1px solid var(--border-default)',
                        color: 'var(--text-primary)',
                      }}
                    />
                  </div>
                  <div>
                    <label className="text-xs font-medium mb-1 block" style={{ color: 'var(--text-subtle)' }}>
                      Дата до
                    </label>
                    <input
                      type="date"
                      value={dateTo}
                      onChange={(e) => setDateTo(e.target.value)}
                      className="w-full px-3 py-2 rounded-lg text-sm"
                      style={{
                        background: 'var(--bg-elevated)',
                        border: '1px solid var(--border-default)',
                        color: 'var(--text-primary)',
                      }}
                    />
                  </div>
                </div>

                {/* Sort */}
                <div className="flex gap-3">
                  <div className="flex-1">
                    <label className="text-xs font-medium mb-1 block" style={{ color: 'var(--text-subtle)' }}>
                      Сортировка
                    </label>
                    <select
                      value={sortBy}
                      onChange={(e) => setSortBy(e.target.value as any)}
                      className="w-full px-3 py-2 rounded-lg text-sm"
                      style={{
                        background: 'var(--bg-elevated)',
                        border: '1px solid var(--border-default)',
                        color: 'var(--text-primary)',
                      }}
                    >
                      <option value="date">По дате</option>
                      <option value="company">По компании</option>
                      <option value="status">По статусу</option>
                    </select>
                  </div>
                  <div className="flex-1">
                    <label className="text-xs font-medium mb-1 block" style={{ color: 'var(--text-subtle)' }}>
                      Порядок
                    </label>
                    <select
                      value={sortOrder}
                      onChange={(e) => setSortOrder(e.target.value as any)}
                      className="w-full px-3 py-2 rounded-lg text-sm"
                      style={{
                        background: 'var(--bg-elevated)',
                        border: '1px solid var(--border-default)',
                        color: 'var(--text-primary)',
                      }}
                    >
                      <option value="desc">Новые сначала</option>
                      <option value="asc">Старые сначала</option>
                    </select>
                  </div>
                </div>

                {/* Clear Filters */}
                {activeFiltersCount > 0 && (
                  <button
                    onClick={clearFilters}
                    className="w-full py-2 text-sm font-medium text-red-500 hover:bg-red-500/10 rounded-lg transition-colors"
                  >
                    Сбросить фильтры
                  </button>
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Requests List */}
      <div className="px-4 pb-24 space-y-3">
        {filteredRequests.length === 0 ? (
          <div className="text-center py-12">
            <Search className="w-16 h-16 mx-auto mb-4" style={{ color: 'var(--text-subtle)' }} />
            <p style={{ color: 'var(--text-muted)' }}>
              {search || activeFiltersCount > 0 ? 'Ничего не найдено' : 'Нет заявок'}
            </p>
          </div>
        ) : (
          filteredRequests.map((request, index) => {
            const config = getStatusConfig(request.status)

            return (
              <motion.button
                key={request.id}
                onClick={() => navigate(`/requests/${request.id}`)}
                className="w-full text-left"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.02 }}
              >
                <div
                  className="p-4 rounded-xl transition-all hover:scale-[1.01]"
                  style={{
                    background: 'var(--bg-surface)',
                    border: '1px solid var(--border-default)',
                  }}
                >
                  <div className="flex items-start gap-3">
                    {/* Avatar */}
                    <div
                      className="w-12 h-12 rounded-xl flex items-center justify-center font-bold text-lg flex-shrink-0"
                      style={{
                        background: 'var(--accent-primary-bg)',
                        color: 'var(--accent-primary)',
                      }}
                    >
                      {request.company_name?.[0]?.toUpperCase() || '?'}
                    </div>

                    {/* Content */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <h3
                          className="font-semibold truncate"
                          style={{ color: 'var(--text-primary)' }}
                        >
                          {request.company_name || 'Без названия'}
                        </h3>
                        {request.tariff === 'premium' && (
                          <span
                            className="px-2 py-0.5 rounded text-[10px] font-bold"
                            style={{
                              background: 'linear-gradient(135deg, #fbbf24 0%, #f59e0b 100%)',
                              color: '#1a1a1a',
                            }}
                          >
                            PREMIUM
                          </span>
                        )}
                      </div>

                      <div className="flex items-center gap-2 text-xs mb-2" style={{ color: 'var(--text-subtle)' }}>
                        <span className="flex items-center gap-1">
                          <Building2 className="w-3 h-3" />
                          {request.client_name || 'Без клиента'}
                        </span>
                        <span>•</span>
                        <span className="flex items-center gap-1">
                          <Calendar className="w-3 h-3" />
                          {formatDate(request.created_at)}
                        </span>
                      </div>

                      <div className="flex items-center gap-2 flex-wrap">
                        {/* Status Badge */}
                        <span className={clsx('px-2 py-1 rounded-lg text-xs font-medium', config.color, config.bg)}>
                          {config.label}
                        </span>

                        {/* Manager */}
                        {request.manager_first_name && (
                          <span
                            className="flex items-center gap-1 px-2 py-1 rounded-lg text-xs"
                            style={{ background: 'var(--bg-elevated)', color: 'var(--text-muted)' }}
                          >
                            <User className="w-3 h-3" />
                            {request.manager_first_name}
                          </span>
                        )}

                        {/* Group */}
                        {request.group_name && (
                          <span
                            className="flex items-center gap-1 px-2 py-1 rounded-lg text-xs"
                            style={{ background: 'var(--bg-elevated)', color: 'var(--text-muted)' }}
                          >
                            <Users2 className="w-3 h-3" />
                            {request.group_name}
                          </span>
                        )}
                      </div>
                    </div>

                    <ChevronRight className="w-5 h-5 flex-shrink-0" style={{ color: 'var(--text-subtle)' }} />
                  </div>
                </div>
              </motion.button>
            )
          })
        )}
      </div>
    </div>
  )
}

