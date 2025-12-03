import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import {
  FileText, Search, Archive, Trash2, CheckCircle2,
  Clock, AlertCircle, Loader2, ChevronRight
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { useTelegram } from '@/contexts/TelegramContext'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const statusFilters = [
  { value: 'all', label: 'Все' },
  { value: 'draft', label: 'Черновики' },
  { value: 'ready_to_generate', label: 'Готовы' },
  { value: 'generating', label: 'В работе' },
  { value: 'success', label: 'Готово' },
  { value: 'error', label: 'Ошибки' },
  { value: 'archived', label: 'Архив' },
]

const statusConfig: Record<string, { icon: React.ReactNode; color: string; label: string }> = {
  draft: { icon: <Clock className="w-4 h-4" />, color: 'text-[#65676b]', label: 'Черновик' },
  ready_to_generate: { icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-[#42b72a]', label: 'Готов' },
  generating: { icon: <Loader2 className="w-4 h-4 animate-spin" />, color: 'text-[#1877f2]', label: 'В работе' },
  success: { icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-emerald-500', label: 'Готово' },
  error: { icon: <AlertCircle className="w-4 h-4" />, color: 'text-red-500', label: 'Ошибка' },
  archived: { icon: <Archive className="w-4 h-4" />, color: 'text-purple-500', label: 'Архив' },
}

export default function AdminRequests() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic, webApp } = useTelegram()
  const [searchQuery, setSearchQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState('all')
  const [selectedIds, setSelectedIds] = useState<string[]>([])
  const [selectionMode, setSelectionMode] = useState(false)

  const { data, isLoading } = useQuery({
    queryKey: ['admin-requests', statusFilter],
    queryFn: () => adminApi.requests.list({ status: statusFilter !== 'all' ? statusFilter : undefined }).then(res => res.data),
  })

  const massArchiveMutation = useMutation({
    mutationFn: (ids: string[]) => adminApi.requests.massArchive(ids),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-requests'] })
      toast.success('Заявки архивированы')
      setSelectedIds([])
      setSelectionMode(false)
    },
    onError: () => toast.error('Ошибка'),
  })

  const massDeleteMutation = useMutation({
    mutationFn: (ids: string[]) => adminApi.requests.massDelete(ids),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-requests'] })
      toast.success('Заявки удалены')
      setSelectedIds([])
      setSelectionMode(false)
    },
    onError: () => toast.error('Ошибка'),
  })

  const requests = data?.items || []

  const filteredRequests = requests.filter((req: any) => {
    if (!searchQuery) return true
    const search = searchQuery.toLowerCase()
    return (
      req.company_name?.toLowerCase().includes(search) ||
      req.client_name?.toLowerCase().includes(search)
    )
  })

  const toggleSelection = (id: string) => {
    setSelectedIds(prev =>
      prev.includes(id) ? prev.filter(i => i !== id) : [...prev, id]
    )
  }

  const handleMassArchive = () => {
    webApp?.showConfirm(`Архивировать ${selectedIds.length} заявок?`, (confirmed) => {
      if (confirmed) massArchiveMutation.mutate(selectedIds)
    })
  }

  const handleMassDelete = () => {
    webApp?.showConfirm(`Удалить ${selectedIds.length} заявок? Это действие нельзя отменить.`, (confirmed) => {
      if (confirmed) massDeleteMutation.mutate(selectedIds)
    })
  }

  return (
    <div className="min-h-screen pb-20">
      {/* Header */}
      <div className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator">
        <div className="p-4 space-y-3">
          <div className="flex items-center justify-between">
            <h2 className="font-semibold text-tg-text">
              {filteredRequests.length} заявок
            </h2>
            <button
              onClick={() => {
                haptic?.impactOccurred('light')
                setSelectionMode(!selectionMode)
                setSelectedIds([])
              }}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-sm font-medium transition-colors',
                selectionMode ? 'bg-[#1877f2] dark:bg-[#e4e6eb] text-white dark:text-[#18191a]' : 'text-[#1877f2] dark:text-[#e4e6eb]'
              )}
            >
              {selectionMode ? 'Отмена' : 'Выбрать'}
            </button>
          </div>

          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
            <input
              type="text"
              placeholder="Поиск заявки..."
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
                  setStatusFilter(filter.value)
                }}
                className={clsx(
                  'px-3 py-1.5 rounded-full text-sm font-medium whitespace-nowrap transition-colors',
                  statusFilter === filter.value
                    ? 'bg-[#1877f2] dark:bg-[#e4e6eb] text-white dark:text-[#18191a]'
                    : 'bg-[#e4e6eb] dark:bg-[#3e4042] text-tg-hint'
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
              <FileText className="w-8 h-8 text-tg-hint" />
            </div>
            <p className="text-tg-text font-medium mb-1">Нет заявок</p>
            <p className="text-sm text-tg-hint">
              {searchQuery ? 'Попробуйте другой запрос' : 'Список пуст'}
            </p>
          </motion.div>
        ) : (
          <AnimatePresence mode="popLayout">
            <div className="space-y-3">
              {filteredRequests.map((request: any, index: number) => {
                const status = request.payload?.site?.meta?.status || request.status || 'draft'
                const config = statusConfig[status] || statusConfig.draft
                const isSelected = selectedIds.includes(request.id)

                return (
                  <motion.button
                    key={request.id}
                    onClick={() => {
                      if (selectionMode) {
                        haptic?.selectionChanged()
                        toggleSelection(request.id)
                      } else {
                        haptic?.impactOccurred('light')
                        navigate(`/requests/${request.id}`)
                      }
                    }}
                    className={clsx(
                      'w-full bg-tg-section rounded-2xl p-4 text-left active:scale-[0.98] transition-all border border-[#e4e6eb] dark:border-[#3e4042]',
                      isSelected && 'ring-2 ring-[#1877f2] dark:ring-white'
                    )}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95 }}
                    transition={{ delay: index * 0.03 }}
                    layout
                  >
                    <div className="flex items-start gap-3">
                      {selectionMode && (
                        <div className={clsx(
                          'w-6 h-6 rounded-full border-2 flex items-center justify-center flex-shrink-0 mt-1',
                          isSelected ? 'bg-[#1877f2] dark:bg-white border-[#1877f2] dark:border-white' : 'border-tg-hint'
                        )}>
                          {isSelected && <CheckCircle2 className="w-4 h-4 text-white dark:text-[#18191a]" />}
                        </div>
                      )}
                      <div className="flex-shrink-0 w-10 h-10 rounded-xl bg-gradient-to-br from-[#1877f2] to-[#0d65d9] dark:from-[#3e4042] dark:to-[#242526] flex items-center justify-center text-white font-bold text-lg">
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
                      {!selectionMode && (
                        <ChevronRight className="w-5 h-5 text-tg-hint flex-shrink-0" />
                      )}
                    </div>
                  </motion.button>
                )
              })}
            </div>
          </AnimatePresence>
        )}
      </div>

      {/* Selection Actions */}
      <AnimatePresence>
        {selectionMode && selectedIds.length > 0 && (
          <motion.div
            className="fixed bottom-0 left-0 right-0 bg-tg-bg border-t border-tg-separator p-4 safe-bottom"
            initial={{ y: '100%' }}
            animate={{ y: 0 }}
            exit={{ y: '100%' }}
          >
            <div className="flex items-center justify-between mb-3">
              <p className="text-sm text-tg-text">
                Выбрано: <span className="font-semibold">{selectedIds.length}</span>
              </p>
              <button
                onClick={() => setSelectedIds(filteredRequests.map((r: any) => r.id))}
                className="text-sm text-[#1877f2] dark:text-[#e4e6eb]"
              >
                Выбрать все
              </button>
            </div>
            <div className="flex gap-3">
              <button
                onClick={handleMassArchive}
                disabled={massArchiveMutation.isPending}
                className="btn btn-secondary flex-1"
              >
                {massArchiveMutation.isPending ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <>
                    <Archive className="w-5 h-5" />
                    В архив
                  </>
                )}
              </button>
              <button
                onClick={handleMassDelete}
                disabled={massDeleteMutation.isPending}
                className="btn btn-destructive flex-1"
              >
                {massDeleteMutation.isPending ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <>
                    <Trash2 className="w-5 h-5" />
                    Удалить
                  </>
                )}
              </button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
