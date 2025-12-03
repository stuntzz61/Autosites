import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { FileText, Search, Archive, Trash2, CheckCircle2, Clock, AlertCircle, Loader2, ChevronRight } from 'lucide-react'
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
  { value: 'archived', label: 'Архив' },
]

const statusConfig: Record<string, { icon: React.ReactNode; label: string }> = {
  draft: { icon: <Clock className="w-4 h-4" />, label: 'Черновик' },
  collecting_info: { icon: <Clock className="w-4 h-4" />, label: 'Сбор данных' },
  collecting_photos: { icon: <Clock className="w-4 h-4" />, label: 'Сбор фото' },
  awaiting_photos: { icon: <Clock className="w-4 h-4" />, label: 'Ожидание фото' },
  ready_to_generate: { icon: <CheckCircle2 className="w-4 h-4" />, label: 'Готов к отправке' },
  generating: { icon: <Loader2 className="w-4 h-4 animate-spin" />, label: 'Генерация...' },
  in_queue: { icon: <Clock className="w-4 h-4" />, label: 'В очереди' },
  success: { icon: <CheckCircle2 className="w-4 h-4" />, label: 'Сайт готов!' },
  error: { icon: <AlertCircle className="w-4 h-4" />, label: 'Ошибка' },
  archived: { icon: <Archive className="w-4 h-4" />, label: 'В архиве' },
}

// Normalize legacy statuses
const normalizeStatus = (status: string): string => {
  const map: Record<string, string> = {
    'generated_ok': 'success',
    'generated_error': 'error',
    'ready': 'ready_to_generate',
  }
  return map[status] || status
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
      toast.success('Архивировано')
      setSelectedIds([])
      setSelectionMode(false)
    },
    onError: () => toast.error('Ошибка'),
  })

  const massDeleteMutation = useMutation({
    mutationFn: (ids: string[]) => adminApi.requests.massDelete(ids),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-requests'] })
      toast.success('Удалено')
      setSelectedIds([])
      setSelectionMode(false)
    },
    onError: () => toast.error('Ошибка'),
  })

  const requests = data?.items || []
  const filteredRequests = requests.filter((req: any) => {
    if (!searchQuery) return true
    const s = searchQuery.toLowerCase()
    return req.company_name?.toLowerCase().includes(s) || req.client_name?.toLowerCase().includes(s)
  })

  const toggleSelection = (id: string) => {
    setSelectedIds(prev => prev.includes(id) ? prev.filter(i => i !== id) : [...prev, id])
  }

  const handleMassArchive = () => {
    webApp?.showConfirm(`Архивировать ${selectedIds.length}?`, (ok) => {
      if (ok) massArchiveMutation.mutate(selectedIds)
    })
  }

  const handleMassDelete = () => {
    webApp?.showConfirm(`Удалить ${selectedIds.length}?`, (ok) => {
      if (ok) massDeleteMutation.mutate(selectedIds)
    })
  }

  return (
    <div className="min-h-screen pb-20">
      <div className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator">
        <div className="p-4 space-y-3">
          <div className="flex justify-between">
            <h2 className="font-semibold">{filteredRequests.length} заявок</h2>
            <button
              onClick={() => {
                haptic?.impactOccurred('light')
                setSelectionMode(!selectionMode)
                setSelectedIds([])
              }}
              className={clsx('px-3 py-1.5 rounded-lg text-sm font-medium', selectionMode ? 'bg-black dark:bg-white text-white dark:text-black' : '')}
            >
              {selectionMode ? 'Отмена' : 'Выбрать'}
            </button>
          </div>

          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
            <input
              type="text"
              placeholder="Поиск..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="input pl-10"
            />
          </div>

          <div className="flex gap-2 overflow-x-auto pb-1 -mx-4 px-4">
            {statusFilters.map(filter => (
              <button
                key={filter.value}
                onClick={() => {
                  haptic?.selectionChanged()
                  setStatusFilter(filter.value)
                }}
                className={clsx(
                  'px-3 py-1.5 rounded-full text-sm font-medium whitespace-nowrap',
                  statusFilter === filter.value ? 'bg-black dark:bg-white text-white dark:text-black' : 'bg-tg-secondary-bg text-tg-hint'
                )}
              >
                {filter.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="p-4">
        {isLoading ? (
          <div className="space-y-3">{[1, 2, 3].map(i => <div key={i} className="skeleton h-20 rounded-2xl" />)}</div>
        ) : filteredRequests.length === 0 ? (
          <motion.div className="text-center py-12" initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
            <FileText className="w-12 h-12 text-tg-hint mx-auto mb-4" />
            <p className="font-medium mb-1">Нет заявок</p>
          </motion.div>
        ) : (
          <AnimatePresence mode="popLayout">
            <div className="space-y-3">
              {filteredRequests.map((request: any, index: number) => {
                const rawStatus = request.payload?.site?.meta?.status || request.status || 'draft'
                const status = normalizeStatus(rawStatus)
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
                      'w-full bg-tg-section rounded-2xl p-4 text-left active:scale-[0.98] border border-tg-separator',
                      isSelected && 'ring-2 ring-black dark:ring-white'
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
                          isSelected ? 'bg-black dark:bg-white border-black dark:border-white' : 'border-tg-hint'
                        )}>
                          {isSelected && <CheckCircle2 className="w-4 h-4 text-white dark:text-black" />}
                        </div>
                      )}
                      <div className="w-10 h-10 rounded-xl bg-black dark:bg-white text-white dark:text-black flex items-center justify-center font-bold">
                        {request.company_name?.[0]?.toUpperCase() || '?'}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="font-semibold truncate">{request.company_name || 'Без названия'}</p>
                        <p className="text-sm text-tg-hint truncate">{request.client_name || 'Без клиента'}</p>
                        <div className="flex items-center gap-1 mt-1 text-tg-hint">
                          {config.icon}
                          <span className="text-xs">{config.label}</span>
                        </div>
                      </div>
                      {!selectionMode && <ChevronRight className="w-5 h-5 text-tg-hint" />}
                    </div>
                  </motion.button>
                )
              })}
            </div>
          </AnimatePresence>
        )}
      </div>

      <AnimatePresence>
        {selectionMode && selectedIds.length > 0 && (
          <motion.div
            className="fixed bottom-0 left-0 right-0 bg-tg-bg border-t border-tg-separator p-4 safe-bottom"
            initial={{ y: '100%' }}
            animate={{ y: 0 }}
            exit={{ y: '100%' }}
          >
            <div className="flex justify-between mb-3">
              <p className="text-sm">Выбрано: <span className="font-semibold">{selectedIds.length}</span></p>
              <button onClick={() => setSelectedIds(filteredRequests.map((r: any) => r.id))} className="text-sm">Выбрать все</button>
            </div>
            <div className="flex gap-3">
              <button onClick={handleMassArchive} disabled={massArchiveMutation.isPending} className="btn btn-secondary flex-1">
                {massArchiveMutation.isPending ? <Loader2 className="w-5 h-5 animate-spin" /> : <><Archive className="w-5 h-5" /> Архив</>}
              </button>
              <button onClick={handleMassDelete} disabled={massDeleteMutation.isPending} className="btn btn-destructive flex-1">
                {massDeleteMutation.isPending ? <Loader2 className="w-5 h-5 animate-spin" /> : <><Trash2 className="w-5 h-5" /> Удалить</>}
              </button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
