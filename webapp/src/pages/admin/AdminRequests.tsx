import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { adminApi } from '@/api/client'
import { Search, FileText, Archive, Trash2, ChevronRight, AlertTriangle, X } from 'lucide-react'
import { motion, AnimatePresence } from 'framer-motion'
import toast from 'react-hot-toast'

const MAX_BULK_DELETE = 10

export default function AdminRequests() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [selectedIds, setSelectedIds] = useState<string[]>([])
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [confirmationCode, setConfirmationCode] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['admin', 'requests'],
    queryFn: () => adminApi.requests.list({ limit: 100 }).then(res => res.data),
  })

  const { data: searchResults } = useQuery({
    queryKey: ['admin', 'requests', 'search', search],
    queryFn: () => adminApi.requests.search(search).then(res => res.data),
    enabled: search.length > 2,
  })

  const massArchiveMutation = useMutation({
    mutationFn: (ids: string[]) => adminApi.requests.massArchive(ids),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'requests'] })
      setSelectedIds([])
      toast.success('Заявки архивированы')
    },
  })

  const massDeleteMutation = useMutation({
    mutationFn: ({ ids, code }: { ids: string[]; code?: string }) =>
      adminApi.requests.massDelete(ids, code),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'requests'] })
      setSelectedIds([])
      setShowDeleteConfirm(false)
      setConfirmationCode('')
      toast.success('Заявки удалены')
    },
    onError: (error: any) => {
      const message = error.response?.data?.detail || 'Ошибка удаления'
      toast.error(message)
    },
  })

  const handleMassDelete = () => {
    if (selectedIds.length > MAX_BULK_DELETE) {
      toast.error(`Нельзя удалить более ${MAX_BULK_DELETE} заявок за раз`)
      return
    }

    if (selectedIds.length > 5) {
      setShowDeleteConfirm(true)
    } else {
      massDeleteMutation.mutate({ ids: selectedIds })
    }
  }

  const confirmMassDelete = () => {
    const expectedCode = `DELETE-${selectedIds.length}`
    if (confirmationCode !== expectedCode) {
      toast.error(`Неверный код. Введите: ${expectedCode}`)
      return
    }
    massDeleteMutation.mutate({ ids: selectedIds, code: confirmationCode })
  }

  const requests = search.length > 2 ? searchResults : data?.items

  const toggleSelect = (id: string) => {
    setSelectedIds(prev =>
      prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]
    )
  }

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="skeleton h-20 rounded-2xl" />
        ))}
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold text-tg-text">Все заявки</h1>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Поиск по компании или клиенту..."
          className="w-full pl-10 pr-4 py-3 bg-tg-secondary-bg rounded-xl text-tg-text placeholder:text-tg-hint"
        />
      </div>

      {/* Mass actions */}
      {selectedIds.length > 0 && (
        <div className="flex gap-2 p-3 bg-tg-secondary-bg rounded-xl">
          <span className="text-sm text-tg-text flex-1">
            Выбрано: {selectedIds.length}
            {selectedIds.length > MAX_BULK_DELETE && (
              <span className="text-red-500 ml-2">(макс. {MAX_BULK_DELETE})</span>
            )}
          </span>
          <button
            onClick={() => massArchiveMutation.mutate(selectedIds)}
            disabled={massArchiveMutation.isPending}
            className="p-2 rounded-lg bg-orange-500/20 text-orange-500 disabled:opacity-50"
          >
            <Archive className="w-4 h-4" />
          </button>
          <button
            onClick={handleMassDelete}
            disabled={massDeleteMutation.isPending || selectedIds.length > MAX_BULK_DELETE}
            className="p-2 rounded-lg bg-red-500/20 text-red-500 disabled:opacity-50"
          >
            <Trash2 className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Mass Delete Confirmation Modal */}
      <AnimatePresence>
        {showDeleteConfirm && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/80 z-[100]"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowDeleteConfirm(false)}
            />
            <motion.div
              className="fixed inset-0 flex items-center justify-center p-4 z-[110]"
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              onClick={(e) => e.stopPropagation()}
            >
              <div className="bg-white dark:bg-zinc-900 rounded-3xl p-6 w-full max-w-sm shadow-2xl border border-zinc-200 dark:border-zinc-800">
                <div className="text-center mb-6">
                  <div className="w-16 h-16 bg-red-100 dark:bg-red-900/30 rounded-full flex items-center justify-center mx-auto mb-4">
                    <AlertTriangle className="w-8 h-8 text-red-500" />
                  </div>
                  <h3 className="text-xl font-bold text-tg-text mb-2">Подтверждение удаления</h3>
                  <p className="text-tg-hint text-sm leading-relaxed mb-4">
                    Вы собираетесь удалить <strong>{selectedIds.length}</strong> заявок.
                    Это действие нельзя отменить.
                  </p>
                  <p className="text-sm text-tg-text mb-2">
                    Для подтверждения введите код:
                  </p>
                  <p className="text-lg font-mono font-bold text-red-500 mb-4">
                    DELETE-{selectedIds.length}
                  </p>
                  <input
                    type="text"
                    value={confirmationCode}
                    onChange={(e) => setConfirmationCode(e.target.value)}
                    placeholder="Введите код подтверждения"
                    className="w-full px-4 py-3 bg-zinc-100 dark:bg-zinc-800 rounded-xl text-center font-mono"
                  />
                </div>
                <div className="flex gap-3">
                  <button
                    onClick={() => {
                      setShowDeleteConfirm(false)
                      setConfirmationCode('')
                    }}
                    className="flex-1 px-4 py-3.5 rounded-xl bg-zinc-100 dark:bg-zinc-800 text-tg-text font-semibold"
                  >
                    Отмена
                  </button>
                  <button
                    onClick={confirmMassDelete}
                    disabled={massDeleteMutation.isPending}
                    className="flex-1 px-4 py-3.5 rounded-xl bg-red-500 text-white font-semibold disabled:opacity-50"
                  >
                    {massDeleteMutation.isPending ? 'Удаление...' : 'Удалить'}
                  </button>
                </div>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Requests list */}
      <div className="space-y-3">
        {requests?.length === 0 ? (
          <p className="text-center text-tg-hint py-8">Нет заявок</p>
        ) : (
          requests?.map((request: any) => (
            <div
              key={request.id}
              className="bg-tg-secondary-bg rounded-2xl p-4 flex items-center gap-3"
            >
              <input
                type="checkbox"
                checked={selectedIds.includes(request.id)}
                onChange={() => toggleSelect(request.id)}
                className="w-5 h-5 rounded"
              />
              <div
                className="flex-1 cursor-pointer"
                onClick={() => navigate(`/requests/${request.id}`)}
              >
                <div className="flex items-center gap-2">
                  <FileText className="w-4 h-4 text-tg-hint" />
                  <p className="font-medium text-tg-text">
                    {request.company_name || 'Без названия'}
                  </p>
                </div>
                <p className="text-sm text-tg-hint">
                  {request.client_name} • {request.status}
                </p>
                {request.manager_first_name && (
                  <p className="text-xs text-tg-hint">
                    Менеджер: {request.manager_first_name}
                  </p>
                )}
              </div>
              <ChevronRight className="w-5 h-5 text-tg-hint" />
            </div>
          ))
        )}
      </div>
    </div>
  )
}

