import { motion, AnimatePresence } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { Archive, ChevronRight, Search, RefreshCw } from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { useTelegram } from '@/contexts/TelegramContext'
import { requestsApi } from '@/api/client'
import toast from 'react-hot-toast'

export default function ArchivePage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { haptic, webApp } = useTelegram()
  const [searchQuery, setSearchQuery] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['requests', 'archived'],
    queryFn: () => requestsApi.list({ status: 'archived' }).then(res => res.data),
  })

  const restoreMutation = useMutation({
    mutationFn: (id: string) => requestsApi.updateStatus(id, 'draft'),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['requests'] })
      toast.success('Восстановлено')
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

  const handleRestore = (id: string, e: React.MouseEvent) => {
    e.stopPropagation()
    webApp?.showConfirm('Восстановить?', (confirmed) => {
      if (confirmed) {
        haptic?.impactOccurred('medium')
        restoreMutation.mutate(id)
      }
    })
  }

  return (
    <div className="min-h-screen bg-tg-bg">
      {/* Header */}
      <div className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator">
        <div className="p-4 space-y-3">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-tg-secondary-bg">
              <Archive className="w-6 h-6 text-tg-hint" />
            </div>
            <div>
              <h1 className="text-xl font-bold">Архив</h1>
              <p className="text-sm text-tg-hint">{requests.length} заявок</p>
            </div>
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
        </div>
      </div>

      <div className="p-4">
        {isLoading ? (
          <div className="space-y-3">
            {[1, 2, 3].map(i => <div key={i} className="skeleton h-20 rounded-2xl" />)}
          </div>
        ) : filteredRequests.length === 0 ? (
          <motion.div className="text-center py-12" initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
            <div className="w-16 h-16 rounded-2xl bg-tg-secondary-bg mx-auto mb-4 flex items-center justify-center">
              <Archive className="w-8 h-8 text-tg-hint" />
            </div>
            <p className="font-medium mb-1">Архив пуст</p>
            <p className="text-sm text-tg-hint">{searchQuery ? 'Ничего не найдено' : 'Здесь будут завершённые заявки'}</p>
          </motion.div>
        ) : (
          <AnimatePresence mode="popLayout">
            <div className="space-y-3">
              {filteredRequests.map((request: any, index: number) => (
                <motion.div
                  key={request.id}
                  className="bg-tg-section rounded-2xl overflow-hidden border border-tg-separator"
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, scale: 0.95 }}
                  transition={{ delay: index * 0.05 }}
                  layout
                >
                  <button
                    onClick={() => {
                      haptic?.impactOccurred('light')
                      navigate(`/requests/${request.id}`)
                    }}
                    className="w-full p-4 text-left"
                  >
                    <div className="flex items-start gap-3">
                      <div className="w-10 h-10 rounded-xl bg-tg-secondary-bg flex items-center justify-center font-bold">
                        {request.company_name?.[0]?.toUpperCase() || '?'}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="font-semibold truncate">{request.company_name || 'Без названия'}</p>
                        <p className="text-sm text-tg-hint truncate">{request.client_name || 'Без клиента'}</p>
                        <p className="text-xs text-tg-hint mt-1">
                          {new Date(request.created_at).toLocaleDateString('ru-RU')}
                        </p>
                      </div>
                      <ChevronRight className="w-5 h-5 text-tg-hint" />
                    </div>
                  </button>
                  <div className="border-t border-tg-separator px-4 py-2">
                    <button
                      onClick={(e) => handleRestore(request.id, e)}
                      disabled={restoreMutation.isPending}
                      className="flex items-center gap-2 text-sm"
                    >
                      <RefreshCw className="w-4 h-4" />
                      Восстановить
                    </button>
                  </div>
                </motion.div>
              ))}
            </div>
          </AnimatePresence>
        )}
      </div>
    </div>
  )
}
