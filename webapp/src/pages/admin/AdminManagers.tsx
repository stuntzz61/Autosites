import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import {
  User, CheckCircle, XCircle, Ban, Unlock, Trash2,
  ChevronRight, Shield, UserCog, X, FileText, BarChart3
} from 'lucide-react'
import { motion, AnimatePresence } from 'framer-motion'
import toast from 'react-hot-toast'

interface Manager {
  id: string
  tg_id: number
  username?: string
  first_name: string
  last_name?: string
  contact?: string
  role: string
  approval_status: string
  is_blocked: boolean
  request_count?: number
  total_requests?: number
  completed_requests?: number
  created_at: string
}

export default function AdminManagers() {
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<'all' | 'pending'>('all')
  const [selectedManager, setSelectedManager] = useState<Manager | null>(null)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)

  const { data: managers, isLoading } = useQuery({
    queryKey: ['admin', 'managers'],
    queryFn: () => adminApi.managers.list().then(res => res.data),
  })

  const { data: pending } = useQuery({
    queryKey: ['admin', 'pending'],
    queryFn: () => adminApi.pending.list().then(res => res.data),
  })

  const approveMutation = useMutation({
    mutationFn: (id: string) => adminApi.pending.approve(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin'] })
      toast.success('Менеджер одобрен')
    },
  })

  const rejectMutation = useMutation({
    mutationFn: (id: string) => adminApi.pending.reject(id, 'Отклонено администратором'),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin'] })
      toast.success('Заявка отклонена')
    },
  })

  const blockMutation = useMutation({
    mutationFn: (id: string) => adminApi.managers.block(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'managers'] })
      toast.success('Менеджер заблокирован')
      setSelectedManager(null)
    },
  })

  const unblockMutation = useMutation({
    mutationFn: (id: string) => adminApi.managers.unblock(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'managers'] })
      toast.success('Менеджер разблокирован')
      setSelectedManager(null)
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => adminApi.managers.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'managers'] })
      toast.success('Менеджер удалён')
      setSelectedManager(null)
      setShowDeleteConfirm(false)
    },
    onError: () => {
      toast.error('Ошибка удаления')
    },
  })

  const makeAdminMutation = useMutation({
    mutationFn: (id: string) => adminApi.managers.makeAdmin(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'managers'] })
      toast.success('Роль изменена на админа')
      setSelectedManager(null)
    },
    onError: () => {
      toast.error('Ошибка')
    },
  })

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="skeleton h-20 rounded-2xl" />
        ))}
      </div>
    )
  }

  const pendingCount = pending?.length || 0

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold text-tg-text">Менеджеры</h1>

      {/* Tabs */}
      <div className="flex gap-2">
        <button
          onClick={() => setTab('all')}
          className={`px-4 py-2 rounded-xl text-sm font-medium transition-colors ${
            tab === 'all'
              ? 'bg-tg-accent text-white'
              : 'bg-tg-secondary-bg text-tg-text'
          }`}
        >
          Все ({managers?.length || 0})
        </button>
        <button
          onClick={() => setTab('pending')}
          className={`px-4 py-2 rounded-xl text-sm font-medium transition-colors ${
            tab === 'pending'
              ? 'bg-tg-accent text-white'
              : 'bg-tg-secondary-bg text-tg-text'
          }`}
        >
          Ожидают ({pendingCount})
        </button>
      </div>

      {/* Pending registrations */}
      {tab === 'pending' && (
        <div className="space-y-3">
          {pending?.length === 0 ? (
            <p className="text-center text-tg-hint py-8">Нет заявок на регистрацию</p>
          ) : (
            pending?.map((user: any) => (
              <div key={user.id} className="bg-tg-secondary-bg rounded-2xl p-4">
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-full bg-tg-accent/20 flex items-center justify-center">
                      <User className="w-5 h-5 text-tg-accent" />
                    </div>
                    <div>
                      <p className="font-medium text-tg-text">
                        {user.first_name} {user.last_name}
                      </p>
                      {user.username && (
                        <p className="text-sm text-tg-hint">@{user.username}</p>
                      )}
                      {user.contact && (
                        <p className="text-sm text-tg-hint">{user.contact}</p>
                      )}
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={() => approveMutation.mutate(user.id)}
                      disabled={approveMutation.isPending}
                      className="p-2 rounded-xl bg-green-500/20 text-green-500"
                    >
                      <CheckCircle className="w-5 h-5" />
                    </button>
                    <button
                      onClick={() => rejectMutation.mutate(user.id)}
                      disabled={rejectMutation.isPending}
                      className="p-2 rounded-xl bg-red-500/20 text-red-500"
                    >
                      <XCircle className="w-5 h-5" />
                    </button>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      )}

      {/* All managers */}
      {tab === 'all' && (
        <div className="space-y-3">
          {managers?.length === 0 ? (
            <p className="text-center text-tg-hint py-8">Нет менеджеров</p>
          ) : (
            managers?.map((manager: Manager) => (
              <button
                key={manager.id}
                onClick={() => setSelectedManager(manager)}
                className="w-full bg-tg-secondary-bg rounded-2xl p-4 text-left"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                      manager.is_blocked ? 'bg-red-500/20' :
                      manager.role === 'admin' ? 'bg-purple-500/20' : 'bg-tg-accent/20'
                    }`}>
                      {manager.role === 'admin' ? (
                        <Shield className={`w-5 h-5 text-purple-500`} />
                      ) : (
                        <User className={`w-5 h-5 ${
                          manager.is_blocked ? 'text-red-500' : 'text-tg-accent'
                        }`} />
                      )}
                    </div>
                    <div>
                      <p className="font-medium text-tg-text">
                        {manager.first_name} {manager.last_name}
                        {manager.is_blocked && (
                          <span className="ml-2 text-xs text-red-500">(заблокирован)</span>
                        )}
                        {manager.role === 'admin' && (
                          <span className="ml-2 text-xs text-purple-500">(админ)</span>
                        )}
                      </p>
                      {manager.username && (
                        <p className="text-sm text-tg-hint">@{manager.username}</p>
                      )}
                      <p className="text-xs text-tg-hint">
                        Заявок: {manager.request_count || manager.total_requests || 0}
                      </p>
                    </div>
                  </div>
                  <ChevronRight className="w-5 h-5 text-tg-hint" />
                </div>
              </button>
            ))
          )}
        </div>
      )}

      {/* Manager Detail Modal */}
      <AnimatePresence>
        {selectedManager && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setSelectedManager(null)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl z-50 safe-bottom max-h-[85vh] flex flex-col"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              {/* Drag handle - fixed */}
              <div className="flex-shrink-0 pt-3 pb-2">
                <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto" />
              </div>

              {/* Scrollable content */}
              <div className="flex-1 overflow-y-auto px-4 pb-2">
                {/* Header */}
                <div className="flex items-center gap-4 mb-6">
                  <div className={`w-16 h-16 rounded-2xl flex items-center justify-center ${
                    selectedManager.is_blocked ? 'bg-red-500/20' :
                    selectedManager.role === 'admin' ? 'bg-purple-500/20' : 'bg-tg-accent/20'
                  }`}>
                    {selectedManager.role === 'admin' ? (
                      <Shield className="w-8 h-8 text-purple-500" />
                    ) : (
                      <User className={`w-8 h-8 ${
                        selectedManager.is_blocked ? 'text-red-500' : 'text-tg-accent'
                      }`} />
                    )}
                  </div>
                  <div className="flex-1 min-w-0">
                    <h2 className="text-xl font-bold text-tg-text truncate">
                      {selectedManager.first_name} {selectedManager.last_name}
                    </h2>
                    {selectedManager.username && (
                      <a
                        href={`https://t.me/${selectedManager.username}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-tg-accent text-sm"
                      >
                        @{selectedManager.username}
                      </a>
                    )}
                  </div>
                  <button
                    onClick={() => setSelectedManager(null)}
                    className="p-2 rounded-full bg-tg-secondary-bg"
                  >
                    <X className="w-5 h-5 text-tg-hint" />
                  </button>
                </div>

                {/* Stats */}
                <div className="grid grid-cols-2 gap-3 mb-4">
                  <div className="bg-tg-secondary-bg rounded-xl p-3">
                    <div className="flex items-center gap-2 text-tg-hint mb-1">
                      <FileText className="w-4 h-4" />
                      <span className="text-xs">Заявок</span>
                    </div>
                    <p className="text-2xl font-bold text-tg-text">
                      {selectedManager.request_count || selectedManager.total_requests || 0}
                    </p>
                  </div>
                  <div className="bg-tg-secondary-bg rounded-xl p-3">
                    <div className="flex items-center gap-2 text-tg-hint mb-1">
                      <BarChart3 className="w-4 h-4" />
                      <span className="text-xs">Завершено</span>
                    </div>
                    <p className="text-2xl font-bold text-green-500">
                      {selectedManager.completed_requests || 0}
                    </p>
                  </div>
                </div>

                {/* Info */}
                <div className="bg-tg-secondary-bg rounded-xl p-4 mb-4 space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-tg-hint">Telegram ID</span>
                    <span className="text-tg-text font-mono">{selectedManager.tg_id}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-tg-hint">Роль</span>
                    <span className="text-tg-text">{selectedManager.role === 'admin' ? 'Администратор' : 'Менеджер'}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-tg-hint">Статус</span>
                    <span className={selectedManager.is_blocked ? 'text-red-500' : 'text-green-500'}>
                      {selectedManager.is_blocked ? 'Заблокирован' : 'Активен'}
                    </span>
                  </div>
                  {selectedManager.contact && (
                    <div className="flex justify-between">
                      <span className="text-tg-hint">Контакт</span>
                      <span className="text-tg-text">{selectedManager.contact}</span>
                    </div>
                  )}
                  <div className="flex justify-between">
                    <span className="text-tg-hint">Регистрация</span>
                    <span className="text-tg-text">
                      {new Date(selectedManager.created_at).toLocaleDateString('ru')}
                    </span>
                  </div>
                </div>
              </div>

              {/* Actions - fixed at bottom */}
              <div className="flex-shrink-0 px-4 pb-4 pt-2 border-t border-tg-separator bg-tg-bg space-y-2">
                <div className="grid grid-cols-2 gap-2">
                  {/* Block/Unblock */}
                  {selectedManager.is_blocked ? (
                    <button
                      onClick={() => unblockMutation.mutate(selectedManager.id)}
                      disabled={unblockMutation.isPending}
                      className="p-3 rounded-xl bg-green-500/20 text-green-600 font-medium flex items-center justify-center gap-2 text-sm"
                    >
                      <Unlock className="w-4 h-4" />
                      Разблокировать
                    </button>
                  ) : (
                    <button
                      onClick={() => blockMutation.mutate(selectedManager.id)}
                      disabled={blockMutation.isPending}
                      className="p-3 rounded-xl bg-orange-500/20 text-orange-600 font-medium flex items-center justify-center gap-2 text-sm"
                    >
                      <Ban className="w-4 h-4" />
                      Заблокировать
                    </button>
                  )}

                  {/* Make Admin */}
                  {selectedManager.role !== 'admin' ? (
                    <button
                      onClick={() => makeAdminMutation.mutate(selectedManager.id)}
                      disabled={makeAdminMutation.isPending}
                      className="p-3 rounded-xl bg-purple-500/20 text-purple-600 font-medium flex items-center justify-center gap-2 text-sm"
                    >
                      <Shield className="w-4 h-4" />
                      Сделать админом
                    </button>
                  ) : (
                    <div />
                  )}
                </div>

                {/* Delete - always visible */}
                <button
                  onClick={() => setShowDeleteConfirm(true)}
                  className="w-full p-3 rounded-xl bg-red-500/10 text-red-500 font-medium flex items-center justify-center gap-2"
                >
                  <Trash2 className="w-5 h-5" />
                  Удалить менеджера
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Delete Confirmation */}
      <AnimatePresence>
        {showDeleteConfirm && selectedManager && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-50"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowDeleteConfirm(false)}
            />
            <motion.div
              className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-tg-bg rounded-2xl p-6 z-50 w-[90%] max-w-sm"
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
            >
              <h3 className="text-lg font-semibold text-tg-text mb-2">Удалить менеджера?</h3>
              <p className="text-tg-hint mb-4">
                {selectedManager.first_name} {selectedManager.last_name} будет удалён. Это действие нельзя отменить.
              </p>
              <div className="flex gap-3">
                <button
                  onClick={() => setShowDeleteConfirm(false)}
                  className="flex-1 p-3 rounded-xl bg-tg-secondary-bg text-tg-text font-medium"
                >
                  Отмена
                </button>
                <button
                  onClick={() => deleteMutation.mutate(selectedManager.id)}
                  disabled={deleteMutation.isPending}
                  className="flex-1 p-3 rounded-xl bg-red-500 text-white font-medium"
                >
                  {deleteMutation.isPending ? 'Удаление...' : 'Удалить'}
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  )
}
