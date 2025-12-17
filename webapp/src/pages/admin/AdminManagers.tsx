import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import {
  User, CheckCircle, XCircle, Ban, Unlock, Trash2,
  ChevronRight, Shield, X, FileText, BarChart3, Users2, ArrowRight, Download, Info, Crown
} from 'lucide-react'
import { getRoleLabel } from '@/stores/authStore'
import { motion, AnimatePresence } from 'framer-motion'
import toast from 'react-hot-toast'

interface Manager {
  id: string
  tg_id: number
  username?: string
  first_name: string
  last_name?: string
  full_name?: string
  phone?: string
  email?: string
  contact?: string
  role: string
  approval_status: string
  is_blocked: boolean
  request_count?: number
  total_requests?: number
  completed_requests?: number
  created_at: string
  group_id?: string
  group_name?: string
  stats?: {
    total_requests: number
    completed_requests: number
    pending_requests: number
    this_week: number
    today: number
  }
  requests?: Array<{
    id: string
    company_name: string
    status: string
    created_at: string
    updated_at: string
    project_id?: string
    project_name?: string
  }>
}

export default function AdminManagers() {
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<'all' | 'pending'>('all')
  const [selectedManager, setSelectedManager] = useState<Manager | null>(null)
  const [managerToDelete, setManagerToDelete] = useState<Manager | null>(null)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [showMoveGroupModal, setShowMoveGroupModal] = useState(false)
  const [managerToMove, setManagerToMove] = useState<Manager | null>(null)
  const [detailedManager, setDetailedManager] = useState<Manager | null>(null)
  const [showDetailedInfo, setShowDetailedInfo] = useState(false)

  const { data: managers, isLoading, error: managersError } = useQuery({
    queryKey: ['admin', 'managers'],
    queryFn: () => adminApi.managers.list().then(res => res.data),
  })

  // Handle error separately
  useEffect(() => {
    if (managersError) {
      const error = managersError as any
      console.error('Error fetching managers:', error)
      toast.error(error.response?.data?.detail || 'Ошибка загрузки менеджеров')
    }
  }, [managersError])

  const { data: detailedManagerData } = useQuery({
    queryKey: ['admin', 'managers', detailedManager?.id],
    queryFn: () => detailedManager ? adminApi.managers.get(detailedManager.id).then(res => res.data) : null,
    enabled: !!detailedManager && showDetailedInfo,
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
    mutationFn: (id: string) => adminApi.managers.delete(id, 'Confirmed by admin'),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'managers'] })
      toast.success('Менеджер удалён')
      setSelectedManager(null)
      setManagerToDelete(null)
      setShowDeleteConfirm(false)
    },
    onError: (error: any) => {
      const message = error.response?.data?.detail || 'Ошибка удаления'
      toast.error(message)
      setShowDeleteConfirm(false)
      setManagerToDelete(null)
    },
  })

  const makeSupervisorMutation = useMutation({
    mutationFn: (id: string) => adminApi.managers.makeSupervisor(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'managers'] })
      toast.success('Роль изменена на админа')
      setSelectedManager(null)
    },
    onError: () => {
      toast.error('Ошибка')
    },
  })

  const { data: groups = [] } = useQuery({
    queryKey: ['admin', 'groups'],
    queryFn: () => adminApi.groups.list().then(res => res.data),
  })

  const moveGroupMutation = useMutation({
    mutationFn: ({ managerId, groupId }: { managerId: string; groupId: string }) =>
      adminApi.managers.moveGroup(managerId, groupId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'managers'] })
      toast.success('Менеджер перемещен в группу')
      setShowMoveGroupModal(false)
      setManagerToMove(null)
      setSelectedManager(null)
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка перемещения')
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
          Все ({Array.isArray(managers) ? managers.length : 0})
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
          {managersError ? (
            <div className="text-center text-red-500 py-8">
              <p>Ошибка загрузки менеджеров</p>
              <p className="text-sm text-tg-hint mt-2">
                {(managersError as any)?.response?.data?.detail || (managersError as Error)?.message || 'Неизвестная ошибка'}
              </p>
            </div>
          ) : !managers || (Array.isArray(managers) && managers.length === 0) ? (
            <p className="text-center text-tg-hint py-8">Нет менеджеров</p>
          ) : (
            (Array.isArray(managers) ? managers : []).map((manager: Manager) => (
              <button
                key={manager.id}
                onClick={() => setSelectedManager(manager)}
                className="w-full bg-tg-secondary-bg rounded-2xl p-4 text-left"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                      manager.is_blocked ? 'bg-red-500/20' :
                      manager.role === 'owner' ? 'bg-yellow-500/20' :
                      manager.role === 'director' ? 'bg-yellow-400/20' :
                      manager.role === 'supervisor' ? 'bg-purple-500/20' : 'bg-tg-accent/20'
                    }`}>
                      {manager.role === 'owner' ? (
                        <Crown className={`w-5 h-5 text-yellow-500`} />
                      ) : manager.role === 'director' ? (
                        <Crown className={`w-5 h-5 text-yellow-400`} />
                      ) : manager.role === 'supervisor' ? (
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
                        {manager.role === 'owner' && (
                          <span className="ml-2 text-xs text-yellow-500">(владелец)</span>
                        )}
                        {manager.role === 'director' && (
                          <span className="ml-2 text-xs text-yellow-400">(директор)</span>
                        )}
                        {manager.role === 'supervisor' && (
                          <span className="ml-2 text-xs text-purple-500">(супервайзер)</span>
                        )}
                      </p>
                      {manager.username && (
                        <p className="text-sm text-tg-hint">@{manager.username}</p>
                      )}
                      {manager.group_name && (
                        <p className="text-xs text-tg-accent font-medium">
                          Группа: {manager.group_name}
                        </p>
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
              className="fixed inset-0 z-40 backdrop-blur-sm"
              style={{ background: 'rgba(0, 0, 0, 0.4)' }}
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
                    selectedManager.role === 'owner' ? 'bg-yellow-500/20' :
                    selectedManager.role === 'director' ? 'bg-yellow-400/20' :
                    selectedManager.role === 'supervisor' ? 'bg-purple-500/20' : 'bg-tg-accent/20'
                  }`}>
                    {selectedManager.role === 'owner' ? (
                      <Crown className="w-8 h-8 text-yellow-500" />
                    ) : selectedManager.role === 'director' ? (
                      <Crown className="w-8 h-8 text-yellow-400" />
                    ) : selectedManager.role === 'supervisor' ? (
                      <Shield className="w-8 h-8 text-purple-500" />
                    ) : (
                      <User className={`w-8 h-8 ${
                        selectedManager.is_blocked ? 'text-red-500' : 'text-tg-accent'
                      }`} />
                    )}
                  </div>
                  <div className="flex-1 min-w-0">
                    <h2 className="text-xl font-bold text-tg-text truncate">
                      {selectedManager.full_name || `${selectedManager.first_name} ${selectedManager.last_name}`.trim() || 'Менеджер'}
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
                  {selectedManager.full_name && (
                    <div className="flex justify-between">
                      <span className="text-tg-hint">ФИО</span>
                      <span className="text-tg-text font-medium">{selectedManager.full_name}</span>
                    </div>
                  )}
                  {selectedManager.phone && (
                    <div className="flex justify-between">
                      <span className="text-tg-hint">Телефон</span>
                      <span className="text-tg-text">{selectedManager.phone}</span>
                    </div>
                  )}
                  {selectedManager.email && (
                    <div className="flex justify-between">
                      <span className="text-tg-hint">Email</span>
                      <span className="text-tg-text">{selectedManager.email}</span>
                    </div>
                  )}
                  <div className="flex justify-between">
                    <span className="text-tg-hint">Telegram ID</span>
                    <span className="text-tg-text font-mono">{selectedManager.tg_id}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-tg-hint">Роль</span>
                    <span className="text-tg-text">{getRoleLabel(selectedManager.role as any)}</span>
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
                  {selectedManager.group_name && (
                    <div className="flex justify-between">
                      <span className="text-tg-hint">Группа</span>
                      <span className="text-tg-text font-medium text-tg-accent">
                        {selectedManager.group_name}
                      </span>
                    </div>
                  )}
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

                  {/* Make Supervisor - only for owner/director */}
                  {selectedManager.role === 'manager' ? (
                    <button
                      onClick={() => makeSupervisorMutation.mutate(selectedManager.id)}
                      disabled={makeSupervisorMutation.isPending}
                      className="p-3 rounded-xl bg-purple-500/20 text-purple-600 font-medium flex items-center justify-center gap-2 text-sm"
                    >
                      <Shield className="w-4 h-4" />
                      Сделать супервайзером
                    </button>
                  ) : (
                    <div />
                  )}
                </div>

                {/* View Detailed Info */}
                <button
                  onClick={() => {
                    setDetailedManager(selectedManager)
                    setSelectedManager(null)
                    setTimeout(() => setShowDetailedInfo(true), 150)
                  }}
                  className="w-full p-3 rounded-xl bg-indigo-500/10 text-indigo-500 font-medium flex items-center justify-center gap-2"
                >
                  <Info className="w-5 h-5" />
                  Детальная информация
                </button>

                {/* Move Group */}
                {selectedManager.role === 'manager' && (
                  <button
                    onClick={() => {
                      setManagerToMove(selectedManager)
                      setSelectedManager(null)
                      setTimeout(() => setShowMoveGroupModal(true), 150)
                    }}
                    className="w-full p-3 rounded-xl bg-blue-500/10 text-blue-500 font-medium flex items-center justify-center gap-2"
                  >
                    <Users2 className="w-5 h-5" />
                    Переместить в группу
                  </button>
                )}

                {/* Delete - always visible */}
                <button
                  onClick={() => {
                    setManagerToDelete(selectedManager) // Save manager to delete
                    setSelectedManager(null) // Close detail modal first
                    setTimeout(() => setShowDeleteConfirm(true), 150)
                  }}
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

      {/* Delete Confirmation - Separate modal */}
      <AnimatePresence>
        {showDeleteConfirm && (
          <>
            <motion.div
              className="fixed inset-0 z-[100] backdrop-blur-md"
              style={{ background: 'rgba(0, 0, 0, 0.6)' }}
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
                    <Trash2 className="w-8 h-8 text-red-500" />
                  </div>
                  <h3 className="text-xl font-bold text-tg-text mb-2">Удалить менеджера?</h3>
                  <p className="text-tg-hint text-sm leading-relaxed">
                    Это действие нельзя отменить. Все данные будут удалены.
                  </p>
                </div>
                <div className="flex gap-3">
                  <button
                    onClick={() => {
                      setShowDeleteConfirm(false)
                      setManagerToDelete(null)
                    }}
                    className="flex-1 px-4 py-3.5 rounded-xl bg-zinc-100 dark:bg-zinc-800 text-tg-text font-semibold hover:bg-zinc-200 dark:hover:bg-zinc-700 transition-colors"
                  >
                    Отмена
                  </button>
                  <button
                    onClick={() => {
                      if (managerToDelete) {
                        deleteMutation.mutate(managerToDelete.id)
                      }
                    }}
                    disabled={deleteMutation.isPending}
                    className="flex-1 px-4 py-3.5 rounded-xl bg-red-500 text-white font-semibold hover:bg-red-600 transition-colors disabled:opacity-50"
                  >
                    {deleteMutation.isPending ? 'Удаление...' : 'Удалить'}
                  </button>
                </div>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Move Group Modal */}
      <AnimatePresence>
        {showMoveGroupModal && managerToMove && (
          <>
            <motion.div
              className="fixed inset-0 z-40 backdrop-blur-sm"
              style={{ background: 'rgba(0, 0, 0, 0.4)' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => {
                setShowMoveGroupModal(false)
                setManagerToMove(null)
              }}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl z-50 safe-bottom max-h-[85vh] flex flex-col"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="flex-shrink-0 pt-3 pb-2">
                <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto" />
              </div>

              <div className="flex-1 overflow-y-auto px-4 pb-2">
                <div className="flex items-center justify-between mb-6">
                  <h2 className="text-xl font-bold text-tg-text">Переместить менеджера</h2>
                  <button
                    onClick={() => {
                      setShowMoveGroupModal(false)
                      setManagerToMove(null)
                    }}
                    className="p-2 rounded-full bg-tg-secondary-bg"
                  >
                    <X className="w-5 h-5 text-tg-hint" />
                  </button>
                </div>

                <div className="mb-4">
                  <p className="text-tg-text font-medium mb-1">
                    {managerToMove.first_name} {managerToMove.last_name}
                  </p>
                  {managerToMove.group_name && (
                    <p className="text-sm text-tg-hint">
                      Текущая группа: <span className="text-tg-accent">{managerToMove.group_name}</span>
                    </p>
                  )}
                </div>

                <div className="space-y-2">
                  <label className="text-xs text-tg-hint mb-1 block">
                    Выберите группу <span className="text-red-500">*</span>
                  </label>
                  {groups.length === 0 ? (
                    <p className="text-sm text-orange-500 py-4">
                      Нет доступных групп. Создайте группу в разделе "Группы"
                    </p>
                  ) : (
                    groups.map((group: any) => (
                      <button
                        key={group.id}
                        onClick={() => {
                          moveGroupMutation.mutate({
                            managerId: managerToMove.id,
                            groupId: group.id,
                          })
                        }}
                        disabled={moveGroupMutation.isPending || group.id === managerToMove.group_id}
                        className={`w-full p-4 rounded-xl text-left transition-colors ${
                          group.id === managerToMove.group_id
                            ? 'bg-tg-accent/20 border-2 border-tg-accent'
                            : 'bg-tg-secondary-bg hover:bg-tg-secondary-bg/80'
                        } ${moveGroupMutation.isPending ? 'opacity-50' : ''}`}
                      >
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="font-medium text-tg-text">{group.name}</p>
                            {group.description && (
                              <p className="text-sm text-tg-hint mt-1">{group.description}</p>
                            )}
                            <p className="text-xs text-tg-hint mt-1">
                              Участников: {group.member_count || 0}
                            </p>
                          </div>
                          {group.id === managerToMove.group_id ? (
                            <CheckCircle className="w-5 h-5 text-tg-accent" />
                          ) : (
                            <ArrowRight className="w-5 h-5 text-tg-hint" />
                          )}
                        </div>
                      </button>
                    ))
                  )}
                </div>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Detailed Info Modal */}
      <AnimatePresence>
        {showDetailedInfo && (detailedManagerData || detailedManager) && (
          <>
            <motion.div
              className="fixed inset-0 z-40 backdrop-blur-sm"
              style={{ background: 'rgba(0, 0, 0, 0.4)' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => {
                setShowDetailedInfo(false)
                setDetailedManager(null)
              }}
            />
            <motion.div
              className="fixed inset-x-0 bottom-0 bg-tg-bg rounded-t-3xl z-50 safe-bottom max-h-[90vh] flex flex-col"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
              style={{ top: 'auto' }}
            >
              <div className="flex-shrink-0 pt-3 pb-2">
                <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto" />
              </div>

              <div className="flex-1 overflow-y-auto px-4 pb-2 min-h-0">
                <div className="flex items-center justify-between mb-6">
                  <h2 className="text-xl font-bold text-tg-text">Отчет о менеджере</h2>
                  <button
                    onClick={() => {
                      setShowDetailedInfo(false)
                      setDetailedManager(null)
                    }}
                    className="p-2 rounded-full bg-tg-secondary-bg"
                  >
                    <X className="w-5 h-5 text-tg-hint" />
                  </button>
                </div>

                {(() => {
                  const manager = detailedManagerData || detailedManager
                  if (!manager) return null

                  return (
                    <>
                      {/* Manager Info */}
                      <div className="bg-tg-secondary-bg rounded-xl p-4 mb-4">
                        <h3 className="font-semibold text-tg-text mb-3">Данные менеджера</h3>
                        <div className="space-y-2 text-sm">
                          {manager.full_name && (
                            <div className="flex justify-between">
                              <span className="text-tg-hint">ФИО</span>
                              <span className="text-tg-text font-medium">{manager.full_name}</span>
                            </div>
                          )}
                          {manager.username && (
                            <div className="flex justify-between">
                              <span className="text-tg-hint">Username</span>
                              <a
                                href={`https://t.me/${manager.username}`}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-tg-accent"
                              >
                                @{manager.username}
                              </a>
                            </div>
                          )}
                          {manager.phone && (
                            <div className="flex justify-between">
                              <span className="text-tg-hint">Телефон</span>
                              <span className="text-tg-text">{manager.phone}</span>
                            </div>
                          )}
                          {manager.email && (
                            <div className="flex justify-between">
                              <span className="text-tg-hint">Email</span>
                              <span className="text-tg-text">{manager.email}</span>
                            </div>
                          )}
                          <div className="flex justify-between">
                            <span className="text-tg-hint">Telegram ID</span>
                            <span className="text-tg-text font-mono">{manager.tg_id}</span>
                          </div>
                          {manager.group_name && (
                            <div className="flex justify-between">
                              <span className="text-tg-hint">Группа</span>
                              <span className="text-tg-text text-tg-accent">{manager.group_name}</span>
                            </div>
                          )}
                          <div className="flex justify-between">
                            <span className="text-tg-hint">Регистрация</span>
                            <span className="text-tg-text">
                              {new Date(manager.created_at).toLocaleDateString('ru')}
                            </span>
                          </div>
                        </div>
                      </div>

                      {/* Statistics */}
                      {manager.stats && (
                        <div className="bg-tg-secondary-bg rounded-xl p-4 mb-4">
                          <h3 className="font-semibold text-tg-text mb-3">Статистика</h3>
                          <div className="grid grid-cols-2 gap-3">
                            <div>
                              <p className="text-xs text-tg-hint mb-1">Всего заявок</p>
                              <p className="text-2xl font-bold text-tg-text">
                                {manager.stats.total_requests || manager.request_count || 0}
                              </p>
                            </div>
                            <div>
                              <p className="text-xs text-tg-hint mb-1">Завершено</p>
                              <p className="text-2xl font-bold text-green-500">
                                {manager.stats.completed_requests || 0}
                              </p>
                            </div>
                            <div>
                              <p className="text-xs text-tg-hint mb-1">В работе</p>
                              <p className="text-2xl font-bold text-blue-500">
                                {manager.stats.pending_requests || 0}
                              </p>
                            </div>
                            <div>
                              <p className="text-xs text-tg-hint mb-1">За неделю</p>
                              <p className="text-2xl font-bold text-orange-500">
                                {manager.stats.this_week || 0}
                              </p>
                            </div>
                          </div>
                        </div>
                      )}

                      {/* Recent Requests */}
                      {manager.requests && manager.requests.length > 0 && (
                        <div className="bg-tg-secondary-bg rounded-xl p-4 mb-4">
                          <h3 className="font-semibold text-tg-text mb-3">Последние заявки</h3>
                          <div className="space-y-2">
                            {manager.requests.slice(0, 10).map((req: any) => (
                              <div
                                key={req.id}
                                className="flex items-center justify-between p-2 rounded-lg bg-tg-bg"
                              >
                                <div className="flex-1 min-w-0">
                                  <p className="text-sm font-medium text-tg-text truncate">
                                    {req.company_name}
                                  </p>
                                  <p className="text-xs text-tg-hint">
                                    {new Date(req.created_at).toLocaleDateString('ru')}
                                  </p>
                                </div>
                                <span className={`text-xs px-2 py-1 rounded ${
                                  req.status === 'completed' ? 'bg-green-500/20 text-green-500' :
                                  req.status === 'pending' ? 'bg-yellow-500/20 text-yellow-500' :
                                  'bg-blue-500/20 text-blue-500'
                                }`}>
                                  {req.status}
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </>
                  )
                })()}
              </div>

              <div className="flex-shrink-0 px-4 pb-4 pt-2 border-t border-tg-separator bg-tg-bg">
                <button
                  onClick={() => {
                    const manager = detailedManagerData || detailedManager
                    if (!manager) return

                    // Generate report text
                    const report = `ОТЧЕТ О МЕНЕДЖЕРЕ\n\n` +
                      `Данные менеджера:\n` +
                      `${manager.full_name ? `ФИО: ${manager.full_name}\n` : ''}` +
                      `${manager.username ? `Username: @${manager.username}\n` : ''}` +
                      `${manager.phone ? `Телефон: ${manager.phone}\n` : ''}` +
                      `${manager.email ? `Email: ${manager.email}\n` : ''}` +
                      `Telegram ID: ${manager.tg_id}\n` +
                      `${manager.group_name ? `Группа: ${manager.group_name}\n` : ''}` +
                      `Регистрация: ${new Date(manager.created_at).toLocaleDateString('ru')}\n\n` +
                      `Статистика:\n` +
                      `Всего заявок: ${manager.stats?.total_requests || manager.request_count || 0}\n` +
                      `Завершено: ${manager.stats?.completed_requests || 0}\n` +
                      `В работе: ${manager.stats?.pending_requests || 0}\n` +
                      `За неделю: ${manager.stats?.this_week || 0}\n`

                    // Copy to clipboard
                    navigator.clipboard.writeText(report)
                    toast.success('Отчет скопирован в буфер обмена')
                  }}
                  className="w-full p-3 rounded-xl bg-indigo-500/10 text-indigo-500 font-medium flex items-center justify-center gap-2"
                >
                  <Download className="w-5 h-5" />
                  Скопировать отчет
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  )
}
