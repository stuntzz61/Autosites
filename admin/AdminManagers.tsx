import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Users, UserPlus, UserCheck, UserX, Search, Ban, CheckCircle,
  Trash2, ChevronRight, X, Loader2
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { useTelegram } from '@/contexts/TelegramContext'
import toast from 'react-hot-toast'
import clsx from 'clsx'

type Tab = 'active' | 'pending' | 'blocked'

export default function AdminManagers() {
  const queryClient = useQueryClient()
  const { haptic, webApp } = useTelegram()
  const [tab, setTab] = useState<Tab>('active')
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedManager, setSelectedManager] = useState<any>(null)

  const { data: managers, isLoading: managersLoading } = useQuery({
    queryKey: ['admin-managers'],
    queryFn: () => adminApi.managers.list().then(res => res.data),
  })

  const { data: pending, isLoading: pendingLoading } = useQuery({
    queryKey: ['admin-pending'],
    queryFn: () => adminApi.pending.list().then(res => res.data),
  })

  const approveMutation = useMutation({
    mutationFn: (id: string) => adminApi.pending.approve(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-pending'] })
      queryClient.invalidateQueries({ queryKey: ['admin-managers'] })
      toast.success('Менеджер подтверждён')
      haptic?.notificationOccurred('success')
    },
    onError: () => toast.error('Ошибка'),
  })

  const rejectMutation = useMutation({
    mutationFn: (id: string) => adminApi.pending.reject(id, 'Отклонено администратором'),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-pending'] })
      toast.success('Заявка отклонена')
    },
    onError: () => toast.error('Ошибка'),
  })

  const blockMutation = useMutation({
    mutationFn: (id: string) => adminApi.managers.block(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-managers'] })
      toast.success('Менеджер заблокирован')
      setSelectedManager(null)
    },
    onError: () => toast.error('Ошибка'),
  })

  const unblockMutation = useMutation({
    mutationFn: (id: string) => adminApi.managers.unblock(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-managers'] })
      toast.success('Менеджер разблокирован')
    },
    onError: () => toast.error('Ошибка'),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => adminApi.managers.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-managers'] })
      toast.success('Менеджер удалён')
      setSelectedManager(null)
    },
    onError: () => toast.error('Ошибка'),
  })

  const activeManagers = managers?.filter((m: any) => !m.is_blocked) || []
  const blockedManagers = managers?.filter((m: any) => m.is_blocked) || []
  const pendingList = pending || []

  const currentList = tab === 'active' ? activeManagers : tab === 'blocked' ? blockedManagers : pendingList
  const isLoading = tab === 'pending' ? pendingLoading : managersLoading

  const filteredList = currentList.filter((m: any) => {
    if (!searchQuery) return true
    const search = searchQuery.toLowerCase()
    return (
      m.first_name?.toLowerCase().includes(search) ||
      m.last_name?.toLowerCase().includes(search) ||
      m.username?.toLowerCase().includes(search)
    )
  })

  return (
    <div className="min-h-screen">
      {/* Header */}
      <div className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator">
        <div className="p-4 space-y-3">
          {/* Tabs */}
          <div className="flex gap-2">
            <TabButton
              active={tab === 'active'}
              onClick={() => setTab('active')}
              icon={<Users className="w-4 h-4" />}
              label="Активные"
              count={activeManagers.length}
            />
            <TabButton
              active={tab === 'pending'}
              onClick={() => setTab('pending')}
              icon={<UserPlus className="w-4 h-4" />}
              label="Ожидают"
              count={pendingList.length}
              highlight={pendingList.length > 0}
            />
            <TabButton
              active={tab === 'blocked'}
              onClick={() => setTab('blocked')}
              icon={<Ban className="w-4 h-4" />}
              label="Заблок."
              count={blockedManagers.length}
            />
          </div>

          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
            <input
              type="text"
              placeholder="Поиск менеджера..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="input pl-10"
            />
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
        ) : filteredList.length === 0 ? (
          <motion.div
            className="text-center py-12"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
          >
            <div className="w-16 h-16 rounded-2xl bg-tg-secondary-bg mx-auto mb-4 flex items-center justify-center">
              <Users className="w-8 h-8 text-tg-hint" />
            </div>
            <p className="text-tg-text font-medium mb-1">
              {tab === 'pending' ? 'Нет ожидающих' : 'Нет менеджеров'}
            </p>
            <p className="text-sm text-tg-hint">
              {searchQuery ? 'Попробуйте другой запрос' : 'Список пуст'}
            </p>
          </motion.div>
        ) : (
          <AnimatePresence mode="popLayout">
            <div className="space-y-3">
              {filteredList.map((manager: any, index: number) => (
                <motion.div
                  key={manager.id}
                  className="bg-tg-section rounded-2xl overflow-hidden"
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, scale: 0.95 }}
                  transition={{ delay: index * 0.05 }}
                  layout
                >
                  <button
                    onClick={() => {
                      haptic?.impactOccurred('light')
                      setSelectedManager(manager)
                    }}
                    className="w-full p-4 text-left"
                  >
                    <div className="flex items-center gap-3">
                      <div className="w-12 h-12 rounded-full bg-gradient-to-br from-brand-500 to-brand-600 flex items-center justify-center text-white font-bold text-lg">
                        {manager.first_name?.[0]?.toUpperCase()}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="font-semibold text-tg-text truncate">
                          {manager.first_name} {manager.last_name}
                        </p>
                        {manager.username && (
                          <p className="text-sm text-tg-hint">@{manager.username}</p>
                        )}
                        {tab === 'active' && (
                          <p className="text-xs text-tg-hint">
                            {manager.request_count || 0} заявок
                          </p>
                        )}
                      </div>
                      {tab === 'blocked' && (
                        <span className="px-2 py-1 rounded-full bg-red-100 text-red-600 text-xs font-medium">
                          Заблокирован
                        </span>
                      )}
                      <ChevronRight className="w-5 h-5 text-tg-hint" />
                    </div>
                  </button>

                  {/* Actions for pending */}
                  {tab === 'pending' && (
                    <div className="border-t border-tg-separator px-4 py-2 flex gap-2">
                      <button
                        onClick={() => {
                          haptic?.impactOccurred('medium')
                          approveMutation.mutate(manager.id)
                        }}
                        disabled={approveMutation.isPending}
                        className="flex-1 btn btn-primary py-2"
                      >
                        <UserCheck className="w-4 h-4" />
                        Принять
                      </button>
                      <button
                        onClick={() => {
                          haptic?.impactOccurred('medium')
                          rejectMutation.mutate(manager.id)
                        }}
                        disabled={rejectMutation.isPending}
                        className="flex-1 btn btn-destructive py-2"
                      >
                        <UserX className="w-4 h-4" />
                        Отклонить
                      </button>
                    </div>
                  )}
                </motion.div>
              ))}
            </div>
          </AnimatePresence>
        )}
      </div>

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
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom max-h-[80vh] overflow-auto"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
              transition={{ type: 'spring', damping: 25 }}
            >
              <div className="flex items-center justify-between mb-4">
                <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto absolute top-2 left-1/2 -translate-x-1/2" />
                <div className="flex items-center gap-3 mt-4">
                  <div className="w-14 h-14 rounded-full bg-gradient-to-br from-brand-500 to-brand-600 flex items-center justify-center text-white font-bold text-xl">
                    {selectedManager.first_name?.[0]?.toUpperCase()}
                  </div>
                  <div>
                    <p className="text-lg font-semibold text-tg-text">
                      {selectedManager.first_name} {selectedManager.last_name}
                    </p>
                    {selectedManager.username && (
                      <p className="text-tg-hint">@{selectedManager.username}</p>
                    )}
                  </div>
                </div>
                <button onClick={() => setSelectedManager(null)} className="p-2 -mr-2">
                  <X className="w-5 h-5 text-tg-hint" />
                </button>
              </div>

              <div className="space-y-3">
                <div className="bg-tg-secondary-bg rounded-xl p-3">
                  <p className="text-xs text-tg-hint mb-1">Telegram ID</p>
                  <p className="text-tg-text font-medium">{selectedManager.tg_id}</p>
                </div>
                <div className="bg-tg-secondary-bg rounded-xl p-3">
                  <p className="text-xs text-tg-hint mb-1">Заявок создано</p>
                  <p className="text-tg-text font-medium">{selectedManager.request_count || 0}</p>
                </div>
                {selectedManager.contact && (
                  <div className="bg-tg-secondary-bg rounded-xl p-3">
                    <p className="text-xs text-tg-hint mb-1">Контакт</p>
                    <p className="text-tg-text font-medium">{selectedManager.contact}</p>
                  </div>
                )}
              </div>

              <div className="mt-6 space-y-2">
                {selectedManager.is_blocked ? (
                  <button
                    onClick={() => {
                      webApp?.showConfirm('Разблокировать менеджера?', (confirmed) => {
                        if (confirmed) unblockMutation.mutate(selectedManager.id)
                      })
                    }}
                    disabled={unblockMutation.isPending}
                    className="btn btn-primary w-full"
                  >
                    {unblockMutation.isPending ? (
                      <Loader2 className="w-5 h-5 animate-spin" />
                    ) : (
                      <>
                        <CheckCircle className="w-5 h-5" />
                        Разблокировать
                      </>
                    )}
                  </button>
                ) : (
                  <button
                    onClick={() => {
                      webApp?.showConfirm('Заблокировать менеджера?', (confirmed) => {
                        if (confirmed) blockMutation.mutate(selectedManager.id)
                      })
                    }}
                    disabled={blockMutation.isPending}
                    className="btn btn-secondary w-full"
                  >
                    {blockMutation.isPending ? (
                      <Loader2 className="w-5 h-5 animate-spin" />
                    ) : (
                      <>
                        <Ban className="w-5 h-5" />
                        Заблокировать
                      </>
                    )}
                  </button>
                )}
                <button
                  onClick={() => {
                    webApp?.showConfirm('Удалить менеджера? Это действие нельзя отменить.', (confirmed) => {
                      if (confirmed) deleteMutation.mutate(selectedManager.id)
                    })
                  }}
                  disabled={deleteMutation.isPending}
                  className="btn btn-destructive w-full"
                >
                  {deleteMutation.isPending ? (
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
          </>
        )}
      </AnimatePresence>
    </div>
  )
}

function TabButton({
  active,
  onClick,
  icon,
  label,
  count,
  highlight,
}: {
  active: boolean
  onClick: () => void
  icon: React.ReactNode
  label: string
  count: number
  highlight?: boolean
}) {
  return (
    <button
      onClick={onClick}
      className={clsx(
        'flex-1 flex items-center justify-center gap-1.5 py-2 rounded-xl text-sm font-medium transition-colors',
        active
          ? 'bg-tg-button text-tg-button-text'
          : highlight
          ? 'bg-amber-500/10 text-amber-600'
          : 'bg-tg-secondary-bg text-tg-hint'
      )}
    >
      {icon}
      {label}
      <span className={clsx(
        'px-1.5 py-0.5 rounded-full text-xs',
        active ? 'bg-white/20' : highlight ? 'bg-amber-500/20' : 'bg-tg-hint/20'
      )}>
        {count}
      </span>
    </button>
  )
}
