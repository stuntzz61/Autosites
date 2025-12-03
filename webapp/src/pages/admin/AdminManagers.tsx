import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { User, CheckCircle, XCircle, Ban, Unlock, Trash2 } from 'lucide-react'
import toast from 'react-hot-toast'

export default function AdminManagers() {
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<'all' | 'pending'>('all')

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
    },
  })

  const unblockMutation = useMutation({
    mutationFn: (id: string) => adminApi.managers.unblock(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'managers'] })
      toast.success('Менеджер разблокирован')
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
                      className="p-2 rounded-xl bg-green-500/20 text-green-500"
                    >
                      <CheckCircle className="w-5 h-5" />
                    </button>
                    <button
                      onClick={() => rejectMutation.mutate(user.id)}
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
            managers?.map((manager: any) => (
              <div key={manager.id} className="bg-tg-secondary-bg rounded-2xl p-4">
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                      manager.is_blocked ? 'bg-red-500/20' : 'bg-tg-accent/20'
                    }`}>
                      <User className={`w-5 h-5 ${
                        manager.is_blocked ? 'text-red-500' : 'text-tg-accent'
                      }`} />
                    </div>
                    <div>
                      <p className="font-medium text-tg-text">
                        {manager.first_name} {manager.last_name}
                        {manager.is_blocked && (
                          <span className="ml-2 text-xs text-red-500">(заблокирован)</span>
                        )}
                      </p>
                      {manager.username && (
                        <p className="text-sm text-tg-hint">@{manager.username}</p>
                      )}
                      <p className="text-xs text-tg-hint">
                        Заявок: {manager.total_requests || 0} | Завершено: {manager.completed_requests || 0}
                      </p>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    {manager.is_blocked ? (
                      <button
                        onClick={() => unblockMutation.mutate(manager.id)}
                        className="p-2 rounded-xl bg-green-500/20 text-green-500"
                        title="Разблокировать"
                      >
                        <Unlock className="w-5 h-5" />
                      </button>
                    ) : (
                      <button
                        onClick={() => blockMutation.mutate(manager.id)}
                        className="p-2 rounded-xl bg-orange-500/20 text-orange-500"
                        title="Заблокировать"
                      >
                        <Ban className="w-5 h-5" />
                      </button>
                    )}
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  )
}

