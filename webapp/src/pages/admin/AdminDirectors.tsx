import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { Crown, Trash2, UserPlus, AlertCircle, CheckCircle2, XCircle } from 'lucide-react'
import { getRoleLabel } from '@/stores/authStore'
import { useAuthStore } from '@/stores/authStore'

interface Director {
  id: string
  tg_id: number
  username?: string
  first_name: string
  last_name?: string
  full_name?: string
  phone?: string
  email?: string
  role: string
  created_at: string
}

export default function AdminDirectors() {
  const { user } = useAuthStore()
  const queryClient = useQueryClient()
  const [selectedDirector, setSelectedDirector] = useState<Director | null>(null)
  const [showAssignModal, setShowAssignModal] = useState(false)
  const [assignRole, setAssignRole] = useState<'director' | 'supervisor' | 'manager'>('director')
  const [targetUserId, setTargetUserId] = useState<string>('')

  // Fetch directors
  const { data: directors = [], isLoading } = useQuery({
    queryKey: ['directors'],
    queryFn: async () => {
      const response = await adminApi.directors.list()
      return response.data
    },
  })

  // Assign role mutation
  const assignRoleMutation = useMutation({
    mutationFn: async ({ userId, role }: { userId: string; role: 'manager' | 'supervisor' | 'director' }) => {
      return adminApi.roles.assign(userId, role)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['directors'] })
      queryClient.invalidateQueries({ queryKey: ['managers'] })
      queryClient.invalidateQueries({ queryKey: ['supervisors'] })
      setShowAssignModal(false)
      setTargetUserId('')
    },
  })

  // Delete director mutation
  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      return adminApi.directors.delete(id)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['directors'] })
      setSelectedDirector(null)
    },
  })

  if (isLoading) {
    return (
      <div className="p-4">
        <div className="text-center text-tg-hint">Загрузка...</div>
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold">Директоры</h2>
          <p className="text-sm text-tg-hint mt-1">
            Управление директорами системы. Только владелец может назначать и удалять директоров.
          </p>
        </div>
        <button
          onClick={() => setShowAssignModal(true)}
          className="px-4 py-2 bg-tg-button text-tg-button-text rounded-xl font-medium flex items-center gap-2"
        >
          <UserPlus className="w-4 h-4" />
          Назначить
        </button>
      </div>

      {/* Directors List */}
      {directors.length === 0 ? (
        <div className="text-center py-12 text-tg-hint">
          <Crown className="w-12 h-12 mx-auto mb-4 opacity-50" />
          <p>Директоры не назначены</p>
        </div>
      ) : (
        <div className="space-y-2">
          {directors.map((director) => (
            <div
              key={director.id}
              onClick={() => setSelectedDirector(director)}
              className="p-4 bg-tg-bg rounded-xl border border-tg-separator cursor-pointer hover:bg-tg-secondary-bg transition-colors"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-full bg-gradient-to-br from-yellow-400 to-orange-500 flex items-center justify-center">
                    <Crown className="w-5 h-5 text-white" />
                  </div>
                  <div>
                    <div className="font-semibold">
                      {director.full_name || `${director.first_name} ${director.last_name || ''}`.trim()}
                    </div>
                    <div className="text-sm text-tg-hint">
                      @{director.username || 'без username'}
                    </div>
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-xs text-tg-hint">Директор</div>
                  <div className="text-xs text-tg-hint">
                    {new Date(director.created_at).toLocaleDateString('ru-RU')}
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Director Detail Modal */}
      {selectedDirector && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-tg-bg rounded-2xl max-w-md w-full max-h-[90vh] overflow-y-auto">
            <div className="p-6 space-y-4">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold">Информация о директоре</h3>
                <button
                  onClick={() => setSelectedDirector(null)}
                  className="p-2 hover:bg-tg-secondary-bg rounded-xl"
                >
                  <XCircle className="w-5 h-5" />
                </button>
              </div>

              <div className="space-y-3">
                <div>
                  <div className="text-xs text-tg-hint mb-1">ФИО</div>
                  <div className="font-medium">
                    {selectedDirector.full_name ||
                     `${selectedDirector.first_name} ${selectedDirector.last_name || ''}`.trim()}
                  </div>
                </div>

                <div>
                  <div className="text-xs text-tg-hint mb-1">Username</div>
                  <div>@{selectedDirector.username || 'не указан'}</div>
                </div>

                {selectedDirector.phone && (
                  <div>
                    <div className="text-xs text-tg-hint mb-1">Телефон</div>
                    <div>{selectedDirector.phone}</div>
                  </div>
                )}

                {selectedDirector.email && (
                  <div>
                    <div className="text-xs text-tg-hint mb-1">Email</div>
                    <div>{selectedDirector.email}</div>
                  </div>
                )}

                <div>
                  <div className="text-xs text-tg-hint mb-1">Роль</div>
                  <div className="inline-flex items-center gap-2 px-3 py-1 bg-yellow-500/20 text-yellow-400 rounded-lg">
                    <Crown className="w-4 h-4" />
                    {getRoleLabel(selectedDirector.role as any)}
                  </div>
                </div>
              </div>

              <div className="pt-4 border-t border-tg-separator">
                <button
                  onClick={() => {
                    if (confirm('Вы уверены, что хотите удалить этого директора?')) {
                      deleteMutation.mutate(selectedDirector.id)
                    }
                  }}
                  className="w-full px-4 py-2 bg-red-500/20 text-red-400 rounded-xl font-medium flex items-center justify-center gap-2"
                >
                  <Trash2 className="w-4 h-4" />
                  Удалить директора
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Assign Role Modal */}
      {showAssignModal && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-tg-bg rounded-2xl max-w-md w-full">
            <div className="p-6 space-y-4">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold">Назначить директора</h3>
                <button
                  onClick={() => setShowAssignModal(false)}
                  className="p-2 hover:bg-tg-secondary-bg rounded-xl"
                >
                  <XCircle className="w-5 h-5" />
                </button>
              </div>

              <div className="space-y-3">
                <div>
                  <label className="text-sm font-medium mb-2 block">ID пользователя</label>
                  <input
                    type="text"
                    value={targetUserId}
                    onChange={(e) => setTargetUserId(e.target.value)}
                    placeholder="Введите ID пользователя"
                    className="w-full px-4 py-2 bg-tg-secondary-bg rounded-xl border border-tg-separator"
                  />
                </div>

                <div>
                  <label className="text-sm font-medium mb-2 block">Роль</label>
                  <select
                    value={assignRole}
                    onChange={(e) => setAssignRole(e.target.value as any)}
                    className="w-full px-4 py-2 bg-tg-secondary-bg rounded-xl border border-tg-separator"
                  >
                    <option value="director">Директор</option>
                    <option value="supervisor">Супервайзер</option>
                    <option value="manager">Менеджер</option>
                  </select>
                </div>
              </div>

              <div className="pt-4 border-t border-tg-separator flex gap-2">
                <button
                  onClick={() => setShowAssignModal(false)}
                  className="flex-1 px-4 py-2 bg-tg-secondary-bg rounded-xl font-medium"
                >
                  Отмена
                </button>
                <button
                  onClick={() => {
                    if (targetUserId) {
                      assignRoleMutation.mutate({ userId: targetUserId, role: assignRole })
                    }
                  }}
                  disabled={!targetUserId || assignRoleMutation.isPending}
                  className="flex-1 px-4 py-2 bg-tg-button text-tg-button-text rounded-xl font-medium disabled:opacity-50"
                >
                  {assignRoleMutation.isPending ? 'Назначение...' : 'Назначить'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

