import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import { Crown, Trash2, UserPlus, XCircle, Search, User, Shield, ChevronRight, Loader2 } from 'lucide-react'
import { getRoleLabel } from '@/stores/authStore'
import { motion, AnimatePresence } from 'framer-motion'
import toast from 'react-hot-toast'

interface UserInfo {
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
  request_count?: number
}

export default function AdminDirectors() {
  const queryClient = useQueryClient()
  const [selectedDirector, setSelectedDirector] = useState<UserInfo | null>(null)
  const [showAssignModal, setShowAssignModal] = useState(false)
  const [assignRole, setAssignRole] = useState<'director' | 'supervisor' | 'manager'>('director')
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedUser, setSelectedUser] = useState<UserInfo | null>(null)

  // Fetch directors
  const { data: directors = [], isLoading: isLoadingDirectors } = useQuery({
    queryKey: ['admin', 'directors'],
    queryFn: async () => {
      const response = await adminApi.directors.list()
      return response.data
    },
  })

  // Fetch supervisors (for role management)
  const { data: supervisors = [], isLoading: isLoadingSupervisors } = useQuery({
    queryKey: ['admin', 'supervisors'],
    queryFn: async () => {
      const response = await adminApi.supervisors.list()
      return response.data
    },
  })

  // Fetch all managers (for promoting to director/supervisor)
  const { data: managers = [], isLoading: isLoadingManagers } = useQuery({
    queryKey: ['admin', 'managers'],
    queryFn: async () => {
      const response = await adminApi.managers.list()
      return response.data
    },
    enabled: showAssignModal, // Only fetch when modal is open
  })

  // Combined list for selection (managers + supervisors that can be promoted)
  const availableUsers = [
    ...supervisors.filter((s: UserInfo) => s.role === 'supervisor'),
    ...managers.filter((m: UserInfo) => m.role === 'manager'),
  ]

  // Filter users by search query
  const filteredUsers = availableUsers.filter((u: UserInfo) => {
    const searchLower = searchQuery.toLowerCase()
    const fullName = `${u.first_name} ${u.last_name || ''}`.toLowerCase()
    const username = (u.username || '').toLowerCase()
    return fullName.includes(searchLower) || username.includes(searchLower)
  })

  // Assign role mutation
  const assignRoleMutation = useMutation({
    mutationFn: async ({ userId, role }: { userId: string; role: 'manager' | 'supervisor' | 'director' }) => {
      return adminApi.roles.assign(userId, role)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'directors'] })
      queryClient.invalidateQueries({ queryKey: ['admin', 'managers'] })
      queryClient.invalidateQueries({ queryKey: ['admin', 'supervisors'] })
      setShowAssignModal(false)
      setSelectedUser(null)
      setSearchQuery('')
      toast.success('Роль успешно назначена')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка назначения роли')
    },
  })

  // Delete director mutation
  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      return adminApi.directors.delete(id)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'directors'] })
      setSelectedDirector(null)
      toast.success('Директор удалён')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка удаления')
    },
  })

  const getRoleIcon = (role: string) => {
    switch (role) {
      case 'director':
        return <Crown className="w-5 h-5 text-blue-400" />
      case 'supervisor':
        return <Shield className="w-5 h-5 text-purple-500" />
      default:
        return <User className="w-5 h-5 text-tg-accent" />
    }
  }

  const handleAssignRole = () => {
    if (!selectedUser) {
      toast.error('Выберите пользователя')
      return
    }
    assignRoleMutation.mutate({ userId: selectedUser.id, role: assignRole })
  }

  if (isLoadingDirectors) {
    return (
      <div className="p-4 flex items-center justify-center">
        <Loader2 className="w-6 h-6 animate-spin text-tg-accent" />
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-tg-text">Директоры</h2>
          <p className="text-sm text-tg-hint mt-1">
            Управление директорами. Только владелец может назначать и удалять директоров.
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
        <div className="text-center py-12">
          <Crown className="w-16 h-16 text-tg-hint mx-auto mb-4 opacity-50" />
          <p className="text-tg-hint mb-4">Директоры не назначены</p>
          <button
            onClick={() => setShowAssignModal(true)}
            className="btn btn-primary"
          >
            Назначить первого директора
          </button>
        </div>
      ) : (
        <div className="space-y-2">
          {directors.map((director: UserInfo) => (
            <button
              key={director.id}
              onClick={() => setSelectedDirector(director)}
              className="w-full p-4 bg-tg-secondary-bg rounded-xl text-left hover:bg-tg-secondary-bg/80 transition-colors"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-full bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center">
                    <Crown className="w-5 h-5 text-white" />
                  </div>
                  <div>
                    <div className="font-semibold text-tg-text">
                      {director.full_name || `${director.first_name} ${director.last_name || ''}`.trim()}
                    </div>
                    <div className="text-sm text-tg-hint">
                      @{director.username || 'без username'}
                    </div>
                  </div>
                </div>
                <ChevronRight className="w-5 h-5 text-tg-hint" />
              </div>
            </button>
          ))}
        </div>
      )}

      {/* Supervisors Section */}
      <div className="pt-4">
        <h3 className="text-lg font-semibold text-tg-text mb-3">Супервайзеры</h3>
        {isLoadingSupervisors ? (
          <div className="flex items-center justify-center py-4">
            <Loader2 className="w-5 h-5 animate-spin text-tg-accent" />
          </div>
        ) : supervisors.length === 0 ? (
          <div className="text-center py-6 bg-tg-secondary-bg rounded-xl">
            <Shield className="w-10 h-10 text-tg-hint mx-auto mb-2 opacity-50" />
            <p className="text-tg-hint text-sm">Супервайзеры не назначены</p>
          </div>
        ) : (
          <div className="space-y-2">
            {supervisors.map((supervisor: UserInfo) => (
              <div
                key={supervisor.id}
                className="p-4 bg-tg-secondary-bg rounded-xl"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-full bg-purple-500/20 flex items-center justify-center">
                      <Shield className="w-5 h-5 text-purple-500" />
                    </div>
                    <div>
                      <div className="font-semibold text-tg-text">
                        {supervisor.full_name || `${supervisor.first_name} ${supervisor.last_name || ''}`.trim()}
                      </div>
                      <div className="text-sm text-tg-hint">
                        @{supervisor.username || 'без username'}
                      </div>
                    </div>
                  </div>
                  <span className="text-xs px-2 py-1 bg-purple-500/20 text-purple-500 rounded">
                    Супервайзер
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Director Detail Modal */}
      <AnimatePresence>
        {selectedDirector && (
          <>
            <motion.div
              className="fixed inset-0 z-40 backdrop-blur-sm"
              style={{ background: 'rgba(0, 0, 0, 0.4)' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setSelectedDirector(null)}
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
                  <h2 className="text-xl font-bold text-tg-text">Информация о директоре</h2>
                  <button
                    onClick={() => setSelectedDirector(null)}
                    className="p-2 rounded-full bg-tg-secondary-bg"
                  >
                    <XCircle className="w-5 h-5 text-tg-hint" />
                  </button>
                </div>

                {/* Director Info */}
                <div className="flex items-center gap-4 mb-6">
                  <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center">
                    <Crown className="w-8 h-8 text-white" />
                  </div>
                  <div>
                    <h3 className="text-lg font-bold text-tg-text">
                      {selectedDirector.full_name ||
                       `${selectedDirector.first_name} ${selectedDirector.last_name || ''}`.trim()}
                    </h3>
                    {selectedDirector.username && (
                      <a
                        href={`https://t.me/${selectedDirector.username}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-tg-accent text-sm"
                      >
                        @{selectedDirector.username}
                      </a>
                    )}
                  </div>
                </div>

                {/* Details */}
                <div className="bg-tg-secondary-bg rounded-xl p-4 space-y-3">
                  {selectedDirector.phone && (
                    <div className="flex justify-between">
                      <span className="text-tg-hint">Телефон</span>
                      <span className="text-tg-text">{selectedDirector.phone}</span>
                    </div>
                  )}
                  {selectedDirector.email && (
                    <div className="flex justify-between">
                      <span className="text-tg-hint">Email</span>
                      <span className="text-tg-text">{selectedDirector.email}</span>
                    </div>
                  )}
                  <div className="flex justify-between">
                    <span className="text-tg-hint">Telegram ID</span>
                    <span className="text-tg-text font-mono">{selectedDirector.tg_id}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-tg-hint">Роль</span>
                    <span className="inline-flex items-center gap-2 px-3 py-1 bg-blue-500/20 text-blue-400 rounded-lg text-sm">
                      <Crown className="w-3.5 h-3.5" />
                      {getRoleLabel(selectedDirector.role as any)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-tg-hint">Дата назначения</span>
                    <span className="text-tg-text">
                      {new Date(selectedDirector.created_at).toLocaleDateString('ru-RU')}
                    </span>
                  </div>
                </div>
              </div>

              {/* Actions */}
              <div className="flex-shrink-0 px-4 pb-4 pt-2 border-t border-tg-separator bg-tg-bg">
                <button
                  onClick={() => {
                    if (confirm('Вы уверены, что хотите удалить этого директора? Он станет обычным менеджером.')) {
                      deleteMutation.mutate(selectedDirector.id)
                    }
                  }}
                  disabled={deleteMutation.isPending}
                  className="w-full p-3 rounded-xl bg-red-500/10 text-red-500 font-medium flex items-center justify-center gap-2"
                >
                  <Trash2 className="w-5 h-5" />
                  {deleteMutation.isPending ? 'Удаление...' : 'Удалить директора'}
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Assign Role Modal */}
      <AnimatePresence>
        {showAssignModal && (
          <>
            <motion.div
              className="fixed inset-0 z-40 backdrop-blur-sm"
              style={{ background: 'rgba(0, 0, 0, 0.4)' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => {
                setShowAssignModal(false)
                setSelectedUser(null)
                setSearchQuery('')
              }}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl z-50 safe-bottom max-h-[90vh] flex flex-col"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="flex-shrink-0 pt-3 pb-2">
                <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto" />
              </div>

              <div className="flex-1 overflow-y-auto px-4 pb-2">
                <div className="flex items-center justify-between mb-6">
                  <h2 className="text-xl font-bold text-tg-text">Назначить роль</h2>
                  <button
                    onClick={() => {
                      setShowAssignModal(false)
                      setSelectedUser(null)
                      setSearchQuery('')
                    }}
                    className="p-2 rounded-full bg-tg-secondary-bg"
                  >
                    <XCircle className="w-5 h-5 text-tg-hint" />
                  </button>
                </div>

                {/* Role Selection */}
                <div className="mb-4">
                  <label className="text-sm font-medium text-tg-hint mb-2 block">Выберите роль</label>
                  <div className="flex gap-2">
                    <button
                      onClick={() => setAssignRole('director')}
                      className={`flex-1 p-3 rounded-xl flex items-center justify-center gap-2 font-medium transition-colors ${
                        assignRole === 'director'
                          ? 'bg-blue-500/20 text-blue-500 border-2 border-blue-500'
                          : 'bg-tg-secondary-bg text-tg-hint'
                      }`}
                    >
                      <Crown className="w-4 h-4" />
                      Директор
                    </button>
                    <button
                      onClick={() => setAssignRole('supervisor')}
                      className={`flex-1 p-3 rounded-xl flex items-center justify-center gap-2 font-medium transition-colors ${
                        assignRole === 'supervisor'
                          ? 'bg-purple-500/20 text-purple-500 border-2 border-purple-500'
                          : 'bg-tg-secondary-bg text-tg-hint'
                      }`}
                    >
                      <Shield className="w-4 h-4" />
                      Супервайзер
                    </button>
                  </div>
                </div>

                {/* Search */}
                <div className="mb-4">
                  <label className="text-sm font-medium text-tg-hint mb-2 block">Выберите пользователя</label>
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-tg-hint" />
                    <input
                      type="text"
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      placeholder="Поиск по имени или username..."
                      className="w-full pl-10 pr-4 py-3 bg-tg-secondary-bg rounded-xl border border-tg-separator focus:border-tg-accent outline-none"
                    />
                  </div>
                </div>

                {/* Selected User */}
                {selectedUser && (
                  <div className="mb-4 p-3 bg-tg-accent/10 border-2 border-tg-accent rounded-xl">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                          selectedUser.role === 'supervisor' ? 'bg-purple-500/20' : 'bg-tg-accent/20'
                        }`}>
                          {getRoleIcon(selectedUser.role)}
                        </div>
                        <div>
                          <p className="font-medium text-tg-text">
                            {selectedUser.full_name || `${selectedUser.first_name} ${selectedUser.last_name || ''}`.trim()}
                          </p>
                          {selectedUser.username && (
                            <p className="text-sm text-tg-hint">@{selectedUser.username}</p>
                          )}
                        </div>
                      </div>
                      <button
                        onClick={() => setSelectedUser(null)}
                        className="p-1 text-tg-hint hover:text-tg-text"
                      >
                        <XCircle className="w-5 h-5" />
                      </button>
                    </div>
                  </div>
                )}

                {/* Users List */}
                <div className="space-y-2 max-h-[40vh] overflow-y-auto">
                  {isLoadingManagers || isLoadingSupervisors ? (
                    <div className="flex items-center justify-center py-8">
                      <Loader2 className="w-6 h-6 animate-spin text-tg-accent" />
                    </div>
                  ) : filteredUsers.length === 0 ? (
                    <div className="text-center py-8">
                      <User className="w-10 h-10 text-tg-hint mx-auto mb-2 opacity-50" />
                      <p className="text-tg-hint text-sm">
                        {searchQuery ? 'Пользователи не найдены' : 'Нет доступных пользователей'}
                      </p>
                    </div>
                  ) : (
                    filteredUsers.map((u: UserInfo) => (
                      <button
                        key={u.id}
                        onClick={() => setSelectedUser(u)}
                        className={`w-full p-3 rounded-xl text-left transition-colors ${
                          selectedUser?.id === u.id
                            ? 'bg-tg-accent/20 border-2 border-tg-accent'
                            : 'bg-tg-secondary-bg hover:bg-tg-secondary-bg/80'
                        }`}
                      >
                        <div className="flex items-center gap-3">
                          <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                            u.role === 'supervisor' ? 'bg-purple-500/20' : 'bg-tg-accent/20'
                          }`}>
                            {getRoleIcon(u.role)}
                          </div>
                          <div className="flex-1 min-w-0">
                            <p className="font-medium text-tg-text truncate">
                              {u.full_name || `${u.first_name} ${u.last_name || ''}`.trim()}
                            </p>
                            <div className="flex items-center gap-2 text-sm">
                              {u.username && (
                                <span className="text-tg-hint">@{u.username}</span>
                              )}
                              <span className={`text-xs px-2 py-0.5 rounded ${
                                u.role === 'supervisor' ? 'bg-purple-500/20 text-purple-500' : 'bg-tg-accent/20 text-tg-accent'
                              }`}>
                                {getRoleLabel(u.role as any)}
                              </span>
                            </div>
                          </div>
                        </div>
                      </button>
                    ))
                  )}
                </div>
              </div>

              {/* Actions */}
              <div className="flex-shrink-0 px-4 pb-4 pt-2 border-t border-tg-separator bg-tg-bg">
                <div className="flex gap-2">
                  <button
                    onClick={() => {
                      setShowAssignModal(false)
                      setSelectedUser(null)
                      setSearchQuery('')
                    }}
                    className="flex-1 p-3 rounded-xl bg-tg-secondary-bg text-tg-text font-medium"
                  >
                    Отмена
                  </button>
                  <button
                    onClick={handleAssignRole}
                    disabled={!selectedUser || assignRoleMutation.isPending}
                    className="flex-1 p-3 rounded-xl bg-tg-button text-tg-button-text font-medium disabled:opacity-50"
                  >
                    {assignRoleMutation.isPending ? (
                      <Loader2 className="w-5 h-5 animate-spin mx-auto" />
                    ) : (
                      `Назначить ${assignRole === 'director' ? 'директором' : 'супервайзером'}`
                    )}
                  </button>
                </div>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  )
}
