import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import {
  Users, Plus, Edit2, Trash2, X, Save, Users2, Loader2
} from 'lucide-react'
import { motion, AnimatePresence } from 'framer-motion'
import toast from 'react-hot-toast'

interface Group {
  id: string
  name: string
  description?: string
  is_active: boolean
  created_at: string
  member_count?: number
  members?: Array<{
    id: string
    first_name: string
    last_name?: string
    username?: string
  }>
}

export default function AdminGroups() {
  const queryClient = useQueryClient()
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [selectedGroup, setSelectedGroup] = useState<Group | null>(null)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [groupToDelete, setGroupToDelete] = useState<Group | null>(null)
  const [editingGroup, setEditingGroup] = useState<Group | null>(null)

  const [formData, setFormData] = useState({
    name: '',
    description: '',
  })

  const { data: groups = [], isLoading } = useQuery({
    queryKey: ['admin', 'groups'],
    queryFn: () => adminApi.groups.list().then(res => res.data),
  })

  const createMutation = useMutation({
    mutationFn: (data: { name: string; description?: string }) =>
      adminApi.groups.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'groups'] })
      toast.success('Группа создана!')
      setShowCreateModal(false)
      setFormData({ name: '', description: '' })
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка создания группы')
    },
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: { name?: string; description?: string; is_active?: boolean } }) =>
      adminApi.groups.update(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'groups'] })
      toast.success('Группа обновлена!')
      setEditingGroup(null)
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка обновления группы')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => adminApi.groups.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'groups'] })
      toast.success('Группа удалена')
      setShowDeleteConfirm(false)
      setGroupToDelete(null)
      setSelectedGroup(null)
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Ошибка удаления группы')
      setShowDeleteConfirm(false)
    },
  })

  const handleCreate = () => {
    if (!formData.name.trim()) {
      toast.error('Введите название группы')
      return
    }

    createMutation.mutate({
      name: formData.name.trim(),
      description: formData.description.trim() || undefined,
    })
  }

  const handleEdit = (group: Group) => {
    setEditingGroup(group)
    setFormData({
      name: group.name,
      description: group.description || '',
    })
  }

  const handleUpdate = () => {
    if (!editingGroup || !formData.name.trim()) {
      toast.error('Введите название группы')
      return
    }

    updateMutation.mutate({
      id: editingGroup.id,
      data: {
        name: formData.name.trim(),
        description: formData.description.trim() || undefined,
      },
    })
  }

  const handleDelete = (group: Group) => {
    setGroupToDelete(group)
    setSelectedGroup(null)
    setTimeout(() => setShowDeleteConfirm(true), 150)
  }

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(3)].map((_, i) => (
          <div key={i} className="skeleton h-20 rounded-2xl" />
        ))}
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-tg-text">Группы</h1>
        <button
          onClick={() => setShowCreateModal(true)}
          className="btn btn-primary flex items-center gap-2"
        >
          <Plus className="w-4 h-4" />
          Создать
        </button>
      </div>

      {groups.length === 0 ? (
        <div className="text-center py-12">
          <Users2 className="w-16 h-16 text-tg-hint mx-auto mb-4 opacity-50" />
          <p className="text-tg-hint mb-4">Нет групп</p>
          <button
            onClick={() => setShowCreateModal(true)}
            className="btn btn-primary"
          >
            Создать первую группу
          </button>
        </div>
      ) : (
        <div className="space-y-3">
          {groups.map((group: Group) => (
            <div
              key={group.id}
              className="bg-tg-secondary-bg rounded-2xl p-4"
            >
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <div className="flex items-center gap-3 mb-2">
                    <div className="w-10 h-10 rounded-xl bg-tg-accent/20 flex items-center justify-center">
                      <Users className="w-5 h-5 text-tg-accent" />
                    </div>
                    <div>
                      <h3 className="font-semibold text-tg-text">{group.name}</h3>
                      {group.description && (
                        <p className="text-sm text-tg-hint mt-1">{group.description}</p>
                      )}
                    </div>
                  </div>
                  <div className="flex items-center gap-4 text-sm text-tg-hint ml-13">
                    <span>Участников: {group.member_count || 0}</span>
                    {!group.is_active && (
                      <span className="text-orange-500">(неактивна)</span>
                    )}
                  </div>
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => handleEdit(group)}
                    className="p-2 rounded-xl bg-blue-500/20 text-blue-500"
                  >
                    <Edit2 className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => handleDelete(group)}
                    className="p-2 rounded-xl bg-red-500/20 text-red-500"
                    disabled={group.member_count && group.member_count > 0}
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Create Modal */}
      <AnimatePresence>
        {showCreateModal && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowCreateModal(false)}
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
                  <h2 className="text-xl font-bold text-tg-text">Новая группа</h2>
                  <button
                    onClick={() => setShowCreateModal(false)}
                    className="p-2 rounded-full bg-tg-secondary-bg"
                  >
                    <X className="w-5 h-5 text-tg-hint" />
                  </button>
                </div>

                <div className="space-y-4">
                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">
                      Название <span className="text-red-500">*</span>
                    </label>
                    <input
                      type="text"
                      value={formData.name}
                      onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                      placeholder="Например: Команда продаж"
                      className="input"
                      autoFocus
                    />
                  </div>

                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">Описание (опционально)</label>
                    <textarea
                      value={formData.description}
                      onChange={(e) => setFormData(prev => ({ ...prev, description: e.target.value }))}
                      placeholder="Описание группы..."
                      className="input min-h-[100px] resize-none"
                    />
                  </div>
                </div>
              </div>

              <div className="flex-shrink-0 px-4 pb-4 pt-2 border-t border-tg-separator bg-tg-bg flex gap-2">
                <button
                  onClick={() => setShowCreateModal(false)}
                  className="btn flex-1"
                >
                  Отмена
                </button>
                <button
                  onClick={handleCreate}
                  disabled={createMutation.isPending}
                  className="btn btn-primary flex-1"
                >
                  {createMutation.isPending ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    'Создать'
                  )}
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Edit Modal */}
      <AnimatePresence>
        {editingGroup && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setEditingGroup(null)}
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
                  <h2 className="text-xl font-bold text-tg-text">Редактировать группу</h2>
                  <button
                    onClick={() => setEditingGroup(null)}
                    className="p-2 rounded-full bg-tg-secondary-bg"
                  >
                    <X className="w-5 h-5 text-tg-hint" />
                  </button>
                </div>

                <div className="space-y-4">
                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">
                      Название <span className="text-red-500">*</span>
                    </label>
                    <input
                      type="text"
                      value={formData.name}
                      onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                      placeholder="Название группы"
                      className="input"
                      autoFocus
                    />
                  </div>

                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">Описание (опционально)</label>
                    <textarea
                      value={formData.description}
                      onChange={(e) => setFormData(prev => ({ ...prev, description: e.target.value }))}
                      placeholder="Описание группы..."
                      className="input min-h-[100px] resize-none"
                    />
                  </div>
                </div>
              </div>

              <div className="flex-shrink-0 px-4 pb-4 pt-2 border-t border-tg-separator bg-tg-bg flex gap-2">
                <button
                  onClick={() => setEditingGroup(null)}
                  className="btn flex-1"
                >
                  Отмена
                </button>
                <button
                  onClick={handleUpdate}
                  disabled={updateMutation.isPending}
                  className="btn btn-primary flex-1"
                >
                  {updateMutation.isPending ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <>
                      <Save className="w-4 h-4" />
                      Сохранить
                    </>
                  )}
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Delete Confirmation */}
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
                    <Trash2 className="w-8 h-8 text-red-500" />
                  </div>
                  <h3 className="text-xl font-bold text-tg-text mb-2">Удалить группу?</h3>
                  <p className="text-tg-hint text-sm leading-relaxed">
                    {groupToDelete?.member_count && groupToDelete.member_count > 0
                      ? `Нельзя удалить группу с ${groupToDelete.member_count} участниками. Сначала переместите или удалите участников.`
                      : 'Это действие нельзя отменить. Группа будет удалена.'}
                  </p>
                </div>
                <div className="flex gap-3">
                  <button
                    onClick={() => {
                      setShowDeleteConfirm(false)
                      setGroupToDelete(null)
                    }}
                    className="flex-1 px-4 py-3.5 rounded-xl bg-zinc-100 dark:bg-zinc-800 text-tg-text font-semibold hover:bg-zinc-200 dark:hover:bg-zinc-700 transition-colors"
                  >
                    Отмена
                  </button>
                  {(!groupToDelete?.member_count || groupToDelete.member_count === 0) && (
                    <button
                      onClick={() => {
                        if (groupToDelete) {
                          deleteMutation.mutate(groupToDelete.id)
                        }
                      }}
                      disabled={deleteMutation.isPending}
                      className="flex-1 px-4 py-3.5 rounded-xl bg-red-500 text-white font-semibold hover:bg-red-600 transition-colors disabled:opacity-50"
                    >
                      {deleteMutation.isPending ? 'Удаление...' : 'Удалить'}
                    </button>
                  )}
                </div>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  )
}

