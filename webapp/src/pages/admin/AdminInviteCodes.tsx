import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { adminApi } from '@/api/client'
import {
  Link2, Plus, Copy, Trash2, Users, Clock, CheckCircle2,
  XCircle, ChevronRight, X, QrCode, Share2, Eye, EyeOff,
  Sparkles, Shield, Calendar, Hash, ToggleLeft, ToggleRight
} from 'lucide-react'
import { motion, AnimatePresence } from 'framer-motion'
import toast from 'react-hot-toast'
import clsx from 'clsx'

interface InviteCode {
  id: string
  code: string
  name?: string
  group_id?: string
  group_name?: string
  max_uses?: number
  uses_count: number
  expires_at?: string
  auto_approve: boolean
  is_active: boolean
  notes?: string
  created_at: string
  creator_first_name?: string
  creator_last_name?: string
}

export default function AdminInviteCodes() {
  const queryClient = useQueryClient()
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [selectedCode, setSelectedCode] = useState<InviteCode | null>(null)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [codeToDelete, setCodeToDelete] = useState<InviteCode | null>(null)

  // Form state
  const [formData, setFormData] = useState({
    name: '',
    group_id: '',
    max_uses: '',
    expires_in_days: '',
    auto_approve: false,
    notes: '',
  })

  // Queries
  const { data: inviteCodes = [], isLoading } = useQuery({
    queryKey: ['admin', 'invite-codes'],
    queryFn: () => adminApi.inviteCodes.list().then(res => res.data),
  })

  const { data: groups = [] } = useQuery({
    queryKey: ['admin', 'groups'],
    queryFn: () => adminApi.groups.list().then(res => res.data),
  })

  // Mutations
  const createMutation = useMutation({
    mutationFn: (data: any) => adminApi.inviteCodes.create(data),
    onSuccess: (res) => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'invite-codes'] })
      toast.success('Инвайт-код создан!')
      setShowCreateModal(false)
      setFormData({
        name: '',
        group_id: '',
        max_uses: '',
        expires_in_days: '',
        auto_approve: false,
        notes: '',
      })
      // Show the new code
      setSelectedCode(res.data)
    },
    onError: () => {
      toast.error('Ошибка создания')
    },
  })

  const toggleActiveMutation = useMutation({
    mutationFn: ({ id, is_active }: { id: string; is_active: boolean }) =>
      adminApi.inviteCodes.update(id, { is_active }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'invite-codes'] })
      toast.success('Статус обновлён')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => adminApi.inviteCodes.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'invite-codes'] })
      toast.success('Инвайт-код удалён')
      setShowDeleteConfirm(false)
      setCodeToDelete(null)
      setSelectedCode(null)
    },
  })

  const handleCreate = () => {
    const data: any = {}
    if (formData.name) data.name = formData.name
    if (formData.group_id) data.group_id = formData.group_id
    if (formData.max_uses) data.max_uses = parseInt(formData.max_uses)
    if (formData.expires_in_days) data.expires_in_days = parseInt(formData.expires_in_days)
    data.auto_approve = formData.auto_approve
    if (formData.notes) data.notes = formData.notes

    createMutation.mutate(data)
  }

  const copyCode = (code: string) => {
    navigator.clipboard.writeText(code)
    toast.success('Код скопирован!')
  }

  const copyLink = (code: string) => {
    const botUsername = 'weblyMN_bot' // Replace with actual bot username
    const link = `https://t.me/${botUsername}?start=invite_${code}`
    navigator.clipboard.writeText(link)
    toast.success('Ссылка скопирована!')
  }

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('ru-RU', {
      day: 'numeric',
      month: 'short',
      year: 'numeric',
    })
  }

  const isExpired = (dateStr?: string) => {
    if (!dateStr) return false
    return new Date(dateStr) < new Date()
  }

  const isExhausted = (code: InviteCode) => {
    if (code.max_uses === null || code.max_uses === undefined) return false
    return code.uses_count >= code.max_uses
  }

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(3)].map((_, i) => (
          <div key={i} className="skeleton h-24 rounded-2xl" />
        ))}
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4 pb-24">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-tg-text">Инвайт-коды</h1>
          <p className="text-sm text-tg-hint">Регистрация менеджеров по ссылке</p>
        </div>
        <button
          onClick={() => setShowCreateModal(true)}
          className="p-3 bg-tg-button text-tg-button-text rounded-xl"
        >
          <Plus className="w-5 h-5" />
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-tg-secondary-bg rounded-xl p-3 text-center">
          <p className="text-2xl font-bold text-tg-text">{inviteCodes.length}</p>
          <p className="text-xs text-tg-hint">Всего</p>
        </div>
        <div className="bg-tg-secondary-bg rounded-xl p-3 text-center">
          <p className="text-2xl font-bold text-green-500">
            {inviteCodes.filter((c: InviteCode) => c.is_active && !isExpired(c.expires_at) && !isExhausted(c)).length}
          </p>
          <p className="text-xs text-tg-hint">Активных</p>
        </div>
        <div className="bg-tg-secondary-bg rounded-xl p-3 text-center">
          <p className="text-2xl font-bold text-blue-500">
            {inviteCodes.reduce((sum: number, c: InviteCode) => sum + c.uses_count, 0)}
          </p>
          <p className="text-xs text-tg-hint">Использований</p>
        </div>
      </div>

      {/* Codes List */}
      <div className="space-y-3">
        {inviteCodes.length === 0 ? (
          <div className="text-center py-12">
            <Link2 className="w-16 h-16 text-tg-hint/30 mx-auto mb-4" />
            <p className="text-tg-hint mb-4">Нет инвайт-кодов</p>
            <button
              onClick={() => setShowCreateModal(true)}
              className="px-4 py-2 bg-tg-button text-tg-button-text rounded-xl"
            >
              Создать первый код
            </button>
          </div>
        ) : (
          inviteCodes.map((code: InviteCode) => {
            const expired = isExpired(code.expires_at)
            const exhausted = isExhausted(code)
            const inactive = !code.is_active || expired || exhausted

            return (
              <button
                key={code.id}
                onClick={() => setSelectedCode(code)}
                className={clsx(
                  'w-full bg-tg-secondary-bg rounded-2xl p-4 text-left transition-all',
                  inactive && 'opacity-60'
                )}
              >
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <div className={clsx(
                      'w-10 h-10 rounded-xl flex items-center justify-center',
                      inactive ? 'bg-gray-500/20' : 'bg-blue-500/20'
                    )}>
                      <Link2 className={clsx(
                        'w-5 h-5',
                        inactive ? 'text-gray-500' : 'text-blue-500'
                      )} />
                    </div>
                    <div>
                      <p className="font-mono font-bold text-tg-text">{code.code}</p>
                      {code.name && (
                        <p className="text-xs text-tg-hint">{code.name}</p>
                      )}
                    </div>
                  </div>
                  <ChevronRight className="w-5 h-5 text-tg-hint" />
                </div>

                <div className="flex items-center gap-3 text-xs text-tg-hint">
                  <span className="flex items-center gap-1">
                    <Users className="w-3 h-3" />
                    {code.uses_count}{code.max_uses !== null && code.max_uses !== undefined ? `/${code.max_uses}` : ''}
                  </span>

                  {code.group_name && (
                    <span className="flex items-center gap-1">
                      <Shield className="w-3 h-3" />
                      {code.group_name}
                    </span>
                  )}

                  {code.auto_approve && (
                    <span className="flex items-center gap-1 text-green-500">
                      <CheckCircle2 className="w-3 h-3" />
                      Авто
                    </span>
                  )}

                  {expired && (
                    <span className="text-red-500">Истёк</span>
                  )}

                  {exhausted && !expired && (
                    <span className="text-orange-500">Исчерпан</span>
                  )}

                  {!code.is_active && !expired && !exhausted && (
                    <span className="text-gray-500">Отключён</span>
                  )}
                </div>
              </button>
            )
          })
        )}
      </div>

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

              <div className="flex-1 overflow-y-auto px-4 pb-4">
                <div className="flex items-center gap-2 mb-4">
                  <Sparkles className="w-5 h-5 text-blue-500" />
                  <h2 className="text-lg font-bold text-tg-text">Новый инвайт-код</h2>
                </div>

                <div className="space-y-4">
                  {/* Name */}
                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">Название (опционально)</label>
                    <input
                      type="text"
                      value={formData.name}
                      onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                      placeholder="Например: Для команды продаж"
                      className="input"
                    />
                  </div>

                  {/* Group */}
                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">Группа</label>
                    <select
                      value={formData.group_id}
                      onChange={(e) => setFormData(prev => ({ ...prev, group_id: e.target.value }))}
                      className="input"
                    >
                      <option value="">Без группы</option>
                      {groups.map((group: any) => (
                        <option key={group.id} value={group.id}>{group.name}</option>
                      ))}
                    </select>
                    <p className="text-xs text-tg-hint/70 mt-1">
                      Менеджер будет автоматически добавлен в эту группу
                    </p>
                  </div>

                  {/* Max uses */}
                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">Лимит использований</label>
                    <input
                      type="number"
                      value={formData.max_uses}
                      onChange={(e) => setFormData(prev => ({ ...prev, max_uses: e.target.value }))}
                      placeholder="Без лимита"
                      min="1"
                      className="input"
                    />
                  </div>

                  {/* Expires */}
                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">Срок действия (дней)</label>
                    <input
                      type="number"
                      value={formData.expires_in_days}
                      onChange={(e) => setFormData(prev => ({ ...prev, expires_in_days: e.target.value }))}
                      placeholder="Без срока"
                      min="1"
                      className="input"
                    />
                  </div>

                  {/* Auto approve */}
                  <label className="flex items-center gap-3 p-4 bg-tg-secondary-bg rounded-xl cursor-pointer">
                    <input
                      type="checkbox"
                      checked={formData.auto_approve}
                      onChange={(e) => setFormData(prev => ({ ...prev, auto_approve: e.target.checked }))}
                      className="w-5 h-5 rounded"
                    />
                    <div className="flex-1">
                      <p className="font-medium text-tg-text">Авто-одобрение</p>
                      <p className="text-xs text-tg-hint">
                        Менеджеры будут одобрены автоматически без проверки
                      </p>
                    </div>
                  </label>

                  {/* Notes */}
                  <div>
                    <label className="text-xs text-tg-hint mb-1 block">Заметки</label>
                    <textarea
                      value={formData.notes}
                      onChange={(e) => setFormData(prev => ({ ...prev, notes: e.target.value }))}
                      placeholder="Приватные заметки..."
                      className="input min-h-[80px] resize-none"
                    />
                  </div>
                </div>
              </div>

              <div className="flex-shrink-0 px-4 pb-4 pt-2 border-t border-tg-separator bg-tg-bg">
                <div className="flex gap-3">
                  <button
                    onClick={() => setShowCreateModal(false)}
                    className="btn btn-secondary flex-1"
                  >
                    Отмена
                  </button>
                  <button
                    onClick={handleCreate}
                    disabled={createMutation.isPending}
                    className="btn btn-primary flex-1"
                  >
                    {createMutation.isPending ? 'Создание...' : 'Создать код'}
                  </button>
                </div>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Code Detail Modal */}
      <AnimatePresence>
        {selectedCode && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setSelectedCode(null)}
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

              <div className="flex-1 overflow-y-auto px-4 pb-4">
                {/* Code Display */}
                <div className="text-center mb-6">
                  <div className="inline-flex items-center gap-2 px-6 py-4 bg-tg-secondary-bg rounded-2xl mb-4">
                    <p className="font-mono text-2xl font-bold text-tg-text tracking-wider">
                      {selectedCode.code}
                    </p>
                  </div>
                  {selectedCode.name && (
                    <p className="text-tg-hint">{selectedCode.name}</p>
                  )}
                </div>

                {/* Copy Buttons */}
                <div className="flex gap-3 mb-6">
                  <button
                    onClick={() => copyCode(selectedCode.code)}
                    className="flex-1 p-3 bg-tg-secondary-bg rounded-xl flex items-center justify-center gap-2 text-tg-text"
                  >
                    <Copy className="w-4 h-4" />
                    Копировать код
                  </button>
                  <button
                    onClick={() => copyLink(selectedCode.code)}
                    className="flex-1 p-3 bg-blue-500/10 text-blue-500 rounded-xl flex items-center justify-center gap-2"
                  >
                    <Share2 className="w-4 h-4" />
                    Копировать ссылку
                  </button>
                </div>

                {/* Stats */}
                <div className="grid grid-cols-2 gap-3 mb-6">
                  <div className="bg-tg-secondary-bg rounded-xl p-4 text-center">
                    <Users className="w-6 h-6 text-blue-500 mx-auto mb-2" />
                    <p className="text-2xl font-bold text-tg-text">
                      {selectedCode.uses_count}
                      {selectedCode.max_uses !== null && selectedCode.max_uses !== undefined && (
                        <span className="text-tg-hint text-sm">/{selectedCode.max_uses}</span>
                      )}
                    </p>
                    <p className="text-xs text-tg-hint">Использований</p>
                  </div>
                  <div className="bg-tg-secondary-bg rounded-xl p-4 text-center">
                    <Calendar className="w-6 h-6 text-purple-500 mx-auto mb-2" />
                    <p className="text-sm font-medium text-tg-text">
                      {selectedCode.expires_at
                        ? formatDate(selectedCode.expires_at)
                        : 'Без срока'}
                    </p>
                    <p className="text-xs text-tg-hint">Истекает</p>
                  </div>
                </div>

                {/* Info */}
                <div className="bg-tg-secondary-bg rounded-xl p-4 space-y-3 mb-6">
                  {selectedCode.group_name && (
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-tg-hint">Группа</span>
                      <span className="font-medium text-tg-text">{selectedCode.group_name}</span>
                    </div>
                  )}
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-tg-hint">Авто-одобрение</span>
                    <span className={clsx(
                      'font-medium',
                      selectedCode.auto_approve ? 'text-green-500' : 'text-tg-text'
                    )}>
                      {selectedCode.auto_approve ? 'Да' : 'Нет'}
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-tg-hint">Статус</span>
                    <span className={clsx(
                      'font-medium',
                      selectedCode.is_active ? 'text-green-500' : 'text-red-500'
                    )}>
                      {selectedCode.is_active ? 'Активен' : 'Отключён'}
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-tg-hint">Создан</span>
                    <span className="font-medium text-tg-text">
                      {formatDate(selectedCode.created_at)}
                    </span>
                  </div>
                </div>

                {selectedCode.notes && (
                  <div className="bg-tg-secondary-bg rounded-xl p-4 mb-6">
                    <p className="text-xs text-tg-hint mb-1">Заметки</p>
                    <p className="text-tg-text">{selectedCode.notes}</p>
                  </div>
                )}

                {/* Deep Link Preview */}
                <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-xl p-4 mb-6">
                  <p className="text-sm font-medium text-blue-600 dark:text-blue-400 mb-2">
                    Ссылка для регистрации:
                  </p>
                  <p className="text-xs text-blue-500 break-all font-mono">
                    https://t.me/weblyMN_bot?start=invite_{selectedCode.code}
                  </p>
                </div>
              </div>

              <div className="flex-shrink-0 px-4 pb-4 pt-2 border-t border-tg-separator bg-tg-bg space-y-2">
                {/* Toggle Active */}
                <button
                  onClick={() => {
                    toggleActiveMutation.mutate({
                      id: selectedCode.id,
                      is_active: !selectedCode.is_active,
                    })
                    setSelectedCode({ ...selectedCode, is_active: !selectedCode.is_active })
                  }}
                  className={clsx(
                    'w-full p-3 rounded-xl font-medium flex items-center justify-center gap-2',
                    selectedCode.is_active
                      ? 'bg-orange-500/10 text-orange-500'
                      : 'bg-green-500/10 text-green-500'
                  )}
                >
                  {selectedCode.is_active ? (
                    <>
                      <EyeOff className="w-5 h-5" />
                      Отключить код
                    </>
                  ) : (
                    <>
                      <Eye className="w-5 h-5" />
                      Включить код
                    </>
                  )}
                </button>

                {/* Delete */}
                <button
                  onClick={() => {
                    setCodeToDelete(selectedCode)
                    setSelectedCode(null)
                    setTimeout(() => setShowDeleteConfirm(true), 150)
                  }}
                  className="w-full p-3 rounded-xl bg-red-500/10 text-red-500 font-medium flex items-center justify-center gap-2"
                >
                  <Trash2 className="w-5 h-5" />
                  Удалить код
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Delete Confirmation */}
      <AnimatePresence>
        {showDeleteConfirm && codeToDelete && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/80 z-[100]"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => {
                setShowDeleteConfirm(false)
                setCodeToDelete(null)
              }}
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
                  <h3 className="text-xl font-bold text-tg-text mb-2">Удалить код?</h3>
                  <p className="text-tg-hint text-sm leading-relaxed">
                    Код <strong className="font-mono">{codeToDelete.code}</strong> будет удалён.
                    Менеджеры, зарегистрированные по нему, останутся.
                  </p>
                </div>
                <div className="flex gap-3">
                  <button
                    onClick={() => {
                      setShowDeleteConfirm(false)
                      setCodeToDelete(null)
                    }}
                    className="flex-1 px-4 py-3.5 rounded-xl bg-zinc-100 dark:bg-zinc-800 text-tg-text font-semibold"
                  >
                    Отмена
                  </button>
                  <button
                    onClick={() => deleteMutation.mutate(codeToDelete.id)}
                    disabled={deleteMutation.isPending}
                    className="flex-1 px-4 py-3.5 rounded-xl bg-red-500 text-white font-semibold disabled:opacity-50"
                  >
                    {deleteMutation.isPending ? 'Удаление...' : 'Удалить'}
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

