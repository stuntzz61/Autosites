import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  MessageSquare, Clock, CheckCircle2, MessageCircle, Send,
  AlertCircle, Loader2, User, Filter, ArrowLeft, Flag,
  XCircle, RefreshCw
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { feedbackApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const categories = [
  { id: 'general', label: 'Общий', icon: '💬' },
  { id: 'bug', label: 'Баг', icon: '🐛' },
  { id: 'feature', label: 'Идея', icon: '💡' },
  { id: 'complaint', label: 'Жалоба', icon: '😤' },
  { id: 'question', label: 'Вопрос', icon: '❓' },
]

const priorities = [
  { id: 'urgent', label: 'Срочный', color: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400', dot: 'bg-red-500' },
  { id: 'high', label: 'Высокий', color: 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400', dot: 'bg-orange-500' },
  { id: 'normal', label: 'Обычный', color: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400', dot: 'bg-yellow-500' },
  { id: 'low', label: 'Низкий', color: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400', dot: 'bg-green-500' },
]

const statuses = [
  { id: 'new', label: 'Новые', icon: <Clock className="w-4 h-4" />, color: 'text-blue-500' },
  { id: 'in_review', label: 'В работе', icon: <MessageCircle className="w-4 h-4" />, color: 'text-yellow-500' },
  { id: 'answered', label: 'Отвечено', icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-green-500' },
  { id: 'closed', label: 'Закрыто', icon: <XCircle className="w-4 h-4" />, color: 'text-gray-500' },
]

export default function AdminFeedback() {
  const queryClient = useQueryClient()
  const { haptic } = useTelegram()
  const [selectedFeedback, setSelectedFeedback] = useState<any>(null)
  const [statusFilter, setStatusFilter] = useState<string>('new')
  const [responseText, setResponseText] = useState('')

  const { data: feedbackData, isLoading, refetch } = useQuery({
    queryKey: ['admin-feedback', statusFilter],
    queryFn: () => feedbackApi.adminList({ status: statusFilter || undefined, limit: 100 }).then(res => res.data),
  })

  const respondMutation = useMutation({
    mutationFn: () => feedbackApi.adminRespond(selectedFeedback.id, {
      response: responseText,
      status: 'answered'
    }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-feedback'] })
      toast.success('Ответ отправлен')
      haptic?.notificationOccurred('success')
      setResponseText('')
      setSelectedFeedback(null)
    },
    onError: () => {
      toast.error('Ошибка отправки')
      haptic?.notificationOccurred('error')
    },
  })

  const updateStatusMutation = useMutation({
    mutationFn: ({ id, status }: { id: string; status: string }) =>
      feedbackApi.adminUpdateStatus(id, status),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-feedback'] })
      toast.success('Статус обновлён')
    },
  })

  const items = feedbackData?.items || []
  const newCount = feedbackData?.new_count || 0

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        <div className="skeleton h-12 rounded-xl" />
        <div className="skeleton h-24 rounded-2xl" />
        <div className="skeleton h-24 rounded-2xl" />
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-tg-bg pb-24">
      {/* Header */}
      <div className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator px-4 py-3">
        <div className="flex items-center justify-between mb-3">
          <div>
            <h1 className="text-lg font-semibold text-tg-text">Обращения</h1>
            <p className="text-sm text-tg-hint">
              {newCount > 0 ? `${newCount} новых` : 'Нет новых'}
            </p>
          </div>
          <button
            onClick={() => refetch()}
            className="p-2 bg-tg-secondary-bg rounded-xl"
          >
            <RefreshCw className="w-5 h-5 text-tg-hint" />
          </button>
        </div>

        {/* Status Filter */}
        <div className="flex gap-2 overflow-x-auto pb-1">
          <button
            onClick={() => setStatusFilter('')}
            className={clsx(
              'px-3 py-1.5 rounded-lg text-sm font-medium whitespace-nowrap',
              !statusFilter
                ? 'bg-black dark:bg-white text-white dark:text-black'
                : 'bg-tg-secondary-bg text-tg-hint'
            )}
          >
            Все
          </button>
          {statuses.map(s => (
            <button
              key={s.id}
              onClick={() => setStatusFilter(s.id)}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-sm font-medium whitespace-nowrap flex items-center gap-1.5',
                statusFilter === s.id
                  ? 'bg-black dark:bg-white text-white dark:text-black'
                  : 'bg-tg-secondary-bg text-tg-hint'
              )}
            >
              {s.icon}
              {s.label}
              {s.id === 'new' && newCount > 0 && (
                <span className="bg-red-500 text-white text-xs px-1.5 py-0.5 rounded-full">
                  {newCount}
                </span>
              )}
            </button>
          ))}
        </div>
      </div>

      {/* Feedback List */}
      <div className="p-4 space-y-3">
        {items.length === 0 ? (
          <div className="text-center py-12">
            <MessageSquare className="w-12 h-12 text-tg-hint mx-auto mb-3" />
            <p className="text-tg-hint">
              {statusFilter ? 'Нет обращений с этим статусом' : 'Нет обращений'}
            </p>
          </div>
        ) : (
          items.map((feedback: any) => {
            const priority = priorities.find(p => p.id === feedback.priority) || priorities[2]
            const status = statuses.find(s => s.id === feedback.status) || statuses[0]
            const category = categories.find(c => c.id === feedback.category)

            return (
              <motion.button
                key={feedback.id}
                onClick={() => setSelectedFeedback(feedback)}
                className="w-full bg-tg-section rounded-2xl p-4 text-left border border-tg-separator"
                whileTap={{ scale: 0.98 }}
              >
                <div className="flex items-start gap-3">
                  {/* Priority indicator */}
                  <div className={clsx('w-2 h-2 rounded-full mt-2', priority.dot)} />

                  <div className="flex-1 min-w-0">
                    {/* Header */}
                    <div className="flex items-start justify-between gap-2 mb-2">
                      <div className="flex items-center gap-2">
                        <span>{category?.icon || '💬'}</span>
                        <span className="font-medium text-tg-text truncate">{feedback.subject}</span>
                      </div>
                      <span className={clsx('flex items-center gap-1 text-xs shrink-0', status.color)}>
                        {status.icon}
                        {status.label}
                      </span>
                    </div>

                    {/* Manager info */}
                    <div className="flex items-center gap-2 text-xs text-tg-hint mb-2">
                      <User className="w-3 h-3" />
                      <span>
                        {feedback.manager_first_name} {feedback.manager_last_name}
                        {feedback.manager_username && ` (@${feedback.manager_username})`}
                      </span>
                    </div>

                    {/* Message preview */}
                    <p className="text-sm text-tg-hint line-clamp-2 mb-2">{feedback.message}</p>

                    {/* Footer */}
                    <div className="flex items-center justify-between">
                      <span className="text-xs text-tg-hint">
                        {new Date(feedback.created_at).toLocaleString('ru')}
                      </span>
                      <span className={clsx('text-xs px-2 py-0.5 rounded-full', priority.color)}>
                        {priority.label}
                      </span>
                    </div>
                  </div>
                </div>
              </motion.button>
            )
          })
        )}
      </div>

      {/* Feedback Detail Modal */}
      <AnimatePresence>
        {selectedFeedback && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => !respondMutation.isPending && setSelectedFeedback(null)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom max-h-[90vh] overflow-y-auto"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />

              <button
                onClick={() => setSelectedFeedback(null)}
                className="flex items-center gap-2 text-tg-link mb-4"
              >
                <ArrowLeft className="w-4 h-4" />
                Назад
              </button>

              <div className="space-y-4">
                {/* Header */}
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <span className="text-xl">
                      {categories.find(c => c.id === selectedFeedback.category)?.icon || '💬'}
                    </span>
                    <h2 className="text-lg font-semibold text-tg-text">{selectedFeedback.subject}</h2>
                  </div>

                  {/* Manager & Meta */}
                  <div className="flex flex-wrap items-center gap-3 text-sm">
                    <span className="flex items-center gap-1 text-tg-hint">
                      <User className="w-4 h-4" />
                      {selectedFeedback.manager_first_name} {selectedFeedback.manager_last_name}
                      {selectedFeedback.manager_username && ` (@${selectedFeedback.manager_username})`}
                    </span>
                    <span className={clsx(
                      'px-2 py-0.5 rounded-full text-xs',
                      priorities.find(p => p.id === selectedFeedback.priority)?.color
                    )}>
                      <Flag className="w-3 h-3 inline mr-1" />
                      {priorities.find(p => p.id === selectedFeedback.priority)?.label}
                    </span>
                  </div>
                  <p className="text-xs text-tg-hint mt-1">
                    {new Date(selectedFeedback.created_at).toLocaleString('ru')}
                  </p>
                </div>

                {/* Message */}
                <div className="bg-tg-secondary-bg rounded-xl p-4">
                  <p className="text-tg-text whitespace-pre-wrap">{selectedFeedback.message}</p>
                </div>

                {/* Existing Response */}
                {selectedFeedback.admin_response && (
                  <div className="bg-green-50 dark:bg-green-900/20 border-l-4 border-green-500 rounded-xl p-4">
                    <p className="text-xs text-green-600 dark:text-green-400 mb-2 flex items-center gap-1">
                      <CheckCircle2 className="w-4 h-4" />
                      Ваш ответ ({new Date(selectedFeedback.responded_at).toLocaleString('ru')})
                    </p>
                    <p className="text-tg-text whitespace-pre-wrap">{selectedFeedback.admin_response}</p>
                  </div>
                )}

                {/* Response Form */}
                {selectedFeedback.status !== 'closed' && (
                  <div className="space-y-3">
                    <label className="text-sm font-medium text-tg-text">
                      {selectedFeedback.admin_response ? 'Обновить ответ' : 'Написать ответ'}
                    </label>
                    <textarea
                      value={responseText}
                      onChange={(e) => setResponseText(e.target.value)}
                      placeholder="Введите ваш ответ..."
                      className="input min-h-[100px] resize-none"
                    />
                    <button
                      onClick={() => respondMutation.mutate()}
                      disabled={!responseText.trim() || respondMutation.isPending}
                      className="btn btn-primary w-full"
                    >
                      {respondMutation.isPending ? (
                        <Loader2 className="w-5 h-5 animate-spin" />
                      ) : (
                        <>
                          <Send className="w-5 h-5" />
                          Отправить ответ
                        </>
                      )}
                    </button>
                  </div>
                )}

                {/* Status Actions */}
                <div className="border-t border-tg-separator pt-4">
                  <p className="text-xs text-tg-hint mb-2">Изменить статус:</p>
                  <div className="flex flex-wrap gap-2">
                    {statuses.map(s => (
                      <button
                        key={s.id}
                        onClick={() => {
                          updateStatusMutation.mutate({ id: selectedFeedback.id, status: s.id })
                          setSelectedFeedback({ ...selectedFeedback, status: s.id })
                        }}
                        disabled={selectedFeedback.status === s.id}
                        className={clsx(
                          'px-3 py-1.5 rounded-lg text-sm font-medium flex items-center gap-1.5',
                          selectedFeedback.status === s.id
                            ? 'bg-black dark:bg-white text-white dark:text-black'
                            : 'bg-tg-secondary-bg text-tg-hint'
                        )}
                      >
                        {s.icon}
                        {s.label}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  )
}

