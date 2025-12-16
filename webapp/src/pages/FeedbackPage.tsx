import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  MessageSquarePlus, Send, Clock, CheckCircle2, MessageCircle,
  AlertCircle, Loader2, ChevronRight, ArrowLeft, Flag
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { feedbackApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

const categories = [
  { id: 'general', label: 'Общий вопрос', icon: '' },
  { id: 'bug', label: 'Ошибка/Баг', icon: '' },
  { id: 'feature', label: 'Предложение', icon: '' },
  { id: 'complaint', label: 'Жалоба', icon: '' },
  { id: 'question', label: 'Вопрос', icon: '' },
]

const priorities = [
  { id: 'low', label: 'Низкий', color: 'text-sky-400' },
  { id: 'normal', label: 'Обычный', color: 'text-yellow-500' },
  { id: 'high', label: 'Высокий', color: 'text-orange-500' },
  { id: 'urgent', label: 'Срочный', color: 'text-red-500' },
]

const statusConfig: Record<string, { label: string; icon: React.ReactNode; color: string }> = {
  new: { label: 'Новое', icon: <Clock className="w-4 h-4" />, color: 'text-blue-500' },
  in_review: { label: 'На рассмотрении', icon: <MessageCircle className="w-4 h-4" />, color: 'text-yellow-500' },
  answered: { label: 'Есть ответ', icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-green-500' },
  closed: { label: 'Закрыто', icon: <CheckCircle2 className="w-4 h-4" />, color: 'text-gray-500' },
}

export default function FeedbackPage() {
  const queryClient = useQueryClient()
  const { haptic } = useTelegram()
  const [showNewFeedback, setShowNewFeedback] = useState(false)
  const [selectedFeedback, setSelectedFeedback] = useState<any>(null)
  const [formData, setFormData] = useState({
    subject: '',
    message: '',
    category: 'general',
    priority: 'normal',
  })

  const { data: feedbackList, isLoading } = useQuery({
    queryKey: ['my-feedback'],
    queryFn: () => feedbackApi.list({ limit: 50 }).then(res => res.data),
  })

  const createMutation = useMutation({
    mutationFn: () => feedbackApi.create(formData),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['my-feedback'] })
      toast.success('Обращение отправлено')
      haptic?.notificationOccurred('success')
      setShowNewFeedback(false)
      setFormData({ subject: '', message: '', category: 'general', priority: 'normal' })
    },
    onError: () => {
      toast.error('Ошибка отправки')
      haptic?.notificationOccurred('error')
    },
  })

  const handleSubmit = () => {
    if (!formData.subject.trim() || !formData.message.trim()) {
      toast.error('Заполните тему и сообщение')
      haptic?.notificationOccurred('error')
      return
    }
    createMutation.mutate()
  }

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        <div className="skeleton h-20 rounded-2xl" />
        <div className="skeleton h-20 rounded-2xl" />
        <div className="skeleton h-20 rounded-2xl" />
      </div>
    )
  }

  const items = feedbackList?.items || []

  return (
    <div className="min-h-screen bg-tg-bg pb-24">
      {/* Header */}
      <div className="sticky top-0 z-10 bg-tg-bg border-b border-tg-separator px-4 py-3">
        <h1 className="text-lg font-semibold text-tg-text">Обратная связь</h1>
        <p className="text-sm text-tg-hint">Связь с администратором</p>
      </div>

      {/* New Feedback Button */}
      <div className="p-4">
        <button
          onClick={() => setShowNewFeedback(true)}
          className="w-full bg-gradient-to-r from-blue-500 to-purple-600 text-white p-4 rounded-2xl flex items-center justify-between"
        >
          <div className="flex items-center gap-3">
            <MessageSquarePlus className="w-6 h-6" />
            <div className="text-left">
              <p className="font-semibold">Написать обращение</p>
              <p className="text-sm opacity-80">Задать вопрос или оставить отзыв</p>
            </div>
          </div>
          <ChevronRight className="w-5 h-5" />
        </button>
      </div>

      {/* Feedback List */}
      <div className="px-4 space-y-3">
        <p className="section-header">Мои обращения</p>

        {items.length === 0 ? (
          <div className="text-center py-8">
            <MessageCircle className="w-12 h-12 text-tg-hint mx-auto mb-3" />
            <p className="text-tg-hint">У вас пока нет обращений</p>
          </div>
        ) : (
          items.map((feedback: any) => {
            const status = statusConfig[feedback.status] || statusConfig.new
            const category = categories.find(c => c.id === feedback.category)

            return (
              <motion.button
                key={feedback.id}
                onClick={() => setSelectedFeedback(feedback)}
                className="w-full bg-tg-section rounded-2xl p-4 text-left border border-tg-separator"
                whileTap={{ scale: 0.98 }}
              >
                <div className="flex items-start justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <span>{category?.icon || '💬'}</span>
                    <span className="font-medium text-tg-text">{feedback.subject}</span>
                  </div>
                  <span className={clsx('flex items-center gap-1 text-xs', status.color)}>
                    {status.icon}
                    {status.label}
                  </span>
                </div>
                <p className="text-sm text-tg-hint line-clamp-2 mb-2">{feedback.message}</p>
                <div className="flex items-center justify-between text-xs text-tg-hint">
                  <span>{new Date(feedback.created_at).toLocaleDateString('ru')}</span>
                  {feedback.admin_response && (
                    <span className="text-green-500 flex items-center gap-1">
                      <CheckCircle2 className="w-3 h-3" />
                      Есть ответ
                    </span>
                  )}
                </div>
              </motion.button>
            )
          })
        )}
      </div>

      {/* New Feedback Modal */}
      <AnimatePresence>
        {showNewFeedback && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => !createMutation.isPending && setShowNewFeedback(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom max-h-[85vh] overflow-y-auto"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
              <p className="text-lg font-semibold mb-4">Новое обращение</p>

              <div className="space-y-4">
                {/* Category */}
                <div>
                  <label className="text-xs text-tg-hint mb-2 block">Категория</label>
                  <div className="flex flex-wrap gap-2">
                    {categories.map(cat => (
                      <button
                        key={cat.id}
                        onClick={() => setFormData(prev => ({ ...prev, category: cat.id }))}
                        className={clsx(
                          'px-3 py-2 rounded-xl text-sm font-medium',
                          formData.category === cat.id
                            ? 'bg-black dark:bg-white text-white dark:text-black'
                            : 'bg-tg-secondary-bg'
                        )}
                      >
                        {cat.icon} {cat.label}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Subject */}
                <div>
                  <label className="text-xs text-tg-hint mb-1 block">Тема *</label>
                  <input
                    type="text"
                    value={formData.subject}
                    onChange={(e) => setFormData(prev => ({ ...prev, subject: e.target.value }))}
                    placeholder="Кратко опишите суть"
                    className="input"
                    maxLength={100}
                  />
                </div>

                {/* Message */}
                <div>
                  <label className="text-xs text-tg-hint mb-1 block">Сообщение *</label>
                  <textarea
                    value={formData.message}
                    onChange={(e) => setFormData(prev => ({ ...prev, message: e.target.value }))}
                    placeholder="Подробно опишите ваш вопрос или проблему..."
                    className="input min-h-[120px] resize-none"
                  />
                </div>

                {/* Priority */}
                <div>
                  <label className="text-xs text-tg-hint mb-2 block flex items-center gap-1">
                    <Flag className="w-3 h-3" />
                    Приоритет
                  </label>
                  <div className="flex gap-2">
                    {priorities.map(p => (
                      <button
                        key={p.id}
                        onClick={() => setFormData(prev => ({ ...prev, priority: p.id }))}
                        className={clsx(
                          'flex-1 px-3 py-2 rounded-xl text-sm font-medium',
                          formData.priority === p.id
                            ? 'bg-black dark:bg-white text-white dark:text-black'
                            : 'bg-tg-secondary-bg'
                        )}
                      >
                        {p.label}
                      </button>
                    ))}
                  </div>
                </div>
              </div>

              <div className="flex gap-3 mt-6">
                <button
                  onClick={() => setShowNewFeedback(false)}
                  disabled={createMutation.isPending}
                  className="btn btn-secondary flex-1"
                >
                  Отмена
                </button>
                <button
                  onClick={handleSubmit}
                  disabled={createMutation.isPending || !formData.subject.trim() || !formData.message.trim()}
                  className="btn btn-primary flex-1"
                >
                  {createMutation.isPending ? (
                    <Loader2 className="w-5 h-5 animate-spin" />
                  ) : (
                    <>
                      <Send className="w-5 h-5" />
                      Отправить
                    </>
                  )}
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Feedback Detail Modal */}
      <AnimatePresence>
        {selectedFeedback && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setSelectedFeedback(null)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom max-h-[85vh] overflow-y-auto"
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
                  <div className="flex items-center gap-3 text-sm text-tg-hint">
                    <span>{new Date(selectedFeedback.created_at).toLocaleString('ru')}</span>
                    <span className={clsx(
                      'flex items-center gap-1',
                      statusConfig[selectedFeedback.status]?.color
                    )}>
                      {statusConfig[selectedFeedback.status]?.icon}
                      {statusConfig[selectedFeedback.status]?.label}
                    </span>
                  </div>
                </div>

                {/* Original Message */}
                <div className="bg-tg-secondary-bg rounded-xl p-4">
                  <p className="text-xs text-tg-hint mb-2">Ваше сообщение:</p>
                  <p className="text-tg-text whitespace-pre-wrap">{selectedFeedback.message}</p>
                </div>

                {/* Admin Response */}
                {selectedFeedback.admin_response && (
                  <div className="bg-green-50 dark:bg-green-900/20 border-l-4 border-green-500 rounded-xl p-4">
                    <div className="flex items-center gap-2 mb-2">
                      <CheckCircle2 className="w-4 h-4 text-green-500" />
                      <p className="text-xs text-green-600 dark:text-green-400">
                        Ответ администратора
                        {selectedFeedback.responded_at && (
                          <span className="ml-2">
                            ({new Date(selectedFeedback.responded_at).toLocaleString('ru')})
                          </span>
                        )}
                      </p>
                    </div>
                    <p className="text-tg-text whitespace-pre-wrap">{selectedFeedback.admin_response}</p>
                  </div>
                )}

                {!selectedFeedback.admin_response && selectedFeedback.status !== 'closed' && (
                  <div className="bg-yellow-50 dark:bg-yellow-900/20 rounded-xl p-4 text-center">
                    <Clock className="w-8 h-8 text-yellow-500 mx-auto mb-2" />
                    <p className="text-tg-text font-medium">Ожидает ответа</p>
                    <p className="text-sm text-tg-hint">Администратор скоро ответит на ваше обращение</p>
                  </div>
                )}
              </div>

              <button
                onClick={() => setSelectedFeedback(null)}
                className="btn btn-secondary w-full mt-6"
              >
                Закрыть
              </button>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  )
}

