import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  MessageSquarePlus, Send, Clock, CheckCircle2, MessageCircle,
  Loader2, ChevronRight, ArrowLeft, Flag
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTelegram } from '@/contexts/TelegramContext'
import { feedbackApi } from '@/api/client'
import toast from 'react-hot-toast'

const categories = [
  { id: 'general', label: 'Общий вопрос', icon: '💬' },
  { id: 'bug', label: 'Ошибка/Баг', icon: '🐛' },
  { id: 'feature', label: 'Предложение', icon: '💡' },
  { id: 'complaint', label: 'Жалоба', icon: '⚠️' },
  { id: 'question', label: 'Вопрос', icon: '❓' },
]

const priorities = [
  { id: 'low', label: 'Низкий', color: 'var(--accent-primary-light)' },
  { id: 'normal', label: 'Обычный', color: 'var(--warning-light)' },
  { id: 'high', label: 'Высокий', color: 'var(--gold-primary)' },
  { id: 'urgent', label: 'Срочный', color: 'var(--error-light)' },
]

const statusConfig: Record<string, { label: string; icon: React.ReactNode; bg: string; color: string; border: string }> = {
  new: {
    label: 'Новое',
    icon: <Clock className="w-3.5 h-3.5" />,
    bg: 'rgba(59, 130, 246, 0.12)',
    color: 'var(--accent-primary-light)',
    border: 'rgba(59, 130, 246, 0.25)'
  },
  in_review: {
    label: 'На рассмотрении',
    icon: <MessageCircle className="w-3.5 h-3.5" />,
    bg: 'var(--warning-bg)',
    color: 'var(--warning-light)',
    border: 'var(--warning-border)'
  },
  answered: {
    label: 'Есть ответ',
    icon: <CheckCircle2 className="w-3.5 h-3.5" />,
    bg: 'var(--success-bg)',
    color: 'var(--success-light)',
    border: 'var(--success-border)'
  },
  closed: {
    label: 'Закрыто',
    icon: <CheckCircle2 className="w-3.5 h-3.5" />,
    bg: 'rgba(100, 116, 139, 0.12)',
    color: 'var(--text-muted)',
    border: 'rgba(100, 116, 139, 0.25)'
  },
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
      <div className="p-4 space-y-4" style={{ background: 'var(--bg-deep)' }}>
        <div className="skeleton h-20 rounded-2xl" />
        <div className="skeleton h-20 rounded-2xl" />
        <div className="skeleton h-20 rounded-2xl" />
      </div>
    )
  }

  const items = feedbackList?.items || []

  return (
    <div className="min-h-screen pb-24" style={{ background: 'var(--bg-deep)' }}>
      {/* Header */}
      <div
        className="sticky top-0 z-10 backdrop-blur-xl border-b px-5 py-4"
        style={{
          background: 'rgba(11, 17, 32, 0.95)',
          borderColor: 'var(--border-subtle)'
        }}
      >
        <h1 className="text-lg font-bold" style={{ color: 'var(--text-primary)' }}>
          Обратная связь
        </h1>
        <p className="text-sm" style={{ color: 'var(--text-subtle)' }}>
          Связь с администратором
        </p>
      </div>

      {/* New Feedback Button */}
      <div className="p-4">
        <button
          onClick={() => setShowNewFeedback(true)}
          className="w-full p-5 rounded-2xl flex items-center justify-between transition-all hover:-translate-y-0.5 active:scale-[0.99]"
          style={{
            background: 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 100%)',
            boxShadow: '0 4px 24px -4px rgba(59, 130, 246, 0.4)'
          }}
        >
          <div className="flex items-center gap-4">
            <div
              className="w-12 h-12 rounded-xl flex items-center justify-center"
              style={{ background: 'rgba(255, 255, 255, 0.15)' }}
            >
              <MessageSquarePlus className="w-6 h-6 text-white" />
            </div>
            <div className="text-left">
              <p className="font-bold text-white">Написать обращение</p>
              <p className="text-sm text-white/70">Задать вопрос или оставить отзыв</p>
            </div>
          </div>
          <ChevronRight className="w-5 h-5 text-white/70" />
        </button>
      </div>

      {/* Feedback List */}
      <div className="px-4 space-y-3">
        <p className="section-header">Мои обращения</p>

        {items.length === 0 ? (
          <div className="text-center py-12">
            <div
              className="w-16 h-16 rounded-2xl flex items-center justify-center mx-auto mb-4"
              style={{
                background: 'var(--bg-surface)',
                border: '1px solid var(--border-default)'
              }}
            >
              <MessageCircle className="w-8 h-8" style={{ color: 'var(--text-subtle)' }} />
            </div>
            <p style={{ color: 'var(--text-muted)' }}>У вас пока нет обращений</p>
          </div>
        ) : (
          items.map((feedback: any) => {
            const status = statusConfig[feedback.status] || statusConfig.new
            const category = categories.find(c => c.id === feedback.category)

            return (
              <motion.button
                key={feedback.id}
                onClick={() => setSelectedFeedback(feedback)}
                className="w-full rounded-2xl p-4 text-left transition-all hover:-translate-y-0.5 active:scale-[0.99]"
                style={{
                  background: 'var(--bg-surface)',
                  border: '1px solid var(--border-default)'
                }}
                whileTap={{ scale: 0.98 }}
              >
                <div className="flex items-start justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <span>{category?.icon || '💬'}</span>
                    <span className="font-semibold" style={{ color: 'var(--text-primary)' }}>
                      {feedback.subject}
                    </span>
                  </div>
                  <span
                    className="flex items-center gap-1 text-xs font-semibold px-2 py-1 rounded-full"
                    style={{
                      background: status.bg,
                      color: status.color,
                      border: `1px solid ${status.border}`
                    }}
                  >
                    {status.icon}
                    {status.label}
                  </span>
                </div>
                <p
                  className="text-sm line-clamp-2 mb-3"
                  style={{ color: 'var(--text-muted)' }}
                >
                  {feedback.message}
                </p>
                <div className="flex items-center justify-between text-xs" style={{ color: 'var(--text-subtle)' }}>
                  <span>{new Date(feedback.created_at).toLocaleDateString('ru')}</span>
                  {feedback.admin_response && (
                    <span className="flex items-center gap-1" style={{ color: 'var(--success-light)' }}>
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
              className="fixed inset-0 z-40"
              style={{ background: 'rgba(0, 0, 0, 0.7)' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => !createMutation.isPending && setShowNewFeedback(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 rounded-t-3xl p-5 z-50 safe-bottom max-h-[85vh] overflow-y-auto"
              style={{
                background: 'var(--bg-elevated)',
                border: '1px solid var(--border-subtle)',
                borderBottom: 'none'
              }}
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
              transition={{ type: 'spring', damping: 30, stiffness: 400 }}
            >
              <div
                className="w-10 h-1 rounded-full mx-auto mb-5"
                style={{ background: 'var(--bg-tertiary)' }}
              />
              <p className="text-lg font-bold mb-5" style={{ color: 'var(--text-primary)' }}>
                Новое обращение
              </p>

              <div className="space-y-5">
                {/* Category */}
                <div>
                  <label className="text-sm font-medium mb-2 block" style={{ color: 'var(--text-muted)' }}>
                    Категория
                  </label>
                  <div className="flex flex-wrap gap-2">
                    {categories.map(cat => (
                      <button
                        key={cat.id}
                        onClick={() => setFormData(prev => ({ ...prev, category: cat.id }))}
                        className="px-3 py-2 rounded-xl text-sm font-medium transition-all active:scale-95"
                        style={{
                          background: formData.category === cat.id
                            ? 'var(--accent-primary)'
                            : 'var(--bg-surface)',
                          border: `1px solid ${formData.category === cat.id ? 'var(--accent-primary)' : 'var(--border-default)'}`,
                          color: formData.category === cat.id ? 'white' : 'var(--text-secondary)'
                        }}
                      >
                        {cat.icon} {cat.label}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Subject */}
                <div>
                  <label className="text-sm font-medium mb-2 block" style={{ color: 'var(--text-muted)' }}>
                    Тема <span style={{ color: 'var(--error-light)' }}>*</span>
                  </label>
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
                  <label className="text-sm font-medium mb-2 block" style={{ color: 'var(--text-muted)' }}>
                    Сообщение <span style={{ color: 'var(--error-light)' }}>*</span>
                  </label>
                  <textarea
                    value={formData.message}
                    onChange={(e) => setFormData(prev => ({ ...prev, message: e.target.value }))}
                    placeholder="Подробно опишите ваш вопрос или проблему..."
                    className="input min-h-[120px]"
                  />
                </div>

                {/* Priority */}
                <div>
                  <label className="text-sm font-medium mb-2 flex items-center gap-1.5" style={{ color: 'var(--text-muted)' }}>
                    <Flag className="w-3.5 h-3.5" />
                    Приоритет
                  </label>
                  <div className="flex gap-2">
                    {priorities.map(p => (
                      <button
                        key={p.id}
                        onClick={() => setFormData(prev => ({ ...prev, priority: p.id }))}
                        className="flex-1 px-3 py-2.5 rounded-xl text-sm font-medium transition-all active:scale-95"
                        style={{
                          background: formData.priority === p.id
                            ? 'var(--accent-primary)'
                            : 'var(--bg-surface)',
                          border: `1px solid ${formData.priority === p.id ? 'var(--accent-primary)' : 'var(--border-default)'}`,
                          color: formData.priority === p.id ? 'white' : 'var(--text-secondary)'
                        }}
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
                      <Send className="w-4 h-4" />
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
              className="fixed inset-0 z-40"
              style={{ background: 'rgba(0, 0, 0, 0.7)' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setSelectedFeedback(null)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 rounded-t-3xl p-5 z-50 safe-bottom max-h-[85vh] overflow-y-auto"
              style={{
                background: 'var(--bg-elevated)',
                border: '1px solid var(--border-subtle)',
                borderBottom: 'none'
              }}
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
              transition={{ type: 'spring', damping: 30, stiffness: 400 }}
            >
              <div
                className="w-10 h-1 rounded-full mx-auto mb-5"
                style={{ background: 'var(--bg-tertiary)' }}
              />

              <button
                onClick={() => setSelectedFeedback(null)}
                className="flex items-center gap-2 mb-5 transition-colors hover:opacity-80"
                style={{ color: 'var(--accent-primary-light)' }}
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
                    <h2 className="text-lg font-bold" style={{ color: 'var(--text-primary)' }}>
                      {selectedFeedback.subject}
                    </h2>
                  </div>
                  <div className="flex items-center gap-3 text-sm" style={{ color: 'var(--text-subtle)' }}>
                    <span>{new Date(selectedFeedback.created_at).toLocaleString('ru')}</span>
                    <span
                      className="flex items-center gap-1 text-xs font-semibold px-2 py-1 rounded-full"
                      style={{
                        background: statusConfig[selectedFeedback.status]?.bg,
                        color: statusConfig[selectedFeedback.status]?.color,
                        border: `1px solid ${statusConfig[selectedFeedback.status]?.border}`
                      }}
                    >
                      {statusConfig[selectedFeedback.status]?.icon}
                      {statusConfig[selectedFeedback.status]?.label}
                    </span>
                  </div>
                </div>

                {/* Original Message */}
                <div
                  className="rounded-xl p-4"
                  style={{
                    background: 'var(--bg-surface)',
                    border: '1px solid var(--border-default)'
                  }}
                >
                  <p className="text-xs mb-2" style={{ color: 'var(--text-subtle)' }}>
                    Ваше сообщение:
                  </p>
                  <p className="whitespace-pre-wrap" style={{ color: 'var(--text-primary)' }}>
                    {selectedFeedback.message}
                  </p>
                </div>

                {/* Admin Response */}
                {selectedFeedback.admin_response && (
                  <div
                    className="rounded-xl p-4"
                    style={{
                      background: 'var(--success-bg)',
                      borderLeft: '4px solid var(--success)',
                      border: '1px solid var(--success-border)'
                    }}
                  >
                    <div className="flex items-center gap-2 mb-2">
                      <CheckCircle2 className="w-4 h-4" style={{ color: 'var(--success)' }} />
                      <p className="text-xs font-medium" style={{ color: 'var(--success-light)' }}>
                        Ответ администратора
                        {selectedFeedback.responded_at && (
                          <span className="ml-2 opacity-70">
                            ({new Date(selectedFeedback.responded_at).toLocaleString('ru')})
                          </span>
                        )}
                      </p>
                    </div>
                    <p className="whitespace-pre-wrap" style={{ color: 'var(--text-primary)' }}>
                      {selectedFeedback.admin_response}
                    </p>
                  </div>
                )}

                {!selectedFeedback.admin_response && selectedFeedback.status !== 'closed' && (
                  <div
                    className="rounded-xl p-5 text-center"
                    style={{
                      background: 'var(--warning-bg)',
                      border: '1px solid var(--warning-border)'
                    }}
                  >
                    <Clock className="w-10 h-10 mx-auto mb-3" style={{ color: 'var(--warning-light)' }} />
                    <p className="font-semibold" style={{ color: 'var(--text-primary)' }}>
                      Ожидает ответа
                    </p>
                    <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
                      Администратор скоро ответит на ваше обращение
                    </p>
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
