import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  User, Phone, Star, FileText, CheckCircle, Clock,
  TrendingUp, Award, Edit2, X, Loader2, LogOut
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { profileApi } from '@/api/client'
import toast from 'react-hot-toast'

const ranks = [
  { min: 0, name: 'Новичок' },
  { min: 5, name: 'Начинающий' },
  { min: 15, name: 'Опытный' },
  { min: 30, name: 'Профессионал' },
  { min: 50, name: 'Эксперт' },
  { min: 100, name: 'Мастер' },
]

export default function ProfilePage() {
  const { user, logout } = useAuthStore()
  const { haptic, webApp } = useTelegram()
  const queryClient = useQueryClient()

  const [editContact, setEditContact] = useState(false)
  const [contactValue, setContactValue] = useState(user?.contact || '')

  const { data: stats } = useQuery({
    queryKey: ['profile-stats'],
    queryFn: () => profileApi.stats().then(res => res.data),
  })

  const updateMutation = useMutation({
    mutationFn: (data: { contact: string }) => profileApi.update(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['profile'] })
      toast.success('Сохранено')
      setEditContact(false)
    },
    onError: () => toast.error('Ошибка'),
  })

  const getRank = (count: number) => {
    return [...ranks].reverse().find(r => count >= r.min) || ranks[0]
  }

  const getNextRank = (count: number) => {
    const idx = ranks.findIndex(r => count < r.min) - 1
    if (idx < ranks.length - 1) return ranks[idx + 1]
    return null
  }

  const totalRequests = stats?.total_requests || 0
  const rank = getRank(totalRequests)
  const nextRank = getNextRank(totalRequests)
  const progress = nextRank ? ((totalRequests - rank.min) / (nextRank.min - rank.min)) * 100 : 100

  const handleLogout = () => {
    webApp?.showConfirm('Выйти?', (confirmed) => {
      if (confirmed) {
        logout()
        webApp?.close()
      }
    })
  }

  return (
    <div className="min-h-screen pb-8 bg-tg-bg">
      {/* Header */}
      <div className="bg-black dark:bg-white text-white dark:text-black p-6 pb-24">
        <div className="text-center">
          <div className="w-20 h-20 rounded-full bg-white/20 dark:bg-black/20 mx-auto mb-4 flex items-center justify-center text-3xl font-bold">
            {user?.first_name?.[0]?.toUpperCase()}
          </div>
          <h1 className="text-2xl font-bold">
            {user?.first_name} {user?.last_name}
          </h1>
          {user?.username && <p className="opacity-70">@{user.username}</p>}
        </div>
      </div>

      <div className="px-4 -mt-16 space-y-4">
        {/* Rank */}
        <motion.div
          className="bg-tg-section rounded-2xl p-5 border border-tg-separator"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
        >
          <div className="flex items-center gap-4 mb-4">
            <div className="p-3 rounded-xl bg-tg-secondary-bg">
              <Award className="w-6 h-6 text-tg-hint" />
            </div>
            <div className="flex-1">
              <p className="text-sm text-tg-hint">Ранг</p>
              <p className="text-lg font-bold">{rank.name}</p>
            </div>
            <div className="text-right">
              <p className="text-2xl font-bold">{totalRequests}</p>
              <p className="text-xs text-tg-hint">заявок</p>
            </div>
          </div>
          {nextRank && (
            <div>
              <div className="flex justify-between text-xs text-tg-hint mb-1">
                <span>{rank.name}</span>
                <span>{nextRank.name}</span>
              </div>
              <div className="h-2 bg-tg-secondary-bg rounded-full overflow-hidden">
                <motion.div
                  className="h-full bg-black dark:bg-white rounded-full"
                  initial={{ width: 0 }}
                  animate={{ width: `${progress}%` }}
                />
              </div>
              <p className="text-xs text-tg-hint mt-1 text-center">
                Ещё {nextRank.min - totalRequests} до следующего
              </p>
            </div>
          )}
        </motion.div>

        {/* Stats */}
        <div className="grid grid-cols-2 gap-3">
          <StatCard icon={<FileText className="w-5 h-5" />} value={stats?.total_requests || 0} label="Всего" />
          <StatCard icon={<CheckCircle className="w-5 h-5" />} value={stats?.completed_requests || 0} label="Готово" />
          <StatCard icon={<Clock className="w-5 h-5" />} value={stats?.pending_requests || 0} label="В работе" />
          <StatCard icon={<TrendingUp className="w-5 h-5" />} value={stats?.this_week || 0} label="За неделю" />
        </div>

        {/* Contact */}
        <div>
          <p className="section-header">Контакт</p>
          <div className="bg-tg-section rounded-2xl border border-tg-separator">
            <button
              onClick={() => {
                haptic?.impactOccurred('light')
                setContactValue(user?.contact || '')
                setEditContact(true)
              }}
              className="list-item w-full text-left"
            >
              <Phone className="w-5 h-5 text-tg-hint" />
              <div className="flex-1">
                <p className="text-xs text-tg-hint">Для связи</p>
                <p>{user?.contact || 'Не указан'}</p>
              </div>
              <Edit2 className="w-4 h-4 text-tg-hint" />
            </button>
          </div>
        </div>

        {/* Account */}
        <div>
          <p className="section-header">Аккаунт</p>
          <div className="bg-tg-section rounded-2xl border border-tg-separator">
            <div className="list-item">
              <User className="w-5 h-5 text-tg-hint" />
              <div className="flex-1">
                <p className="text-xs text-tg-hint">Telegram ID</p>
                <p>{user?.tg_id}</p>
              </div>
            </div>
            <div className="divider" />
            <div className="list-item">
              <Star className="w-5 h-5 text-tg-hint" />
              <div className="flex-1">
                <p className="text-xs text-tg-hint">Роль</p>
                <p>{user?.role === 'admin' ? 'Админ' : 'Менеджер'}</p>
              </div>
            </div>
            <div className="divider" />
            <button onClick={handleLogout} className="list-item w-full text-left text-red-500">
              <LogOut className="w-5 h-5" />
              <span>Выйти</span>
            </button>
          </div>
        </div>
      </div>

      {/* Edit Modal */}
      <AnimatePresence>
        {editContact && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/50 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setEditContact(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="flex justify-between mb-4">
                <p className="text-lg font-semibold">Контакт</p>
                <button onClick={() => setEditContact(false)}>
                  <X className="w-5 h-5 text-tg-hint" />
                </button>
              </div>
              <input
                type="text"
                value={contactValue}
                onChange={(e) => setContactValue(e.target.value)}
                placeholder="Телефон или email"
                className="input"
                autoFocus
              />
              <button
                onClick={() => updateMutation.mutate({ contact: contactValue })}
                disabled={updateMutation.isPending}
                className="btn btn-primary w-full mt-4"
              >
                {updateMutation.isPending ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Сохранить'}
              </button>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  )
}

function StatCard({ icon, value, label }: { icon: React.ReactNode; value: number; label: string }) {
  return (
    <div className="bg-tg-section rounded-2xl p-4 border border-tg-separator">
      <div className="flex items-center gap-2 mb-2 text-tg-hint">
        {icon}
        <span className="text-xs">{label}</span>
      </div>
      <p className="text-2xl font-bold">{value}</p>
    </div>
  )
}
