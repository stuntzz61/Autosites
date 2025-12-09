import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  User, Phone, Star, FileText, CheckCircle, Clock,
  TrendingUp, Award, Edit2, X, Loader2, LogOut, Sparkles, Crown
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { profileApi } from '@/api/client'
import toast from 'react-hot-toast'

const ranks = [
  { min: 0, name: 'Новичок', emoji: '🌱' },
  { min: 5, name: 'Начинающий', emoji: '🌿' },
  { min: 15, name: 'Опытный', emoji: '🌲' },
  { min: 30, name: 'Профессионал', emoji: '⭐' },
  { min: 50, name: 'Эксперт', emoji: '💎' },
  { min: 100, name: 'Мастер', emoji: '👑' },
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
    webApp?.showConfirm('Выйти из аккаунта?', (confirmed) => {
      if (confirmed) {
        logout()
        webApp?.close()
      }
    })
  }

  return (
    <div className="min-h-screen pb-8 bg-tg-bg">
      {/* Header with gradient */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-br from-zinc-900 via-zinc-800 to-zinc-900 dark:from-zinc-100 dark:via-zinc-50 dark:to-zinc-100" />
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_bottom_left,_var(--tw-gradient-stops))] from-purple-500/20 via-transparent to-blue-500/20" />

        <div className="relative px-5 pt-12 pb-24">
          <motion.div
            className="text-center"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
          >
            <div className="relative inline-block">
              <div className="w-24 h-24 rounded-3xl bg-gradient-to-br from-white/30 to-white/10 dark:from-black/30 dark:to-black/10 backdrop-blur-sm mx-auto mb-4 flex items-center justify-center text-4xl font-bold text-white dark:text-zinc-900 shadow-2xl border border-white/20 dark:border-black/10">
                {user?.first_name?.[0]?.toUpperCase()}
              </div>
              {user?.role === 'admin' && (
                <div className="absolute -bottom-1 -right-1 w-8 h-8 bg-gradient-to-br from-amber-400 to-orange-500 rounded-xl flex items-center justify-center shadow-lg">
                  <Crown className="w-4 h-4 text-white" />
                </div>
              )}
            </div>
            <h1 className="text-2xl font-bold text-white dark:text-zinc-900 mb-1">
              {user?.first_name} {user?.last_name}
            </h1>
            {user?.username && (
              <p className="text-zinc-400 dark:text-zinc-600">@{user.username}</p>
            )}
          </motion.div>
        </div>
      </div>

      <div className="px-4 -mt-16 space-y-4">
        {/* Rank Card */}
        <motion.div
          className="bg-white dark:bg-zinc-900 rounded-2xl p-5 border border-zinc-200 dark:border-zinc-800 shadow-xl shadow-zinc-900/5 dark:shadow-black/20"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <div className="flex items-center gap-4 mb-4">
            <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-amber-100 to-orange-100 dark:from-amber-900/30 dark:to-orange-900/30 flex items-center justify-center text-2xl">
              {rank.emoji}
            </div>
            <div className="flex-1">
              <p className="text-xs text-tg-hint font-semibold uppercase tracking-wider mb-0.5">Ваш ранг</p>
              <p className="text-xl font-bold text-tg-text">{rank.name}</p>
            </div>
            <div className="text-right">
              <p className="text-3xl font-bold text-tg-text">{totalRequests}</p>
              <p className="text-xs text-tg-hint">заявок</p>
            </div>
          </div>
          {nextRank && (
            <div>
              <div className="flex justify-between text-xs font-medium mb-2">
                <span className="text-tg-hint">{rank.name}</span>
                <span className="text-tg-text">{nextRank.name} {nextRank.emoji}</span>
              </div>
              <div className="h-2.5 bg-zinc-100 dark:bg-zinc-800 rounded-full overflow-hidden">
                <motion.div
                  className="h-full bg-gradient-to-r from-amber-400 to-orange-500 rounded-full"
                  initial={{ width: 0 }}
                  animate={{ width: `${progress}%` }}
                  transition={{ duration: 1, ease: "easeOut" }}
                />
              </div>
              <p className="text-xs text-tg-hint mt-2 text-center">
                Ещё <span className="font-bold text-tg-text">{nextRank.min - totalRequests}</span> до следующего ранга
              </p>
            </div>
          )}
        </motion.div>

        {/* Stats Grid */}
        <motion.div
          className="grid grid-cols-2 gap-3"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
        >
          <StatCard icon={<FileText className="w-5 h-5" />} value={stats?.total_requests || 0} label="Всего" />
          <StatCard icon={<CheckCircle className="w-5 h-5" />} value={stats?.completed_requests || 0} label="Готово" accent="green" />
          <StatCard icon={<Clock className="w-5 h-5" />} value={stats?.pending_requests || 0} label="В работе" accent="blue" />
          <StatCard icon={<TrendingUp className="w-5 h-5" />} value={stats?.this_week || 0} label="За неделю" />
        </motion.div>

        {/* Contact Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
        >
          <p className="section-header">Контакт для связи</p>
          <div className="bg-white dark:bg-zinc-900 rounded-2xl border border-zinc-200 dark:border-zinc-800 shadow-sm overflow-hidden">
            <button
              onClick={() => {
                haptic?.impactOccurred('light')
                setContactValue(user?.contact || '')
                setEditContact(true)
              }}
              className="list-item w-full text-left group"
            >
              <div className="w-10 h-10 rounded-xl bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center">
                <Phone className="w-5 h-5 text-blue-600 dark:text-blue-400" />
              </div>
              <div className="flex-1">
                <p className="text-xs text-tg-hint font-medium mb-0.5">Телефон / Email</p>
                <p className="font-semibold text-tg-text">{user?.contact || 'Не указан'}</p>
              </div>
              <Edit2 className="w-4 h-4 text-tg-hint group-hover:text-tg-text transition-colors" />
            </button>
          </div>
        </motion.div>

        {/* Account Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
        >
          <p className="section-header">Аккаунт</p>
          <div className="bg-white dark:bg-zinc-900 rounded-2xl border border-zinc-200 dark:border-zinc-800 shadow-sm overflow-hidden">
            <div className="list-item">
              <div className="w-10 h-10 rounded-xl bg-zinc-100 dark:bg-zinc-800 flex items-center justify-center">
                <User className="w-5 h-5 text-tg-hint" />
              </div>
              <div className="flex-1">
                <p className="text-xs text-tg-hint font-medium mb-0.5">Telegram ID</p>
                <p className="font-mono text-tg-text">{user?.tg_id}</p>
              </div>
            </div>
            <div className="divider" />
            <div className="list-item">
              <div className="w-10 h-10 rounded-xl bg-purple-100 dark:bg-purple-900/30 flex items-center justify-center">
                {user?.role === 'admin' ? (
                  <Sparkles className="w-5 h-5 text-purple-600 dark:text-purple-400" />
                ) : (
                  <Star className="w-5 h-5 text-purple-600 dark:text-purple-400" />
                )}
              </div>
              <div className="flex-1">
                <p className="text-xs text-tg-hint font-medium mb-0.5">Роль</p>
                <p className="font-semibold text-tg-text">
                  {user?.role === 'admin' ? 'Администратор' : 'Менеджер'}
                </p>
              </div>
            </div>
            <div className="divider" />
            <button onClick={handleLogout} className="list-item w-full text-left group">
              <div className="w-10 h-10 rounded-xl bg-red-100 dark:bg-red-900/30 flex items-center justify-center">
                <LogOut className="w-5 h-5 text-red-600 dark:text-red-400" />
              </div>
              <span className="font-semibold text-red-600 dark:text-red-400">Выйти</span>
            </button>
          </div>
        </motion.div>
      </div>

      {/* Edit Contact Modal */}
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
              className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl z-50 safe-bottom"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
            >
              <div className="p-4">
                <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
                <div className="flex justify-between items-center mb-4">
                  <h3 className="text-lg font-bold text-tg-text">Контакт для связи</h3>
                  <button onClick={() => setEditContact(false)} className="p-2 rounded-full bg-tg-secondary-bg">
                    <X className="w-5 h-5 text-tg-hint" />
                  </button>
                </div>
                <p className="text-sm text-tg-hint mb-4">
                  Укажите телефон или email для связи с вами
                </p>
                <input
                  type="text"
                  value={contactValue}
                  onChange={(e) => setContactValue(e.target.value)}
                  placeholder="+7 999 999-99-99 или email@example.com"
                  className="input mb-4"
                  autoFocus
                />
                <button
                  onClick={() => updateMutation.mutate({ contact: contactValue })}
                  disabled={updateMutation.isPending}
                  className="btn btn-primary w-full"
                >
                  {updateMutation.isPending ? (
                    <Loader2 className="w-5 h-5 animate-spin" />
                  ) : (
                    'Сохранить'
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

function StatCard({ icon, value, label, accent }: {
  icon: React.ReactNode
  value: number
  label: string
  accent?: 'green' | 'blue' | 'orange'
}) {
  const accentClasses = {
    green: 'text-emerald-500',
    blue: 'text-blue-500',
    orange: 'text-orange-500',
  }

  return (
    <div className="bg-white dark:bg-zinc-900 rounded-2xl p-4 border border-zinc-200 dark:border-zinc-800 shadow-sm">
      <div className={`flex items-center gap-2 mb-2 ${accent ? accentClasses[accent] : 'text-tg-hint'}`}>
        {icon}
        <span className="text-xs font-semibold">{label}</span>
      </div>
      <p className={`text-3xl font-bold ${accent ? accentClasses[accent] : 'text-tg-text'}`}>{value}</p>
    </div>
  )
}
