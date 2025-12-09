import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  User, Phone, Star, FileText, CheckCircle, Clock,
  TrendingUp, Edit2, X, Loader2, LogOut, Sparkles, Crown
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { profileApi } from '@/api/client'
import toast from 'react-hot-toast'

const ranks = [
  { min: 0, name: 'Новичок', emoji: '🌱', color: 'from-zinc-400 to-zinc-500' },
  { min: 5, name: 'Начинающий', emoji: '🌿', color: 'from-emerald-400 to-emerald-600' },
  { min: 15, name: 'Опытный', emoji: '🌲', color: 'from-green-400 to-green-600' },
  { min: 30, name: 'Профессионал', emoji: '⭐', color: 'from-amber-400 to-orange-500' },
  { min: 50, name: 'Эксперт', emoji: '💎', color: 'from-cyan-400 to-blue-500' },
  { min: 100, name: 'Мастер', emoji: '👑', color: 'from-amber-300 to-amber-500' },
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
      {/* Header */}
      <div className="bg-zinc-900 dark:bg-zinc-100 px-5 pt-10 pb-24">
        <motion.div
          className="text-center"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
        >
          <div className="relative inline-block mb-3">
            <div className="w-20 h-20 rounded-2xl bg-white/10 dark:bg-black/5 backdrop-blur-sm flex items-center justify-center text-3xl font-bold text-white dark:text-zinc-900 border border-white/20 dark:border-black/10">
              {user?.first_name?.[0]?.toUpperCase()}
            </div>
            {user?.role === 'admin' && (
              <div className="absolute -bottom-1 -right-1 w-7 h-7 bg-gradient-to-br from-amber-400 to-orange-500 rounded-lg flex items-center justify-center shadow-lg">
                <Crown className="w-3.5 h-3.5 text-white" />
              </div>
            )}
          </div>
          <h1 className="text-xl font-bold text-white dark:text-zinc-900 mb-0.5">
            {user?.first_name} {user?.last_name}
          </h1>
          {user?.username && (
            <p className="text-sm text-zinc-400 dark:text-zinc-600">@{user.username}</p>
          )}
        </motion.div>
      </div>

      {/* Content */}
      <div className="px-4 -mt-16 space-y-4 relative z-10">
        {/* Rank Card */}
        <motion.div
          className="bg-white dark:bg-zinc-900 rounded-2xl overflow-hidden border border-zinc-200 dark:border-zinc-800 shadow-xl"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <div className={`bg-gradient-to-r ${rank.color} p-4`}>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <span className="text-3xl">{rank.emoji}</span>
                <div>
                  <p className="text-white/70 text-xs font-medium">Ваш ранг</p>
                  <p className="text-white text-lg font-bold">{rank.name}</p>
                </div>
              </div>
              <div className="text-right">
                <p className="text-white text-2xl font-bold">{totalRequests}</p>
                <p className="text-white/70 text-xs">заявок</p>
              </div>
            </div>
          </div>
          
          {nextRank && (
            <div className="p-4 bg-white dark:bg-zinc-900">
              <div className="flex justify-between text-xs font-medium mb-2">
                <span className="text-tg-hint">{rank.name}</span>
                <span className="text-tg-text">{nextRank.name} {nextRank.emoji}</span>
              </div>
              <div className="h-2 bg-zinc-100 dark:bg-zinc-800 rounded-full overflow-hidden">
                <motion.div
                  className={`h-full bg-gradient-to-r ${rank.color} rounded-full`}
                  initial={{ width: 0 }}
                  animate={{ width: `${progress}%` }}
                  transition={{ duration: 0.8, ease: "easeOut" }}
                />
              </div>
              <p className="text-xs text-tg-hint mt-2 text-center">
                Ещё <span className="font-bold text-tg-text">{nextRank.min - totalRequests}</span> до следующего
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
          <StatCard 
            icon={<FileText className="w-4 h-4" />} 
            value={stats?.total_requests || 0} 
            label="Всего" 
          />
          <StatCard 
            icon={<CheckCircle className="w-4 h-4" />} 
            value={stats?.completed_requests || 0} 
            label="Готово" 
            accent="emerald" 
          />
          <StatCard 
            icon={<Clock className="w-4 h-4" />} 
            value={stats?.pending_requests || 0} 
            label="В работе" 
            accent="blue" 
          />
          <StatCard 
            icon={<TrendingUp className="w-4 h-4" />} 
            value={stats?.this_week || 0} 
            label="За неделю" 
            accent="amber"
          />
        </motion.div>

        {/* Contact Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
        >
          <p className="section-header">Контакт</p>
          <div className="bg-white dark:bg-zinc-900 rounded-2xl border border-zinc-200 dark:border-zinc-800 overflow-hidden">
            <button
              onClick={() => {
                haptic?.impactOccurred('light')
                setContactValue(user?.contact || '')
                setEditContact(true)
              }}
              className="flex items-center gap-3 p-4 w-full text-left"
            >
              <div className="w-10 h-10 rounded-xl bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center">
                <Phone className="w-5 h-5 text-blue-500" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-xs text-tg-hint mb-0.5">Телефон / Email</p>
                <p className="font-semibold text-tg-text truncate">{user?.contact || 'Не указан'}</p>
              </div>
              <Edit2 className="w-4 h-4 text-tg-hint shrink-0" />
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
          <div className="bg-white dark:bg-zinc-900 rounded-2xl border border-zinc-200 dark:border-zinc-800 overflow-hidden divide-y divide-zinc-100 dark:divide-zinc-800">
            <div className="flex items-center gap-3 p-4">
              <div className="w-10 h-10 rounded-xl bg-zinc-100 dark:bg-zinc-800 flex items-center justify-center">
                <User className="w-5 h-5 text-tg-hint" />
              </div>
              <div className="flex-1">
                <p className="text-xs text-tg-hint mb-0.5">Telegram ID</p>
                <p className="font-mono text-sm text-tg-text">{user?.tg_id}</p>
              </div>
            </div>
            
            <div className="flex items-center gap-3 p-4">
              <div className="w-10 h-10 rounded-xl bg-purple-100 dark:bg-purple-900/30 flex items-center justify-center">
                {user?.role === 'admin' ? (
                  <Sparkles className="w-5 h-5 text-purple-500" />
                ) : (
                  <Star className="w-5 h-5 text-purple-500" />
                )}
              </div>
              <div className="flex-1">
                <p className="text-xs text-tg-hint mb-0.5">Роль</p>
                <p className="font-semibold text-tg-text">
                  {user?.role === 'admin' ? 'Администратор' : 'Менеджер'}
                </p>
              </div>
            </div>
            
            <button 
              onClick={handleLogout} 
              className="flex items-center gap-3 p-4 w-full text-left"
            >
              <div className="w-10 h-10 rounded-xl bg-red-100 dark:bg-red-900/30 flex items-center justify-center">
                <LogOut className="w-5 h-5 text-red-500" />
              </div>
              <span className="font-semibold text-red-500">Выйти из аккаунта</span>
            </button>
          </div>
        </motion.div>
      </div>

      {/* Edit Contact Modal */}
      <AnimatePresence>
        {editContact && (
          <>
            <motion.div
              className="fixed inset-0 bg-black/60 z-40"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setEditContact(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 bg-white dark:bg-zinc-900 rounded-t-3xl z-50 safe-bottom"
              initial={{ y: '100%' }}
              animate={{ y: 0 }}
              exit={{ y: '100%' }}
              transition={{ type: 'spring', damping: 30, stiffness: 400 }}
            >
              <div className="p-5">
                <div className="w-10 h-1 bg-zinc-300 dark:bg-zinc-700 rounded-full mx-auto mb-5" />
                
                <div className="flex justify-between items-center mb-5">
                  <h3 className="text-lg font-bold text-tg-text">Контакт для связи</h3>
                  <button 
                    onClick={() => setEditContact(false)} 
                    className="w-8 h-8 rounded-full bg-zinc-100 dark:bg-zinc-800 flex items-center justify-center"
                  >
                    <X className="w-4 h-4 text-tg-hint" />
                  </button>
                </div>
                
                <p className="text-sm text-tg-hint mb-4">
                  Укажите телефон или email для связи
                </p>
                
                <input
                  type="text"
                  value={contactValue}
                  onChange={(e) => setContactValue(e.target.value)}
                  placeholder="+7 999 999-99-99"
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
  accent?: 'emerald' | 'blue' | 'amber'
}) {
  const colors = {
    emerald: 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-400',
    blue: 'bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400',
    amber: 'bg-amber-100 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400',
  }

  return (
    <div className="bg-white dark:bg-zinc-900 rounded-2xl p-4 border border-zinc-200 dark:border-zinc-800">
      <div className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-lg text-xs font-semibold mb-2 ${accent ? colors[accent] : 'bg-zinc-100 dark:bg-zinc-800 text-tg-hint'}`}>
        {icon}
        {label}
      </div>
      <p className={`text-2xl font-bold ${accent ? colors[accent].split(' ')[2] : 'text-tg-text'}`}>{value}</p>
    </div>
  )
}
