import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  User, Phone, Star, FileText, CheckCircle, Clock,
  TrendingUp, Edit2, X, Loader2, LogOut, Sparkles, Crown, Mail, Shield
} from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { profileApi } from '@/api/client'
import toast from 'react-hot-toast'

const ranks = [
  { min: 0, name: 'Новичок', color: 'from-slate-500 to-slate-400', ring: 'slate' },
  { min: 5, name: 'Начинающий', color: 'from-sky-500 to-blue-500', ring: 'blue' },
  { min: 15, name: 'Опытный', color: 'from-indigo-500 to-purple-500', ring: 'purple' },
  { min: 30, name: 'Профессионал', color: 'from-amber-400 to-orange-500', ring: 'gold' },
  { min: 50, name: 'Эксперт', color: 'from-cyan-400 to-blue-500', ring: 'premium' },
  { min: 100, name: 'Мастер', color: 'from-amber-300 to-yellow-500', ring: 'gold' },
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

  // Determine avatar ring class based on rank
  const getAvatarRingClass = () => {
    if (user?.role && ['owner', 'director'].includes(user.role)) return 'avatar-ring-gold'
    if (user?.role === 'supervisor') return 'avatar-ring-premium'
    if (rank.ring === 'gold') return 'avatar-ring-gold'
    if (rank.ring === 'premium') return 'avatar-ring-premium'
    return ''
  }

  return (
    <div className="min-h-screen pb-8" style={{ background: 'var(--bg-deep)' }}>
      {/* Header with gradient background */}
      <div
        className="relative px-5 pt-12 pb-24 overflow-hidden"
        style={{
          background: 'linear-gradient(180deg, var(--bg-deep) 0%, var(--bg-elevated) 100%)'
        }}
      >
        {/* Decorative background glow */}
        {(user?.role && ['owner', 'director', 'supervisor'].includes(user.role) || rank.ring === 'gold') && (
          <div
            className="absolute top-0 left-1/2 -translate-x-1/2 w-80 h-80 rounded-full blur-3xl opacity-15"
            style={{ background: 'var(--gold-primary)' }}
          />
        )}

        <motion.div
          className="text-center relative z-10"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
        >
          {/* Avatar with status ring */}
          <div className="relative inline-block mb-4">
            <div
              className={`w-24 h-24 rounded-2xl flex items-center justify-center text-3xl font-bold ${getAvatarRingClass()}`}
              style={{
                background: 'linear-gradient(135deg, var(--bg-surface) 0%, var(--bg-elevated) 100%)',
                border: '1px solid var(--border-accent)',
                color: 'var(--accent-primary-light)'
              }}
            >
              {user?.first_name?.[0]?.toUpperCase()}
            </div>

            {/* Role badge */}
            {user?.role && ['owner', 'director', 'supervisor'].includes(user.role) && (
              <motion.div
                className="absolute -bottom-1 -right-1 w-8 h-8 rounded-lg flex items-center justify-center"
                initial={{ scale: 0 }}
                animate={{ scale: 1 }}
                transition={{ delay: 0.2, type: 'spring' }}
                style={{
                  background: 'linear-gradient(135deg, var(--gold-light) 0%, var(--gold-primary) 100%)',
                  boxShadow: '0 4px 12px -2px var(--gold-glow)'
                }}
              >
                <Crown className="w-4 h-4 text-amber-900" />
              </motion.div>
            )}
          </div>

          <h1
            className="text-xl font-bold mb-0.5"
            style={{ color: 'var(--text-primary)' }}
          >
            {user?.full_name || `${user?.first_name || ''} ${user?.last_name || ''}`.trim() || 'Менеджер'}
          </h1>
          {user?.username && (
            <p className="text-sm" style={{ color: 'var(--text-subtle)' }}>
              @{user.username}
            </p>
          )}
        </motion.div>
      </div>

      {/* Content */}
      <div className="px-4 -mt-16 space-y-4 relative z-10">
        {/* Rank Card - Premium Design */}
        <motion.div
          className="rounded-2xl overflow-hidden"
          style={{
            background: 'var(--bg-surface)',
            border: '1px solid var(--border-default)',
            boxShadow: '0 4px 24px rgba(0, 0, 0, 0.25)'
          }}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          {/* Rank header with gradient */}
          <div
            className={`relative bg-gradient-to-r ${rank.color} p-5 overflow-hidden`}
          >
            {/* Shine effect */}
            <div
              className="absolute inset-0 opacity-20"
              style={{
                background: 'linear-gradient(45deg, transparent 30%, rgba(255,255,255,0.3) 50%, transparent 70%)',
                backgroundSize: '200% 200%',
                animation: 'shimmer 3s ease-in-out infinite'
              }}
            />

            <div className="relative flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 rounded-xl bg-white/15 backdrop-blur-sm flex items-center justify-center border border-white/20">
                  <span className="text-sm font-bold tracking-wide text-white/90">LVL</span>
                </div>
                <div>
                  <p className="text-white/70 text-xs font-medium uppercase tracking-wider">Ваш ранг</p>
                  <p className="text-white text-lg font-bold">{rank.name}</p>
                </div>
              </div>
              <div className="text-right">
                <p className="text-white text-3xl font-bold number-animate">{totalRequests}</p>
                <p className="text-white/70 text-xs font-medium">заявок</p>
              </div>
            </div>
          </div>

          {/* Progress section */}
          {nextRank && (
            <div className="p-5" style={{ background: 'var(--bg-elevated)' }}>
              <div className="flex justify-between text-xs font-semibold mb-3">
                <span style={{ color: 'var(--text-subtle)' }}>{rank.name}</span>
                <span style={{ color: 'var(--text-primary)' }}>{nextRank.name}</span>
              </div>

              {/* Premium progress bar */}
              <div className={`progress-bar ${rank.ring === 'gold' ? 'progress-bar-gold' : ''}`}>
                <motion.div
                  className="progress-bar-fill"
                  initial={{ width: 0 }}
                  animate={{ width: `${progress}%` }}
                  transition={{ duration: 1, ease: [0.4, 0, 0.2, 1] }}
                  style={rank.ring === 'gold' ? {
                    background: `linear-gradient(90deg, var(--gold-dark) 0%, var(--gold-primary) 50%, var(--gold-light) 100%)`
                  } : {}}
                />
              </div>

              <p className="text-xs mt-3 text-center" style={{ color: 'var(--text-subtle)' }}>
                Ещё <span className="font-bold" style={{ color: 'var(--text-primary)' }}>{nextRank.min - totalRequests}</span> до следующего ранга
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
            accent="gold"
          />
        </motion.div>

        {/* Contact Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
        >
          <p className="section-header">Контактная информация</p>
          <div
            className="rounded-2xl overflow-hidden space-y-1"
            style={{
              background: 'var(--bg-surface)',
              border: '1px solid var(--border-default)'
            }}
          >
            {/* Phone */}
            {user?.phone && (
              <div className="flex items-center gap-3 p-4">
                <div
                  className="w-11 h-11 rounded-xl flex items-center justify-center"
                  style={{
                    background: 'rgba(59, 130, 246, 0.12)',
                    border: '1px solid var(--border-accent)'
                  }}
                >
                  <Phone className="w-5 h-5" style={{ color: 'var(--accent-primary-light)' }} />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-xs mb-0.5" style={{ color: 'var(--text-subtle)' }}>
                    Телефон
                  </p>
                  <p className="font-semibold truncate" style={{ color: 'var(--text-primary)' }}>
                    {user.phone}
                  </p>
                </div>
              </div>
            )}

            {/* Email */}
            {user?.email && (
              <div
                className="flex items-center gap-3 p-4"
                style={{ borderTop: user?.phone ? '1px solid var(--border-subtle)' : 'none' }}
              >
                <div
                  className="w-11 h-11 rounded-xl flex items-center justify-center"
                  style={{
                    background: 'rgba(16, 185, 129, 0.12)',
                    border: '1px solid rgba(16, 185, 129, 0.3)'
                  }}
                >
                  <Mail className="w-5 h-5" style={{ color: 'var(--success)' }} />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-xs mb-0.5" style={{ color: 'var(--text-subtle)' }}>
                    Email
                  </p>
                  <p className="font-semibold truncate" style={{ color: 'var(--text-primary)' }}>
                    {user.email}
                  </p>
                </div>
              </div>
            )}

            {/* Fallback if no phone/email */}
            {!user?.phone && !user?.email && (
              <div className="flex items-center gap-3 p-4">
                <div
                  className="w-11 h-11 rounded-xl flex items-center justify-center"
                  style={{
                    background: 'rgba(59, 130, 246, 0.12)',
                    border: '1px solid var(--border-accent)'
                  }}
                >
                  <Phone className="w-5 h-5" style={{ color: 'var(--accent-primary-light)' }} />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-xs mb-0.5" style={{ color: 'var(--text-subtle)' }}>
                    Контакт не указан
                  </p>
                </div>
              </div>
            )}
          </div>
        </motion.div>

        {/* Account Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
        >
          <p className="section-header">Аккаунт</p>
          <div
            className="rounded-2xl overflow-hidden"
            style={{
              background: 'var(--bg-surface)',
              border: '1px solid var(--border-default)'
            }}
          >
            {/* Telegram ID */}
            <div
              className="flex items-center gap-3 p-4"
              style={{ borderBottom: '1px solid var(--border-subtle)' }}
            >
              <div
                className="w-11 h-11 rounded-xl flex items-center justify-center"
                style={{ background: 'var(--bg-tertiary)' }}
              >
                <User className="w-5 h-5" style={{ color: 'var(--text-muted)' }} />
              </div>
              <div className="flex-1">
                <p className="text-xs mb-0.5" style={{ color: 'var(--text-subtle)' }}>
                  Telegram ID
                </p>
                <p className="font-mono text-sm" style={{ color: 'var(--text-primary)' }}>
                  {user?.tg_id}
                </p>
              </div>
            </div>

            {/* Role */}
            <div
              className="flex items-center gap-3 p-4"
              style={{ borderBottom: '1px solid var(--border-subtle)' }}
            >
              <div
                className="w-11 h-11 rounded-xl flex items-center justify-center"
                style={{
                  background: user?.role === 'owner'
                    ? 'linear-gradient(135deg, rgba(245, 158, 11, 0.2) 0%, rgba(245, 158, 11, 0.15) 100%)'
                    : user?.role === 'director'
                    ? 'linear-gradient(135deg, rgba(251, 191, 36, 0.15) 0%, rgba(251, 191, 36, 0.1) 100%)'
                    : user?.role === 'supervisor'
                    ? 'linear-gradient(135deg, rgba(245, 158, 11, 0.15) 0%, rgba(245, 158, 11, 0.1) 100%)'
                    : 'rgba(139, 92, 246, 0.12)'
                }}
              >
                {user?.role === 'owner' ? (
                  <Crown className="w-5 h-5" style={{ color: 'var(--gold-primary)' }} />
                ) : user?.role === 'director' ? (
                  <Shield className="w-5 h-5" style={{ color: '#fbbf24' }} />
                ) : user?.role === 'supervisor' ? (
                  <Sparkles className="w-5 h-5" style={{ color: 'var(--gold-primary)' }} />
                ) : (
                  <Star className="w-5 h-5" style={{ color: 'var(--info-light)' }} />
                )}
              </div>
              <div className="flex-1">
                <p className="text-xs mb-0.5" style={{ color: 'var(--text-subtle)' }}>Роль</p>
                <div className="flex items-center gap-2">
                  <p className="font-semibold" style={{ color: 'var(--text-primary)' }}>
                    {user?.role === 'owner' ? 'Владелец' :
                     user?.role === 'director' ? 'Директор' :
                     user?.role === 'supervisor' ? 'Супервайзер' : 'Менеджер'}
                  </p>
                  {user?.role === 'owner' && (
                    <span className="badge-premium text-[9px] px-2 py-1">OWNER</span>
                  )}
                  {user?.role === 'director' && (
                    <span className="text-[9px] px-2 py-1 bg-yellow-500/20 text-yellow-400 rounded">DIRECTOR</span>
                  )}
                  {user?.role === 'supervisor' && (
                    <span className="badge-premium text-[9px] px-2 py-1">SUPERVISOR</span>
                  )}
                </div>
              </div>
            </div>

            {/* Logout button */}
            <button
              onClick={handleLogout}
              className="flex items-center gap-3 p-4 w-full text-left transition-all hover:bg-red-500/5 active:scale-[0.99]"
            >
              <div
                className="w-11 h-11 rounded-xl flex items-center justify-center"
                style={{ background: 'var(--error-bg)' }}
              >
                <LogOut className="w-5 h-5" style={{ color: 'var(--error-light)' }} />
              </div>
              <span className="font-semibold" style={{ color: 'var(--error-light)' }}>
                Выйти из аккаунта
              </span>
            </button>
          </div>
        </motion.div>
      </div>

      {/* Edit Contact Modal */}
      <AnimatePresence>
        {editContact && (
          <>
            <motion.div
              className="fixed inset-0 z-40"
              style={{ background: 'rgba(0, 0, 0, 0.7)' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setEditContact(false)}
            />
            <motion.div
              className="fixed bottom-0 left-0 right-0 rounded-t-3xl z-50 safe-bottom"
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
              <div className="p-5">
                {/* Drag handle */}
                <div
                  className="w-10 h-1 rounded-full mx-auto mb-5"
                  style={{ background: 'var(--bg-tertiary)' }}
                />

                <div className="flex justify-between items-center mb-5">
                  <h3 className="text-lg font-bold" style={{ color: 'var(--text-primary)' }}>
                    Контакт для связи
                  </h3>
                  <button
                    onClick={() => setEditContact(false)}
                    className="w-8 h-8 rounded-full flex items-center justify-center transition-colors hover:bg-white/5"
                    style={{ background: 'var(--bg-tertiary)' }}
                  >
                    <X className="w-4 h-4" style={{ color: 'var(--text-muted)' }} />
                  </button>
                </div>

                <p className="text-sm mb-4" style={{ color: 'var(--text-subtle)' }}>
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
                  className="btn btn-primary btn-lg w-full"
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

interface StatCardProps {
  icon: React.ReactNode
  value: number
  label: string
  accent?: 'emerald' | 'blue' | 'gold'
}

function StatCard({ icon, value, label, accent }: StatCardProps) {
  const colors = {
    emerald: {
      bg: 'rgba(16, 185, 129, 0.12)',
      color: 'var(--success)',
      value: 'var(--success-light)'
    },
    blue: {
      bg: 'rgba(59, 130, 246, 0.12)',
      color: 'var(--accent-primary)',
      value: 'var(--accent-primary-light)'
    },
    gold: {
      bg: 'rgba(245, 158, 11, 0.12)',
      color: 'var(--gold-primary)',
      value: 'var(--gold-light)'
    },
  }

  const color = accent ? colors[accent] : {
    bg: 'var(--bg-tertiary)',
    color: 'var(--text-muted)',
    value: 'var(--text-primary)'
  }

  return (
    <div
      className="rounded-2xl p-4"
      style={{
        background: 'var(--bg-surface)',
        border: '1px solid var(--border-default)'
      }}
    >
      <div
        className="inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-semibold mb-2"
        style={{
          background: color.bg,
          color: color.color
        }}
      >
        {icon}
        {label}
      </div>
      <p className="text-2xl font-bold number-animate" style={{ color: color.value }}>
        {value}
      </p>
    </div>
  )
}
