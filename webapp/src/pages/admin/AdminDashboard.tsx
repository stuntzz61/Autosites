import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { adminApi } from '@/api/client'
import {
  Users, FileText, CheckCircle2, TrendingUp,
  Crown, Shield, Users2, ChevronRight, AlertCircle, Plus
} from 'lucide-react'
import { useAuthStore, isOwnerRole, isDirectorRole } from '@/stores/authStore'
import { motion } from 'framer-motion'

interface Director {
  id: string
  first_name: string
  last_name?: string
}

interface Supervisor {
  id: string
  first_name: string
  last_name?: string
}

export default function AdminDashboard() {
  const navigate = useNavigate()
  const { user } = useAuthStore()
  const isOwner = !!(user && isOwnerRole(user.role))
  const isDirector = !!(user && isDirectorRole(user.role))

  const { data: stats, isLoading } = useQuery({
    queryKey: ['admin', 'stats'],
    queryFn: () => adminApi.stats.overview().then(res => res.data),
  })

  const { data: directors = [] } = useQuery<Director[]>({
    queryKey: ['admin', 'directors'],
    queryFn: () => adminApi.directors.list().then(res => res.data),
    enabled: isOwner,
  })

  const { data: supervisors = [] } = useQuery<Supervisor[]>({
    queryKey: ['admin', 'supervisors'],
    queryFn: () => adminApi.supervisors.list().then(res => res.data),
    enabled: isDirector || isOwner,
  })

  const { data: pending = [] } = useQuery({
    queryKey: ['admin', 'pending'],
    queryFn: () => adminApi.pending.list().then(res => res.data),
  })

  if (isLoading) {
    return (
      <div className="p-4 space-y-4">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="skeleton h-24 rounded-2xl" />
        ))}
      </div>
    )
  }

  // Role-specific greeting
  const getRoleGreeting = () => {
    if (isOwner) return 'Владелец системы'
    if (isDirector) return 'Директор'
    return 'Супервайзер'
  }

  return (
    <div className="p-4 space-y-5">
      {/* Role-specific Header */}
      <motion.div
        className="flex items-center gap-4"
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
      >
        <div className={`w-14 h-14 rounded-2xl flex items-center justify-center ${
          isOwner ? 'bg-gradient-to-br from-yellow-400 to-orange-500' :
          isDirector ? 'bg-gradient-to-br from-yellow-400 to-amber-500' :
          'bg-gradient-to-br from-purple-500 to-indigo-600'
        }`}>
          {isOwner ? (
            <Crown className="w-7 h-7 text-white" />
          ) : isDirector ? (
            <Crown className="w-7 h-7 text-white" />
          ) : (
            <Shield className="w-7 h-7 text-white" />
          )}
        </div>
        <div>
          <h1 className="text-2xl font-bold text-tg-text">{user?.first_name}</h1>
          <p className="text-sm text-tg-hint">{getRoleGreeting()}</p>
        </div>
      </motion.div>

      {/* Pending Approvals Alert */}
      {pending.length > 0 && (
        <motion.button
          onClick={() => navigate('/admin/managers')}
          className="w-full bg-orange-500/10 border border-orange-500/20 rounded-2xl p-4 text-left"
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          whileTap={{ scale: 0.98 }}
        >
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-orange-500/20 flex items-center justify-center">
              <AlertCircle className="w-5 h-5 text-orange-500" />
            </div>
            <div className="flex-1">
              <p className="font-semibold text-orange-400">
                {pending.length} заявок на регистрацию
              </p>
              <p className="text-sm text-tg-hint">Требуется ваше решение</p>
            </div>
            <ChevronRight className="w-5 h-5 text-orange-500" />
          </div>
        </motion.button>
      )}

      {/* Owner-specific: Directors & Supervisors counts */}
      {isOwner && (
        <motion.div
          className="grid grid-cols-2 gap-3"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <button
            onClick={() => navigate('/admin/directors')}
            className="bg-gradient-to-br from-yellow-500/10 to-orange-500/10 border border-yellow-500/20 rounded-2xl p-4 text-left hover:scale-[1.02] transition-transform"
          >
            <div className="w-10 h-10 bg-yellow-500/20 rounded-xl flex items-center justify-center text-yellow-500 mb-3">
              <Crown className="w-5 h-5" />
            </div>
            <p className="text-2xl font-bold text-yellow-400">{directors.length}</p>
            <p className="text-sm text-tg-hint">Директоров</p>
          </button>
          <button
            onClick={() => navigate('/admin/supervisors')}
            className="bg-gradient-to-br from-purple-500/10 to-indigo-500/10 border border-purple-500/20 rounded-2xl p-4 text-left hover:scale-[1.02] transition-transform"
          >
            <div className="w-10 h-10 bg-purple-500/20 rounded-xl flex items-center justify-center text-purple-500 mb-3">
              <Shield className="w-5 h-5" />
            </div>
            <p className="text-2xl font-bold text-purple-400">{supervisors.length}</p>
            <p className="text-sm text-tg-hint">Супервайзеров</p>
          </button>
        </motion.div>
      )}

      {/* Director-specific: Supervisors count */}
      {isDirector && !isOwner && (
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <button
            onClick={() => navigate('/admin/supervisors')}
            className="w-full bg-gradient-to-br from-purple-500/10 to-indigo-500/10 border border-purple-500/20 rounded-2xl p-4 text-left hover:scale-[1.01] transition-transform"
          >
            <div className="flex items-center gap-4">
              <div className="w-12 h-12 bg-purple-500/20 rounded-xl flex items-center justify-center text-purple-500">
                <Shield className="w-6 h-6" />
              </div>
              <div className="flex-1">
                <p className="text-2xl font-bold text-purple-400">{supervisors.length}</p>
                <p className="text-sm text-tg-hint">Супервайзеров под управлением</p>
              </div>
              <ChevronRight className="w-5 h-5 text-purple-500" />
            </div>
          </button>
        </motion.div>
      )}

      {/* Main Stats Grid */}
      <motion.div
        className="grid grid-cols-2 gap-3"
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2 }}
      >
        <button
          onClick={() => navigate('/admin/managers')}
          className="bg-tg-secondary-bg rounded-2xl p-4 text-left hover:scale-[1.02] transition-transform"
        >
          <div className="w-10 h-10 bg-blue-500 rounded-xl flex items-center justify-center text-white mb-3">
            <Users className="w-5 h-5" />
          </div>
          <p className="text-2xl font-bold text-tg-text">{stats?.total_managers || 0}</p>
          <p className="text-sm text-tg-hint">Менеджеров</p>
        </button>
        <button
          onClick={() => navigate('/admin/groups')}
          className="bg-tg-secondary-bg rounded-2xl p-4 text-left hover:scale-[1.02] transition-transform"
        >
          <div className="w-10 h-10 bg-indigo-500 rounded-xl flex items-center justify-center text-white mb-3">
            <Users2 className="w-5 h-5" />
          </div>
          <p className="text-2xl font-bold text-tg-text">{stats?.total_groups || 0}</p>
          <p className="text-sm text-tg-hint">Групп</p>
        </button>
        <button
          onClick={() => navigate('/admin/requests')}
          className="bg-tg-secondary-bg rounded-2xl p-4 text-left hover:scale-[1.02] transition-transform"
        >
          <div className="w-10 h-10 bg-purple-500 rounded-xl flex items-center justify-center text-white mb-3">
            <FileText className="w-5 h-5" />
          </div>
          <p className="text-2xl font-bold text-tg-text">{stats?.total_requests || 0}</p>
          <p className="text-sm text-tg-hint">Всего заявок</p>
        </button>
        <div className="bg-tg-secondary-bg rounded-2xl p-4 text-left">
          <div className="w-10 h-10 bg-green-500 rounded-xl flex items-center justify-center text-white mb-3">
            <CheckCircle2 className="w-5 h-5" />
          </div>
          <p className="text-2xl font-bold text-tg-text">{stats?.completed_today || 0}</p>
          <p className="text-sm text-tg-hint">Сегодня готово</p>
        </div>
      </motion.div>

      {/* Quick Actions for Supervisors */}
      {!isDirector && (
        <motion.div
          className="space-y-3"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
        >
          <p className="text-sm font-medium text-tg-hint px-1">Быстрые действия</p>
          <button
            onClick={() => navigate('/admin/invite-codes')}
            className="w-full bg-tg-secondary-bg rounded-2xl p-4 text-left hover:bg-tg-secondary-bg/80 transition-colors"
          >
            <div className="flex items-center gap-4">
              <div className="w-10 h-10 bg-tg-accent/20 rounded-xl flex items-center justify-center text-tg-accent">
                <Plus className="w-5 h-5" />
              </div>
              <div className="flex-1">
                <p className="font-medium text-tg-text">Создать инвайт-код</p>
                <p className="text-sm text-tg-hint">Для регистрации нового менеджера</p>
              </div>
              <ChevronRight className="w-5 h-5 text-tg-hint" />
            </div>
          </button>
        </motion.div>
      )}

      {/* Activity Overview */}
      <motion.div
        className="bg-tg-secondary-bg rounded-2xl p-4"
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.4 }}
      >
        <div className="flex items-center gap-2 mb-4">
          <TrendingUp className="w-5 h-5 text-tg-accent" />
          <h2 className="font-semibold text-tg-text">Активность</h2>
        </div>
        <div className="grid grid-cols-3 gap-4 text-center">
          <div>
            <p className="text-xl font-bold text-tg-text">{stats?.requests_today || 0}</p>
            <p className="text-xs text-tg-hint">Сегодня</p>
          </div>
          <div>
            <p className="text-xl font-bold text-tg-text">{stats?.requests_this_week || 0}</p>
            <p className="text-xs text-tg-hint">За неделю</p>
          </div>
          <div>
            <p className="text-xl font-bold text-tg-text">{stats?.requests_this_month || 0}</p>
            <p className="text-xs text-tg-hint">За месяц</p>
          </div>
        </div>
      </motion.div>

      {/* Role-based Tips */}
      <motion.div
        className="bg-gradient-to-r from-tg-accent/10 to-purple-500/10 border border-tg-accent/20 rounded-2xl p-4"
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.5 }}
      >
        <p className="text-sm text-tg-text font-medium mb-1">
          {isOwner ? '💡 Совет владельца' : isDirector ? '💡 Совет директора' : '💡 Совет супервайзера'}
        </p>
        <p className="text-sm text-tg-hint">
          {isOwner
            ? 'Назначайте директоров для делегирования управления командами и повышения эффективности.'
            : isDirector
            ? 'Создавайте супервайзеров для управления группами менеджеров в разных направлениях.'
            : 'Используйте инвайт-коды с авто-одобрением для быстрого добавления проверенных менеджеров.'}
        </p>
      </motion.div>
    </div>
  )
}
