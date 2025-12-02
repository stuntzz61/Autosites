import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Shield, Lock, Eye, EyeOff, Loader2, ArrowLeft } from 'lucide-react'
import { useAuthStore } from '@/stores/authStore'
import { useTelegram } from '@/contexts/TelegramContext'
import { api } from '@/api/client'
import toast from 'react-hot-toast'

export default function AdminLoginPage() {
  const navigate = useNavigate()
  const { haptic } = useTelegram()
  const { setAdmin } = useAuthStore()
  const [password, setPassword] = useState('')
  const [showPassword, setShowPassword] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!password.trim()) {
      setError('Введите пароль')
      return
    }

    setIsLoading(true)
    setError('')

    try {
      const response = await api.post('/auth/admin-login', { password })

      if (response.data.success) {
        setAdmin(true)
        haptic?.notificationOccurred('success')
        toast.success('Добро пожаловать, администратор!')
        navigate('/admin')
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Неверный пароль')
      haptic?.notificationOccurred('error')
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex flex-col bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900">
      {/* Background pattern */}
      <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxnIGZpbGw9IiNmZmYiIGZpbGwtb3BhY2l0eT0iMC4wMyI+PHBhdGggZD0iTTM2IDM0djItSDI0di0yaDEyek0zNiAyNHYySDE0di0yaDIyek0zNiAxNHYySDRWMTRoMzJ6Ii8+PC9nPjwvZz48L3N2Zz4=')] opacity-50" />

      {/* Header */}
      <div className="relative z-10 p-4">
        <button
          onClick={() => navigate(-1)}
          className="flex items-center gap-2 text-white/70 hover:text-white transition-colors"
        >
          <ArrowLeft className="w-5 h-5" />
          <span>Назад</span>
        </button>
      </div>

      {/* Content */}
      <div className="relative z-10 flex-1 flex items-center justify-center p-6">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="w-full max-w-sm"
        >
          {/* Icon */}
          <motion.div
            initial={{ scale: 0.8 }}
            animate={{ scale: 1 }}
            transition={{ type: 'spring', delay: 0.1 }}
            className="flex justify-center mb-8"
          >
            <div className="w-20 h-20 rounded-3xl bg-gradient-to-br from-amber-400 to-orange-500 flex items-center justify-center shadow-lg shadow-orange-500/30">
              <Shield className="w-10 h-10 text-white" />
            </div>
          </motion.div>

          {/* Title */}
          <div className="text-center mb-8">
            <h1 className="text-2xl font-bold text-white mb-2">Вход в админку</h1>
            <p className="text-white/60 text-sm">
              Введите пароль администратора для доступа к панели управления
            </p>
          </div>

          {/* Form */}
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="relative">
              <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-white/40" />
              <input
                type={showPassword ? 'text' : 'password'}
                value={password}
                onChange={(e) => {
                  setPassword(e.target.value)
                  setError('')
                }}
                placeholder="Пароль"
                className="w-full bg-white/10 backdrop-blur border border-white/10 rounded-2xl pl-12 pr-12 py-4 text-white placeholder:text-white/40 focus:outline-none focus:ring-2 focus:ring-amber-500/50 focus:border-amber-500/50 transition-all"
                autoFocus
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute right-4 top-1/2 -translate-y-1/2 text-white/40 hover:text-white/60 transition-colors"
              >
                {showPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
              </button>
            </div>

            {error && (
              <motion.p
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                className="text-red-400 text-sm text-center"
              >
                {error}
              </motion.p>
            )}

            <button
              type="submit"
              disabled={isLoading}
              className="w-full bg-gradient-to-r from-amber-500 to-orange-500 hover:from-amber-600 hover:to-orange-600 text-white font-semibold py-4 rounded-2xl flex items-center justify-center gap-2 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-orange-500/25"
            >
              {isLoading ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <>
                  <Shield className="w-5 h-5" />
                  Войти
                </>
              )}
            </button>
          </form>

          {/* Info */}
          <p className="text-white/40 text-xs text-center mt-6">
            Доступ только для администраторов системы
          </p>
        </motion.div>
      </div>
    </div>
  )
}

