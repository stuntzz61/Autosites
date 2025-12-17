import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { User, Loader2, Shield } from 'lucide-react'
import { useAuthStore } from '@/stores/authStore'
import { api } from '@/api/client'
import toast from 'react-hot-toast'

export default function DevLoginPage() {
  const navigate = useNavigate()
  const [tgId, setTgId] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!tgId.trim()) {
      setError('Введите Telegram ID')
      return
    }

    const userId = parseInt(tgId)
    if (isNaN(userId)) {
      setError('Telegram ID должен быть числом')
      return
    }

    setIsLoading(true)
    setError('')

    try {
      // Use dev-login endpoint
      const response = await api.post('/auth/dev-login', {
        tg_id: userId
      })

      if (response.data.user) {
        // Manually set user in store (bypass Telegram init)
        useAuthStore.setState({
          user: response.data.user,
          isAdmin: ['supervisor', 'director', 'owner'].includes(response.data.user.role),
          isLoading: false,
        })

        toast.success('Вход выполнен!')
        navigate('/')
      }
    } catch (err: any) {
      console.error('Dev login error:', err)
      const errorMsg = err.response?.data?.detail || 'Не удалось войти'
      setError(errorMsg)

      if (errorMsg.includes('DEBUG')) {
        toast.error('Включите DEBUG режим в API')
      }
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex flex-col bg-tg-bg p-6">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="max-w-md w-full mx-auto mt-20"
      >
        {/* Header */}
        <div className="text-center mb-8">
          <div className="w-20 h-20 rounded-2xl bg-black dark:bg-white flex items-center justify-center mx-auto mb-4">
            <User className="w-10 h-10 text-white dark:text-black" />
          </div>
          <h1 className="text-2xl font-bold text-tg-text mb-2">
            Dev Login
          </h1>
          <p className="text-tg-hint text-sm">
            Тестовый вход для разработки
          </p>
        </div>

        {/* Form */}
        <form onSubmit={handleLogin} className="space-y-4">
          <div>
            <label className="text-sm text-tg-hint mb-2 block">
              Telegram ID
            </label>
            <input
              type="text"
              value={tgId}
              onChange={(e) => {
                setTgId(e.target.value)
                setError('')
              }}
              placeholder="852297440"
              className="input w-full"
              autoFocus
            />
            <p className="text-xs text-tg-hint mt-1">
              Введите ваш Telegram ID (можно найти через @userinfobot)
            </p>
          </div>

          {error && (
            <motion.p
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="text-red-500 text-sm"
            >
              {error}
            </motion.p>
          )}

          <button
            type="submit"
            disabled={isLoading}
            className="btn btn-primary w-full py-4"
          >
            {isLoading ? (
              <Loader2 className="w-5 h-5 animate-spin" />
            ) : (
              'Войти'
            )}
          </button>
        </form>

        {/* Admin Login Link */}
        <div className="mt-6 text-center">
          <button
            onClick={() => navigate('/admin-login')}
            className="text-sm text-tg-hint hover:text-tg-text flex items-center justify-center gap-2"
          >
            <Shield className="w-4 h-4" />
            Войти как админ
          </button>
        </div>

        {/* Info */}
        <div className="mt-8 p-4 bg-tg-secondary-bg rounded-xl">
          <p className="text-xs text-tg-hint">
            <strong>Примечание:</strong> Для работы в браузере нужен пользователь в БД.
            Если пользователя нет, используйте админ-вход или создайте пользователя через бота.
          </p>
        </div>
      </motion.div>
    </div>
  )
}

