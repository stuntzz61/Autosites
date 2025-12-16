import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import {
  User, Phone, Mail, CheckCircle2, AlertCircle, Loader2,
  ArrowRight
} from 'lucide-react'
import { useMutation } from '@tanstack/react-query'
import { managerApi } from '@/api/client'
import toast from 'react-hot-toast'
import { useAuthStore } from '@/stores/authStore'
import { useNavigate } from 'react-router-dom'

export default function ManagerRegistrationPage() {
  const navigate = useNavigate()
  const { user, refreshUser } = useAuthStore()

  const [fullName, setFullName] = useState('')
  const [phone, setPhone] = useState('')
  const [email, setEmail] = useState('')
  const [agreeTerms, setAgreeTerms] = useState(true)
  const [errors, setErrors] = useState<{ fullName?: string; phone?: string; email?: string }>({})

  // Check if user needs registration
  useEffect(() => {
    if (user) {
      // If already registered, redirect to home
      if (user.registration_completed_at) {
        navigate('/')
        return
      }
      // If not approved, show message
      if (user.approval_status !== 'approved') {
        // Will be handled by App.tsx
        return
      }
    }
  }, [user, navigate])

  // Registration mutation
  const registerMutation = useMutation({
    mutationFn: async () => {
      return managerApi.register({
        full_name: fullName.trim(),
        phone: phone,
        email: email.trim(),
        agree_terms: agreeTerms
      })
    },
    onMutate: () => {
      setErrors({})
    },
    onSuccess: async (res) => {
      const data = res.data
      toast.success('Регистрация успешно завершена!')
      // Refresh user data
      await refreshUser()
      // Navigate to home
      navigate('/')
    },
    onError: (err: unknown) => {
      const error = err as { response?: { data?: { detail?: string | Array<{ loc?: string[]; msg?: string }> } } }
      const detail = error.response?.data?.detail
      if (typeof detail === 'string') {
        toast.error(detail)
      } else if (Array.isArray(detail)) {
        // Pydantic validation errors
        const newErrors: { fullName?: string; phone?: string; email?: string } = {}
        detail.forEach((e) => {
          if (e.loc?.includes('full_name')) {
            newErrors.fullName = e.msg || 'Ошибка валидации ФИО'
          }
          if (e.loc?.includes('phone')) {
            newErrors.phone = e.msg || 'Ошибка валидации телефона'
          }
          if (e.loc?.includes('email')) {
            newErrors.email = e.msg || 'Ошибка валидации email'
          }
        })
        setErrors(newErrors)
      } else {
        toast.error('Ошибка регистрации. Попробуйте ещё раз.')
      }
      console.error('Registration error:', err)
    }
  })

  // Phone formatting
  const formatPhone = (value: string) => {
    // Remove non-digits
    let digits = value.replace(/\D/g, '')

    // Handle Russian phone format
    if (digits.startsWith('8')) {
      digits = '7' + digits.slice(1)
    }
    if (!digits.startsWith('7') && digits.length > 0) {
      digits = '7' + digits
    }

    // Format: +7 (XXX) XXX-XX-XX
    if (digits.length === 0) return ''
    if (digits.length <= 1) return `+${digits}`
    if (digits.length <= 4) return `+${digits.slice(0, 1)} (${digits.slice(1)}`
    if (digits.length <= 7) return `+${digits.slice(0, 1)} (${digits.slice(1, 4)}) ${digits.slice(4)}`
    if (digits.length <= 9) return `+${digits.slice(0, 1)} (${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`
    return `+${digits.slice(0, 1)} (${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7, 9)}-${digits.slice(9, 11)}`
  }

  const handlePhoneChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const formatted = formatPhone(e.target.value)
    setPhone(formatted)
  }

  // Validation
  const validateForm = () => {
    const newErrors: { fullName?: string; phone?: string; email?: string } = {}

    if (!fullName.trim()) {
      newErrors.fullName = 'Укажите ФИО'
    } else if (fullName.trim().split(/\s+/).length < 2) {
      newErrors.fullName = 'Укажите полное ФИО (имя и фамилию)'
    }

    const phoneDigits = phone.replace(/\D/g, '')
    if (phoneDigits.length !== 11) {
      newErrors.phone = 'Укажите полный номер телефона'
    }

    const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/
    if (!email.trim()) {
      newErrors.email = 'Укажите email'
    } else if (!emailRegex.test(email.trim())) {
      newErrors.email = 'Некорректный формат email'
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (validateForm()) {
      registerMutation.mutate()
    }
  }

  // Loading state
  if (!user) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-indigo-50 via-white to-purple-50 dark:from-zinc-900 dark:via-zinc-900 dark:to-indigo-950 flex items-center justify-center p-4">
        <div className="text-center">
          <Loader2 className="w-12 h-12 text-indigo-500 animate-spin mx-auto mb-4" />
          <p className="text-zinc-600 dark:text-zinc-400">Загрузка...</p>
        </div>
      </div>
    )
  }

  // Registration form
  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-50 via-white to-purple-50 dark:from-zinc-900 dark:via-zinc-900 dark:to-indigo-950 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="max-w-md w-full bg-white dark:bg-zinc-800 rounded-3xl shadow-2xl overflow-hidden"
      >
        {/* Header */}
        <div className="bg-gradient-to-r from-indigo-500 to-purple-500 p-6 text-white text-center">
          <motion.div
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            transition={{ type: "spring", delay: 0.2 }}
            className="w-16 h-16 bg-white/20 backdrop-blur rounded-2xl flex items-center justify-center mx-auto mb-4"
          >
            <User className="w-8 h-8" />
          </motion.div>
          <h1 className="text-2xl font-bold mb-2">Завершите регистрацию</h1>
          <p className="text-white/80 text-sm">
            Ваша заявка одобрена! Заполните форму для доступа к системе
          </p>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="p-6 space-y-5">
          {/* Full Name */}
          <div>
            <label className="block text-sm font-medium text-zinc-700 dark:text-zinc-300 mb-2">
              ФИО *
            </label>
            <div className="relative">
              <User className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-zinc-400" />
              <input
                type="text"
                value={fullName}
                onChange={(e) => setFullName(e.target.value)}
                placeholder="Иванов Иван Иванович"
                className={`w-full pl-12 pr-4 py-3 rounded-xl border-2 ${
                  errors.fullName
                    ? 'border-red-300 dark:border-red-600 focus:border-red-500'
                    : 'border-zinc-200 dark:border-zinc-600 focus:border-indigo-500'
                } bg-white dark:bg-zinc-700 text-zinc-900 dark:text-white placeholder-zinc-400 dark:placeholder-zinc-500 outline-none transition-colors`}
                disabled={registerMutation.isPending}
              />
            </div>
            {errors.fullName && (
              <p className="mt-1 text-sm text-red-500">{errors.fullName}</p>
            )}
          </div>

          {/* Phone */}
          <div>
            <label className="block text-sm font-medium text-zinc-700 dark:text-zinc-300 mb-2">
              Телефон *
            </label>
            <div className="relative">
              <Phone className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-zinc-400" />
              <input
                type="tel"
                value={phone}
                onChange={handlePhoneChange}
                placeholder="+7 (999) 123-45-67"
                className={`w-full pl-12 pr-4 py-3 rounded-xl border-2 ${
                  errors.phone
                    ? 'border-red-300 dark:border-red-600 focus:border-red-500'
                    : 'border-zinc-200 dark:border-zinc-600 focus:border-indigo-500'
                } bg-white dark:bg-zinc-700 text-zinc-900 dark:text-white placeholder-zinc-400 dark:placeholder-zinc-500 outline-none transition-colors`}
                disabled={registerMutation.isPending}
              />
            </div>
            {errors.phone && (
              <p className="mt-1 text-sm text-red-500">{errors.phone}</p>
            )}
          </div>

          {/* Email */}
          <div>
            <label className="block text-sm font-medium text-zinc-700 dark:text-zinc-300 mb-2">
              Email *
            </label>
            <div className="relative">
              <Mail className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-zinc-400" />
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="ivan@example.com"
                className={`w-full pl-12 pr-4 py-3 rounded-xl border-2 ${
                  errors.email
                    ? 'border-red-300 dark:border-red-600 focus:border-red-500'
                    : 'border-zinc-200 dark:border-zinc-600 focus:border-indigo-500'
                } bg-white dark:bg-zinc-700 text-zinc-900 dark:text-white placeholder-zinc-400 dark:placeholder-zinc-500 outline-none transition-colors`}
                disabled={registerMutation.isPending}
              />
            </div>
            {errors.email && (
              <p className="mt-1 text-sm text-red-500">{errors.email}</p>
            )}
          </div>

          {/* Terms */}
          <label className="flex items-start gap-3 cursor-pointer">
            <input
              type="checkbox"
              checked={agreeTerms}
              onChange={(e) => setAgreeTerms(e.target.checked)}
              className="mt-1 w-5 h-5 rounded border-zinc-300 dark:border-zinc-600 text-indigo-500 focus:ring-indigo-500"
              disabled={registerMutation.isPending}
            />
            <span className="text-sm text-zinc-600 dark:text-zinc-400">
              Я соглашаюсь с{' '}
              <a href="#" className="text-indigo-500 hover:underline">условиями использования</a>
              {' '}и{' '}
              <a href="#" className="text-indigo-500 hover:underline">политикой конфиденциальности</a>
            </span>
          </label>

          {/* Submit */}
          <button
            type="submit"
            disabled={registerMutation.isPending || !agreeTerms}
            className="w-full py-4 bg-gradient-to-r from-indigo-500 to-purple-500 text-white rounded-xl font-bold text-lg hover:opacity-90 transition-opacity disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
          >
            {registerMutation.isPending ? (
              <>
                <Loader2 className="w-5 h-5 animate-spin" />
                Регистрация...
              </>
            ) : (
              <>
                Завершить регистрацию
                <ArrowRight className="w-5 h-5" />
              </>
            )}
          </button>

          {/* Help */}
          <p className="text-center text-sm text-zinc-500 dark:text-zinc-400">
            Проблемы с регистрацией?{' '}
            <a
              href="https://t.me/wenlix_bot"
              target="_blank"
              rel="noopener noreferrer"
              className="text-indigo-500 hover:underline"
            >
              Напишите нам
            </a>
          </p>
        </form>
      </motion.div>
    </div>
  )
}

