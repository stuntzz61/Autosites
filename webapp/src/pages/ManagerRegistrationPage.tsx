import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  User, Phone, Mail, AlertCircle, Loader2,
  ArrowRight, Check
} from 'lucide-react'
import { useMutation } from '@tanstack/react-query'
import { managerApi } from '@/api/client'
import toast from 'react-hot-toast'
import { useAuthStore } from '@/stores/authStore'
import { useNavigate } from 'react-router-dom'
import { useTelegram } from '@/contexts/TelegramContext'

export default function ManagerRegistrationPage() {
  const navigate = useNavigate()
  const { user, refreshUser } = useAuthStore()
  const { haptic } = useTelegram()

  const [fullName, setFullName] = useState('')
  const [phone, setPhone] = useState('')
  const [email, setEmail] = useState('')
  const [agreeTerms, setAgreeTerms] = useState(true)
  const [errors, setErrors] = useState<{ fullName?: string; phone?: string; email?: string }>({})
  const [focusedField, setFocusedField] = useState<string | null>(null)

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
    onSuccess: async () => {
      haptic?.notificationOccurred('success')
      toast.success('Регистрация успешно завершена!')
      // Refresh user data
      await refreshUser()
      // Navigate to home
      navigate('/')
    },
    onError: (err: unknown) => {
      haptic?.notificationOccurred('error')
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
    },
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
    haptic?.impactOccurred('medium')
    if (validateForm()) {
      registerMutation.mutate()
    }
  }

  // Loading state
  if (!user) {
    return (
      <div className="min-h-screen flex items-center justify-center p-4" style={{ background: 'var(--bg-deep)' }}>
        <div className="text-center">
          <motion.div
            animate={{ rotate: 360 }}
            transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
            className="w-12 h-12 mx-auto mb-4"
            style={{ color: 'var(--accent-primary)' }}
          >
            <Loader2 className="w-full h-full" />
          </motion.div>
          <p style={{ color: 'var(--text-muted)' }}>Загрузка...</p>
        </div>
      </div>
    )
  }

  // Registration form
  return (
    <div className="min-h-screen flex items-center justify-center p-4" style={{ background: 'var(--bg-deep)' }}>
      {/* Background decorative elements */}
      <div
        className="fixed inset-0 pointer-events-none overflow-hidden"
        style={{ zIndex: 0 }}
      >
        <div
          className="absolute top-0 right-0 w-96 h-96 rounded-full blur-3xl opacity-20"
          style={{ background: 'var(--accent-primary)' }}
        />
        <div
          className="absolute bottom-0 left-0 w-96 h-96 rounded-full blur-3xl opacity-20"
          style={{ background: 'var(--info)' }}
        />
      </div>

      <motion.div
        initial={{ opacity: 0, y: 20, scale: 0.95 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        transition={{ duration: 0.4, ease: [0.4, 0, 0.2, 1] }}
        className="max-w-md w-full relative z-10"
      >
        {/* Premium Card */}
        <div
          className="rounded-3xl overflow-hidden"
          style={{
            background: 'var(--bg-surface)',
            border: '1px solid var(--border-default)',
            boxShadow: '0 20px 60px rgba(0, 0, 0, 0.3), 0 0 0 1px rgba(255, 255, 255, 0.05) inset'
          }}
        >
          {/* Premium Header with gradient */}
          <div
            className="relative overflow-hidden px-6 pt-8 pb-6"
            style={{
              background: 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 50%, var(--info) 100%)',
            }}
          >
            {/* Shimmer effect */}
            <motion.div
              className="absolute inset-0"
              animate={{
                backgroundPosition: ['0% 0%', '100% 100%'],
              }}
              transition={{
                duration: 3,
                repeat: Infinity,
                repeatType: 'reverse',
                ease: 'linear'
              }}
              style={{
                background: 'linear-gradient(135deg, transparent 0%, rgba(255, 255, 255, 0.1) 50%, transparent 100%)',
                backgroundSize: '200% 200%',
              }}
            />

            {/* Premium glow */}
            <div
              className="absolute top-0 right-0 w-64 h-64 rounded-full blur-3xl opacity-30"
              style={{ background: 'var(--accent-primary)' }}
            />

            <motion.div
              initial={{ scale: 0, rotate: -180 }}
              animate={{ scale: 1, rotate: 0 }}
              transition={{ type: "spring", delay: 0.2, stiffness: 200 }}
              className="relative z-10 w-20 h-20 mx-auto mb-4 rounded-2xl flex items-center justify-center"
              style={{
                background: 'rgba(255, 255, 255, 0.15)',
                backdropFilter: 'blur(10px)',
                border: '1px solid rgba(255, 255, 255, 0.2)',
                boxShadow: '0 8px 32px rgba(0, 0, 0, 0.2)'
              }}
            >
              <User className="w-10 h-10 text-white" strokeWidth={2} />
            </motion.div>

            <motion.h1
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              className="text-2xl font-bold text-center text-white mb-2 relative z-10"
            >
              Завершите регистрацию
            </motion.h1>
            <motion.p
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.4 }}
              className="text-center text-white/90 text-sm relative z-10"
            >
              Ваша заявка одобрена! Заполните форму для доступа к системе
            </motion.p>
          </div>

          {/* Form */}
          <form onSubmit={handleSubmit} className="p-6 space-y-5" style={{ background: 'var(--bg-surface)' }}>
            {/* Full Name */}
            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.1 }}
            >
              <label
                className="block text-sm font-semibold mb-2"
                style={{ color: 'var(--text-primary)' }}
              >
                ФИО *
              </label>
              <div className="relative">
                <div
                  className={`absolute left-4 top-1/2 -translate-y-1/2 transition-colors ${
                    focusedField === 'fullName' ? 'text-blue-500' : 'text-gray-400'
                  }`}
                >
                  <User className="w-5 h-5" />
                </div>
                <input
                  type="text"
                  value={fullName}
                  onChange={(e) => setFullName(e.target.value)}
                  onFocus={() => {
                    setFocusedField('fullName')
                    haptic?.impactOccurred('light')
                  }}
                  onBlur={() => setFocusedField(null)}
                  placeholder="Иванов Иван Иванович"
                  className={`w-full pl-12 pr-4 py-3.5 rounded-xl transition-all ${
                    errors.fullName
                      ? 'border-2 border-red-500/50 focus:border-red-500'
                      : focusedField === 'fullName'
                      ? 'border-2 border-blue-500/50 focus:border-blue-500'
                      : 'border border-gray-300/30 focus:border-blue-500/50'
                  }`}
                  style={{
                    background: errors.fullName
                      ? 'rgba(239, 68, 68, 0.05)'
                      : focusedField === 'fullName'
                      ? 'rgba(59, 130, 246, 0.05)'
                      : 'var(--bg-elevated)',
                    color: 'var(--text-primary)',
                    outline: 'none',
                  }}
                  disabled={registerMutation.isPending}
                />
              </div>
              <AnimatePresence>
                {errors.fullName && (
                  <motion.p
                    initial={{ opacity: 0, y: -5 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -5 }}
                    className="mt-1.5 text-sm flex items-center gap-1.5 text-red-500"
                  >
                    <AlertCircle className="w-4 h-4" />
                    {errors.fullName}
                  </motion.p>
                )}
              </AnimatePresence>
            </motion.div>

            {/* Phone */}
            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.15 }}
            >
              <label
                className="block text-sm font-semibold mb-2"
                style={{ color: 'var(--text-primary)' }}
              >
                Телефон *
              </label>
              <div className="relative">
                <div
                  className={`absolute left-4 top-1/2 -translate-y-1/2 transition-colors ${
                    focusedField === 'phone' ? 'text-blue-500' : 'text-gray-400'
                  }`}
                >
                  <Phone className="w-5 h-5" />
                </div>
                <input
                  type="tel"
                  value={phone}
                  onChange={handlePhoneChange}
                  onFocus={() => {
                    setFocusedField('phone')
                    haptic?.impactOccurred('light')
                  }}
                  onBlur={() => setFocusedField(null)}
                  placeholder="+7 (999) 123-45-67"
                  className={`w-full pl-12 pr-4 py-3.5 rounded-xl transition-all ${
                    errors.phone
                      ? 'border-2 border-red-500/50 focus:border-red-500'
                      : focusedField === 'phone'
                      ? 'border-2 border-blue-500/50 focus:border-blue-500'
                      : 'border border-gray-300/30 focus:border-blue-500/50'
                  }`}
                  style={{
                    background: errors.phone
                      ? 'rgba(239, 68, 68, 0.05)'
                      : focusedField === 'phone'
                      ? 'rgba(59, 130, 246, 0.05)'
                      : 'var(--bg-elevated)',
                    color: 'var(--text-primary)',
                    outline: 'none',
                  }}
                  disabled={registerMutation.isPending}
                />
              </div>
              <AnimatePresence>
                {errors.phone && (
                  <motion.p
                    initial={{ opacity: 0, y: -5 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -5 }}
                    className="mt-1.5 text-sm flex items-center gap-1.5 text-red-500"
                  >
                    <AlertCircle className="w-4 h-4" />
                    {errors.phone}
                  </motion.p>
                )}
              </AnimatePresence>
            </motion.div>

            {/* Email */}
            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.2 }}
            >
              <label
                className="block text-sm font-semibold mb-2"
                style={{ color: 'var(--text-primary)' }}
              >
                Email *
              </label>
              <div className="relative">
                <div
                  className={`absolute left-4 top-1/2 -translate-y-1/2 transition-colors ${
                    focusedField === 'email' ? 'text-blue-500' : 'text-gray-400'
                  }`}
                >
                  <Mail className="w-5 h-5" />
                </div>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  onFocus={() => {
                    setFocusedField('email')
                    haptic?.impactOccurred('light')
                  }}
                  onBlur={() => setFocusedField(null)}
                  placeholder="ivan@example.com"
                  className={`w-full pl-12 pr-4 py-3.5 rounded-xl transition-all ${
                    errors.email
                      ? 'border-2 border-red-500/50 focus:border-red-500'
                      : focusedField === 'email'
                      ? 'border-2 border-blue-500/50 focus:border-blue-500'
                      : 'border border-gray-300/30 focus:border-blue-500/50'
                  }`}
                  style={{
                    background: errors.email
                      ? 'rgba(239, 68, 68, 0.05)'
                      : focusedField === 'email'
                      ? 'rgba(59, 130, 246, 0.05)'
                      : 'var(--bg-elevated)',
                    color: 'var(--text-primary)',
                    outline: 'none',
                  }}
                  disabled={registerMutation.isPending}
                />
              </div>
              <AnimatePresence>
                {errors.email && (
                  <motion.p
                    initial={{ opacity: 0, y: -5 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -5 }}
                    className="mt-1.5 text-sm flex items-center gap-1.5 text-red-500"
                  >
                    <AlertCircle className="w-4 h-4" />
                    {errors.email}
                  </motion.p>
                )}
              </AnimatePresence>
            </motion.div>

            {/* Terms */}
            <motion.label
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.25 }}
              className="flex items-start gap-3 cursor-pointer group"
            >
              <div className="relative flex-shrink-0 mt-0.5">
                <input
                  type="checkbox"
                  checked={agreeTerms}
                  onChange={(e) => {
                    setAgreeTerms(e.target.checked)
                    haptic?.impactOccurred('light')
                  }}
                  className="sr-only"
                  disabled={registerMutation.isPending}
                />
                <div
                  className={`w-5 h-5 rounded-lg border-2 flex items-center justify-center transition-all ${
                    agreeTerms
                      ? 'bg-blue-500 border-blue-500'
                      : 'border-gray-300/50 bg-transparent'
                  }`}
                >
                  {agreeTerms && (
                    <motion.div
                      initial={{ scale: 0 }}
                      animate={{ scale: 1 }}
                      transition={{ type: "spring", stiffness: 500, damping: 30 }}
                    >
                      <Check className="w-3.5 h-3.5 text-white" strokeWidth={3} />
                    </motion.div>
                  )}
                </div>
              </div>
              <span
                className="text-sm leading-relaxed"
                style={{ color: 'var(--text-muted)' }}
              >
                Я соглашаюсь с{' '}
                <a
                  href="#"
                  className="font-medium hover:underline transition-colors"
                  style={{ color: 'var(--accent-primary)' }}
                  onClick={(e) => e.preventDefault()}
                >
                  условиями использования
                </a>
                {' '}и{' '}
                <a
                  href="#"
                  className="font-medium hover:underline transition-colors"
                  style={{ color: 'var(--accent-primary)' }}
                  onClick={(e) => e.preventDefault()}
                >
                  политикой конфиденциальности
                </a>
              </span>
            </motion.label>

            {/* Submit Button */}
            <motion.button
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              type="submit"
              disabled={registerMutation.isPending || !agreeTerms}
              className="w-full py-4 rounded-xl font-bold text-lg text-white relative overflow-hidden group disabled:opacity-50 disabled:cursor-not-allowed"
              style={{
                background: agreeTerms && !registerMutation.isPending
                  ? 'linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-dark) 50%, var(--info) 100%)'
                  : 'var(--bg-elevated)',
                boxShadow: agreeTerms && !registerMutation.isPending
                  ? '0 8px 24px -4px rgba(59, 130, 246, 0.5), 0 0 0 1px rgba(255, 255, 255, 0.1) inset'
                  : 'none',
              }}
              whileHover={agreeTerms && !registerMutation.isPending ? { scale: 1.02 } : {}}
              whileTap={agreeTerms && !registerMutation.isPending ? { scale: 0.98 } : {}}
            >
              {/* Shimmer effect on button */}
              {agreeTerms && !registerMutation.isPending && (
                <motion.div
                  className="absolute inset-0"
                  animate={{
                    backgroundPosition: ['-100% 0%', '100% 0%'],
                  }}
                  transition={{
                    duration: 2,
                    repeat: Infinity,
                    repeatType: 'reverse',
                    ease: 'linear'
                  }}
                  style={{
                    background: 'linear-gradient(90deg, transparent 0%, rgba(255, 255, 255, 0.2) 50%, transparent 100%)',
                    backgroundSize: '200% 100%',
                  }}
                />
              )}

              <span className="relative z-10 flex items-center justify-center gap-2">
                {registerMutation.isPending ? (
                  <>
                    <Loader2 className="w-5 h-5 animate-spin" />
                    Регистрация...
                  </>
                ) : (
                  <>
                    Завершить регистрацию
                    <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                  </>
                )}
              </span>
            </motion.button>

            {/* Help */}
            <motion.p
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.35 }}
              className="text-center text-sm"
              style={{ color: 'var(--text-subtle)' }}
            >
              Проблемы с регистрацией?{' '}
              <a
                href="https://t.me/wenlix_bot"
                target="_blank"
                rel="noopener noreferrer"
                className="font-medium hover:underline transition-colors"
                style={{ color: 'var(--accent-primary)' }}
              >
                Напишите нам
              </a>
            </motion.p>
          </form>
        </div>
      </motion.div>
    </div>
  )
}
