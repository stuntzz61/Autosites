import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import { motion } from 'framer-motion'
import {
  User, Phone, CheckCircle2, AlertCircle, Loader2,
  ExternalLink, Copy, Check, ArrowRight
} from 'lucide-react'
import { useMutation } from '@tanstack/react-query'
import { managerApi } from '@/api/client'
import toast from 'react-hot-toast'

type InviteStatus = 'loading' | 'new' | 'activated' | 'invalid' | 'expired' | 'registering' | 'success'

export default function InvitePage() {
  const { token } = useParams<{ token: string }>()

  const [status, setStatus] = useState<InviteStatus>('loading')
  const [fullName, setFullName] = useState('')
  const [phone, setPhone] = useState('')
  const [agreeTerms, setAgreeTerms] = useState(true)
  const [errors, setErrors] = useState<{ fullName?: string; phone?: string }>({})
  const [copied, setCopied] = useState(false)

  // Cleanup timeout for copied state
  useEffect(() => {
    if (copied) {
      const timeout = setTimeout(() => setCopied(false), 2000)
      return () => clearTimeout(timeout)
    }
  }, [copied])

  // Existing data for activated invites
  const [managerData, setManagerData] = useState<{ id: string; full_name?: string; phone?: string } | null>(null)
  const [workspaceData, setWorkspaceData] = useState<{ id: string; name: string; slug: string } | null>(null)
  const [redirectUrl, setRedirectUrl] = useState('/')

  // Check invite status on mount
  useEffect(() => {
    let cancelled = false

    const checkInvite = async () => {
      if (!token) {
        if (!cancelled) setStatus('invalid')
        return
      }
      try {
        const res = await managerApi.checkInvite(token)
        if (cancelled) return

        const data = res.data
        if (data.status === 'new') {
          setStatus('new')
        } else if (data.status === 'activated') {
          setStatus('activated')
          setManagerData(data.manager || null)
          setWorkspaceData(data.workspace || null)
        } else if (data.status === 'expired') {
          setStatus('expired')
        } else {
          setStatus('invalid')
        }
      } catch {
        if (!cancelled) setStatus('invalid')
      }
    }

    checkInvite()

    return () => {
      cancelled = true
    }
  }, [token])

  // Registration mutation
  const registerMutation = useMutation({
    mutationFn: async () => {
      if (!token) throw new Error('Token is required')
      return managerApi.register(token, {
        full_name: fullName.trim(),
        phone: phone,
        agree_terms: agreeTerms
      })
    },
    onMutate: () => {
      setStatus('registering')
      setErrors({})
    },
    onSuccess: (res) => {
      const data = res.data
      setStatus('success')
      setManagerData(data.manager)
      setWorkspaceData(data.workspace)
      setRedirectUrl(data.redirect_url || '/')
      toast.success('Регистрация успешно завершена!')
    },
    onError: (err: unknown) => {
      setStatus('new')
      const error = err as { response?: { data?: { detail?: string | Array<{ loc?: string[]; msg?: string }> } } }
      const detail = error.response?.data?.detail
      if (typeof detail === 'string') {
        toast.error(detail)
      } else if (Array.isArray(detail)) {
        // Pydantic validation errors
        const newErrors: { fullName?: string; phone?: string } = {}
        detail.forEach((e) => {
          if (e.loc?.includes('full_name')) {
            newErrors.fullName = e.msg || 'Ошибка валидации ФИО'
          }
          if (e.loc?.includes('phone')) {
            newErrors.phone = e.msg || 'Ошибка валидации телефона'
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
    const newErrors: { fullName?: string; phone?: string } = {}

    if (!fullName.trim()) {
      newErrors.fullName = 'Укажите ФИО'
    } else if (fullName.trim().split(/\s+/).length < 2) {
      newErrors.fullName = 'Укажите полное ФИО (имя и фамилию)'
    }

    const phoneDigits = phone.replace(/\D/g, '')
    if (phoneDigits.length !== 11) {
      newErrors.phone = 'Укажите полный номер телефона'
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

  const handleOpenDashboard = () => {
    window.open(redirectUrl, '_blank')
  }

  const handleCopyLink = () => {
    navigator.clipboard.writeText(window.location.href)
    setCopied(true)
    toast.success('Ссылка скопирована')
  }

  // Loading state
  if (status === 'loading') {
    return (
      <div className="min-h-screen bg-gradient-to-br from-indigo-50 via-white to-purple-50 dark:from-zinc-900 dark:via-zinc-900 dark:to-indigo-950 flex items-center justify-center p-4">
        <div className="text-center">
          <Loader2 className="w-12 h-12 text-indigo-500 animate-spin mx-auto mb-4" />
          <p className="text-zinc-600 dark:text-zinc-400">Проверяем приглашение...</p>
        </div>
      </div>
    )
  }

  // Invalid/expired link
  if (status === 'invalid' || status === 'expired') {
    return (
      <div className="min-h-screen bg-gradient-to-br from-red-50 via-white to-orange-50 dark:from-zinc-900 dark:via-zinc-900 dark:to-red-950 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          className="max-w-md w-full bg-white dark:bg-zinc-800 rounded-3xl shadow-2xl p-8 text-center"
        >
          <div className="w-20 h-20 bg-red-100 dark:bg-red-900/30 rounded-full flex items-center justify-center mx-auto mb-6">
            <AlertCircle className="w-10 h-10 text-red-500" />
          </div>
          <h1 className="text-2xl font-bold text-zinc-900 dark:text-white mb-3">
            {status === 'expired' ? 'Срок действия истёк' : 'Ссылка недействительна'}
          </h1>
          <p className="text-zinc-600 dark:text-zinc-400 mb-6">
            {status === 'expired'
              ? 'Срок действия этой ссылки истёк. Запросите новую ссылку у администратора.'
              : 'Эта ссылка недействительна или уже была использована. Обратитесь к администратору.'}
          </p>
          <a
            href="https://t.me/wenlix_bot"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-6 py-3 bg-indigo-500 text-white rounded-xl font-medium hover:bg-indigo-600 transition-colors"
          >
            <ExternalLink className="w-5 h-5" />
            Написать в поддержку
          </a>
        </motion.div>
      </div>
    )
  }

  // Already activated - show redirect
  if (status === 'activated') {
    return (
      <div className="min-h-screen bg-gradient-to-br from-green-50 via-white to-emerald-50 dark:from-zinc-900 dark:via-zinc-900 dark:to-green-950 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          className="max-w-md w-full bg-white dark:bg-zinc-800 rounded-3xl shadow-2xl p-8 text-center"
        >
          <div className="w-20 h-20 bg-green-100 dark:bg-green-900/30 rounded-full flex items-center justify-center mx-auto mb-6">
            <CheckCircle2 className="w-10 h-10 text-green-500" />
          </div>
          <h1 className="text-2xl font-bold text-zinc-900 dark:text-white mb-3">
            Вы уже зарегистрированы
          </h1>
          {managerData && (
            <div className="bg-zinc-50 dark:bg-zinc-700/50 rounded-xl p-4 mb-6 text-left">
              <p className="text-sm text-zinc-500 dark:text-zinc-400 mb-1">Ваш профиль:</p>
              <p className="font-medium text-zinc-900 dark:text-white">{managerData.full_name}</p>
              {managerData.phone && (
                <p className="text-sm text-zinc-600 dark:text-zinc-300">{managerData.phone}</p>
              )}
            </div>
          )}
          <button
            onClick={handleOpenDashboard}
            className="w-full flex items-center justify-center gap-2 px-6 py-4 bg-gradient-to-r from-indigo-500 to-purple-500 text-white rounded-xl font-medium hover:opacity-90 transition-opacity"
          >
            Открыть рабочий кабинет
            <ExternalLink className="w-5 h-5" />
          </button>
          <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-4">
            Кабинет откроется в новом окне
          </p>
        </motion.div>
      </div>
    )
  }

  // Success - show confirmation
  if (status === 'success') {
    return (
      <div className="min-h-screen bg-gradient-to-br from-green-50 via-white to-emerald-50 dark:from-zinc-900 dark:via-zinc-900 dark:to-green-950 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          className="max-w-md w-full bg-white dark:bg-zinc-800 rounded-3xl shadow-2xl p-8 text-center"
        >
          <motion.div
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            transition={{ type: "spring", delay: 0.2 }}
            className="w-24 h-24 bg-gradient-to-br from-green-400 to-emerald-500 rounded-full flex items-center justify-center mx-auto mb-6 shadow-lg"
          >
            <CheckCircle2 className="w-12 h-12 text-white" />
          </motion.div>
          <h1 className="text-2xl font-bold text-zinc-900 dark:text-white mb-3">
            Регистрация завершена!
          </h1>
          <p className="text-zinc-600 dark:text-zinc-400 mb-6">
            Добро пожаловать, {managerData?.full_name?.split(' ')[0]}! Ваш рабочий кабинет готов.
          </p>

          {workspaceData && (
            <div className="bg-indigo-50 dark:bg-indigo-900/20 rounded-xl p-4 mb-6 text-left">
              <p className="text-sm text-indigo-600 dark:text-indigo-400 mb-1">Ваше рабочее пространство:</p>
              <p className="font-medium text-indigo-800 dark:text-indigo-200">{workspaceData.name}</p>
            </div>
          )}

          <button
            onClick={handleOpenDashboard}
            className="w-full flex items-center justify-center gap-2 px-6 py-4 bg-gradient-to-r from-indigo-500 to-purple-500 text-white rounded-xl font-bold text-lg hover:opacity-90 transition-opacity shadow-lg"
          >
            Открыть рабочий кабинет
            <ArrowRight className="w-5 h-5" />
          </button>

          <div className="mt-6 pt-6 border-t border-zinc-200 dark:border-zinc-700">
            <p className="text-sm text-zinc-500 dark:text-zinc-400 mb-3">
              Сохраните эту ссылку для быстрого доступа:
            </p>
            <button
              onClick={handleCopyLink}
              className="inline-flex items-center gap-2 px-4 py-2 bg-zinc-100 dark:bg-zinc-700 rounded-lg text-sm text-zinc-600 dark:text-zinc-300 hover:bg-zinc-200 dark:hover:bg-zinc-600 transition-colors"
            >
              {copied ? <Check className="w-4 h-4 text-green-500" /> : <Copy className="w-4 h-4" />}
              {copied ? 'Скопировано!' : 'Копировать ссылку'}
            </button>
          </div>
        </motion.div>
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
          <div className="w-16 h-16 bg-white/20 backdrop-blur rounded-2xl flex items-center justify-center mx-auto mb-4">
            <User className="w-8 h-8" />
          </div>
          <h1 className="text-2xl font-bold mb-2">Регистрация менеджера</h1>
          <p className="text-white/80 text-sm">
            Заполните форму для создания рабочего кабинета
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
                disabled={status === 'registering'}
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
                disabled={status === 'registering'}
              />
            </div>
            {errors.phone && (
              <p className="mt-1 text-sm text-red-500">{errors.phone}</p>
            )}
          </div>

          {/* Terms */}
          <label className="flex items-start gap-3 cursor-pointer">
            <input
              type="checkbox"
              checked={agreeTerms}
              onChange={(e) => setAgreeTerms(e.target.checked)}
              className="mt-1 w-5 h-5 rounded border-zinc-300 dark:border-zinc-600 text-indigo-500 focus:ring-indigo-500"
              disabled={status === 'registering'}
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
            disabled={status === 'registering' || !agreeTerms}
            className="w-full py-4 bg-gradient-to-r from-indigo-500 to-purple-500 text-white rounded-xl font-bold text-lg hover:opacity-90 transition-opacity disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
          >
            {status === 'registering' ? (
              <>
                <Loader2 className="w-5 h-5 animate-spin" />
                Регистрация...
              </>
            ) : (
              'Зарегистрироваться'
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

