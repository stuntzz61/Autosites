import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Globe, Search, CheckCircle2, XCircle, Loader2, AlertCircle,
  ChevronRight, RefreshCw, Shield, CreditCard, X
} from 'lucide-react'
import { useMutation } from '@tanstack/react-query'
import { domainApi } from '@/api/client'
import toast from 'react-hot-toast'
import clsx from 'clsx'

type DomainStatus =
  | 'idle'
  | 'checking'
  | 'available'
  | 'taken'
  | 'invalid'
  | 'registering'
  | 'bill_created'
  | 'configuring'
  | 'connected'
  | 'error'

interface Alternative {
  domain: string
  price?: { currency: string; amount: number }
}

interface DomainModalProps {
  isOpen: boolean
  onClose: () => void
  requestId: string
  currentDomain?: string
  onDomainRegistered?: (domain: string) => void
}

export default function DomainModal({
  isOpen,
  onClose,
  requestId,
  currentDomain,
  onDomainRegistered
}: DomainModalProps) {
  const [domainInput, setDomainInput] = useState(currentDomain || '')
  const [status, setStatus] = useState<DomainStatus>('idle')
  const [price, setPrice] = useState<{ currency: string; amount: number } | null>(null)
  const [alternatives, setAlternatives] = useState<Alternative[]>([])
  const [errorMessage, setErrorMessage] = useState('')
  const [billId, setBillId] = useState<string | null>(null)
  const [registeredDomain, setRegisteredDomain] = useState<string | null>(null)

  // Reset state when modal opens
  useEffect(() => {
    if (isOpen) {
      setDomainInput(currentDomain || '')
      setStatus('idle')
      setPrice(null)
      setAlternatives([])
      setErrorMessage('')
      setBillId(null)
      setRegisteredDomain(null)
    }
  }, [isOpen, currentDomain])

  // Check domain mutation
  const checkMutation = useMutation({
    mutationFn: (domain: string) => domainApi.check(requestId, {
      domain,
      max_suggestions: 15
    }),
    onMutate: () => {
      setStatus('checking')
      setPrice(null)
      setAlternatives([])
      setErrorMessage('')
    },
    onSuccess: (res) => {
      const data = res.data
      if (data.status === 'available') {
        setStatus('available')
        setPrice(data.price || null)
      } else if (data.status === 'taken') {
        setStatus('taken')
        setAlternatives(data.alternatives || [])
      } else if (data.status === 'invalid_domain') {
        setStatus('invalid')
        setErrorMessage(data.message || 'Некорректный формат домена')
      }
    },
    onError: (err: any) => {
      setStatus('error')
      setErrorMessage(err.response?.data?.detail || 'Ошибка проверки домена')
      console.error('Domain check error:', err)
    }
  })

  // Register domain mutation
  const registerMutation = useMutation({
    mutationFn: (domain: string) => domainApi.register(requestId, {
      domain,
      configure_dns: true
    }),
    onMutate: () => {
      setStatus('registering')
      setErrorMessage('')
    },
    onSuccess: (res) => {
      const data = res.data
      if (data.status === 'registered') {
        setStatus('configuring')
        setRegisteredDomain(data.domain)

        // Wait a bit then show as connected
        setTimeout(() => {
          setStatus('connected')
          toast.success('Домен успешно оформлен!')
          onDomainRegistered?.(data.domain)
        }, 2000)
      } else if (data.status === 'bill_created') {
        setStatus('bill_created')
        setBillId(data.bill_id)
        setErrorMessage(data.message || 'Недостаточно средств на балансе REG.RU')
      } else if (data.status === 'taken') {
        setStatus('taken')
        setErrorMessage('Домен был занят в процессе регистрации')
        // Re-check to get alternatives
        checkMutation.mutate(domainInput)
      } else {
        setStatus('error')
        setErrorMessage(data.message || 'Ошибка регистрации')
      }
    },
    onError: (err: any) => {
      setStatus('error')
      setErrorMessage(err.response?.data?.detail || 'Ошибка регистрации домена')
      console.error('Domain register error:', err)
    }
  })

  const handleCheck = () => {
    const domain = domainInput.trim().toLowerCase()
    if (!domain) {
      toast.error('Введите домен')
      return
    }
    checkMutation.mutate(domain)
  }

  const handleSelectAlternative = (alt: Alternative) => {
    setDomainInput(alt.domain)
    setPrice(alt.price || null)
    setStatus('available')
    setAlternatives([])
  }

  const handleRegister = () => {
    const domain = domainInput.trim().toLowerCase()
    if (!domain) return
    registerMutation.mutate(domain)
  }

  const handleRefreshStatus = () => {
    // Re-check domain status
    checkMutation.mutate(domainInput)
  }

  if (!isOpen) return null

  return (
    <AnimatePresence>
      <motion.div
        className="fixed inset-0 bg-black/50 z-40"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        onClick={onClose}
      />
      <motion.div
        className="fixed bottom-0 left-0 right-0 bg-tg-bg rounded-t-3xl p-4 z-50 safe-bottom max-h-[85vh] overflow-y-auto"
        initial={{ y: '100%' }}
        animate={{ y: 0 }}
        exit={{ y: '100%' }}
      >
        <div className="w-12 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />

        {/* Header */}
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Globe className="w-5 h-5 text-purple-500" />
            <p className="text-lg font-semibold">Подключить домен</p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-tg-secondary-bg rounded-lg transition-colors"
          >
            <X className="w-5 h-5 text-tg-hint" />
          </button>
        </div>

        <div className="space-y-4">
          {/* Domain Input */}
          <div>
            <label className="text-xs text-tg-hint mb-1 block">Домен</label>
            <div className="flex gap-2">
              <input
                type="text"
                value={domainInput}
                onChange={(e) => {
                  setDomainInput(e.target.value.toLowerCase().replace(/\s/g, ''))
                  if (status !== 'idle' && status !== 'checking') {
                    setStatus('idle')
                  }
                }}
                onKeyDown={(e) => e.key === 'Enter' && handleCheck()}
                placeholder="example.ru"
                className="input flex-1"
                disabled={status === 'registering' || status === 'configuring' || status === 'connected'}
              />
              <button
                onClick={handleCheck}
                disabled={checkMutation.isPending || status === 'registering' || status === 'configuring'}
                className="btn btn-primary px-4"
              >
                {checkMutation.isPending ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <Search className="w-5 h-5" />
                )}
              </button>
            </div>
            <p className="text-xs text-tg-hint mt-1">
              Укажите домен без http:// и www
            </p>
          </div>

          {/* Status Messages */}
          {status === 'checking' && (
            <div className="flex items-center gap-3 p-4 bg-blue-50 dark:bg-blue-900/20 rounded-xl">
              <Loader2 className="w-5 h-5 text-blue-500 animate-spin" />
              <span className="text-sm text-blue-600 dark:text-blue-400">
                Проверяем доступность...
              </span>
            </div>
          )}

          {status === 'available' && (
            <div className="p-4 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-xl">
              <div className="flex items-center gap-3 mb-3">
                <CheckCircle2 className="w-6 h-6 text-green-500" />
                <div>
                  <p className="font-medium text-green-700 dark:text-green-400">
                    Домен свободен!
                  </p>
                  <p className="text-sm text-green-600 dark:text-green-500">
                    {domainInput}
                  </p>
                </div>
              </div>
              {price && (
                <div className="flex items-center justify-between p-3 bg-white dark:bg-black/20 rounded-lg mb-3">
                  <span className="text-sm text-tg-hint">Стоимость регистрации:</span>
                  <span className="font-bold text-lg text-tg-text">
                    {price.amount} {price.currency === 'RUB' ? '₽' : price.currency}/год
                  </span>
                </div>
              )}
              <button
                onClick={handleRegister}
                disabled={registerMutation.isPending}
                className="w-full btn btn-primary bg-green-500 hover:bg-green-600"
              >
                {registerMutation.isPending ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <>
                    <Shield className="w-5 h-5 mr-2" />
                    Оформить и подключить
                  </>
                )}
              </button>
            </div>
          )}

          {status === 'taken' && (
            <div className="space-y-3">
              <div className="flex items-center gap-3 p-4 bg-red-50 dark:bg-red-900/20 rounded-xl">
                <XCircle className="w-5 h-5 text-red-500" />
                <span className="text-sm text-red-600 dark:text-red-400">
                  Домен <strong>{domainInput}</strong> уже занят
                </span>
              </div>

              {alternatives.length > 0 && (
                <div>
                  <p className="text-sm font-medium text-tg-text mb-2">
                    Доступные варианты:
                  </p>
                  <div className="space-y-2 max-h-[200px] overflow-y-auto">
                    {alternatives.map((alt, i) => (
                      <button
                        key={i}
                        onClick={() => handleSelectAlternative(alt)}
                        className="w-full flex items-center justify-between p-3 bg-tg-secondary-bg hover:bg-tg-hint/10 rounded-xl transition-colors text-left"
                      >
                        <div className="flex items-center gap-2">
                          <CheckCircle2 className="w-4 h-4 text-green-500" />
                          <span className="font-medium text-tg-text">{alt.domain}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          {alt.price && (
                            <span className="text-sm text-tg-hint">
                              {alt.price.amount} ₽
                            </span>
                          )}
                          <ChevronRight className="w-4 h-4 text-tg-hint" />
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {status === 'invalid' && (
            <div className="flex items-center gap-3 p-4 bg-orange-50 dark:bg-orange-900/20 rounded-xl">
              <AlertCircle className="w-5 h-5 text-orange-500" />
              <span className="text-sm text-orange-600 dark:text-orange-400">
                {errorMessage}
              </span>
            </div>
          )}

          {status === 'registering' && (
            <div className="p-4 bg-blue-50 dark:bg-blue-900/20 rounded-xl">
              <div className="flex items-center gap-3 mb-2">
                <Loader2 className="w-5 h-5 text-blue-500 animate-spin" />
                <span className="font-medium text-blue-600 dark:text-blue-400">
                  Создаём заявку в REG.RU...
                </span>
              </div>
              <div className="w-full bg-blue-200 dark:bg-blue-800 rounded-full h-2">
                <div className="bg-blue-500 h-2 rounded-full w-1/3 animate-pulse" />
              </div>
            </div>
          )}

          {status === 'configuring' && (
            <div className="p-4 bg-purple-50 dark:bg-purple-900/20 rounded-xl">
              <div className="flex items-center gap-3 mb-2">
                <Loader2 className="w-5 h-5 text-purple-500 animate-spin" />
                <span className="font-medium text-purple-600 dark:text-purple-400">
                  Настраиваем DNS...
                </span>
              </div>
              <div className="w-full bg-purple-200 dark:bg-purple-800 rounded-full h-2">
                <div className="bg-purple-500 h-2 rounded-full w-2/3 animate-pulse" />
              </div>
            </div>
          )}

          {status === 'connected' && (
            <div className="p-4 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-xl">
              <div className="flex items-center gap-3">
                <CheckCircle2 className="w-6 h-6 text-green-500" />
                <div>
                  <p className="font-medium text-green-700 dark:text-green-400">
                    Домен подключён!
                  </p>
                  <p className="text-sm text-green-600 dark:text-green-500">
                    {registeredDomain}
                  </p>
                </div>
              </div>
              <p className="text-xs text-tg-hint mt-3">
                DNS записи настроены. Полная активация может занять до 24 часов.
              </p>
              <button
                onClick={onClose}
                className="w-full btn btn-primary mt-3"
              >
                Готово
              </button>
            </div>
          )}

          {status === 'bill_created' && (
            <div className="p-4 bg-orange-50 dark:bg-orange-900/20 border border-orange-200 dark:border-orange-800 rounded-xl">
              <div className="flex items-center gap-3 mb-3">
                <CreditCard className="w-6 h-6 text-orange-500" />
                <div>
                  <p className="font-medium text-orange-700 dark:text-orange-400">
                    Создан счёт на оплату
                  </p>
                  {billId && (
                    <p className="text-sm text-orange-600 dark:text-orange-500">
                      Счёт №{billId}
                    </p>
                  )}
                </div>
              </div>
              <p className="text-sm text-tg-hint mb-3">
                {errorMessage || 'Недостаточно средств на балансе REG.RU. Оплатите счёт в личном кабинете REG.RU.'}
              </p>
              <div className="flex gap-2">
                <button
                  onClick={handleRefreshStatus}
                  disabled={checkMutation.isPending}
                  className="btn btn-secondary flex-1"
                >
                  {checkMutation.isPending ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <>
                      <RefreshCw className="w-4 h-4 mr-2" />
                      Обновить статус
                    </>
                  )}
                </button>
              </div>
            </div>
          )}

          {status === 'error' && (
            <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl">
              <div className="flex items-center gap-3 mb-2">
                <AlertCircle className="w-5 h-5 text-red-500" />
                <span className="font-medium text-red-600 dark:text-red-400">
                  Произошла ошибка
                </span>
              </div>
              <p className="text-sm text-tg-hint mb-3">
                {errorMessage || 'Попробуйте ещё раз или обратитесь в поддержку.'}
              </p>
              <button
                onClick={handleCheck}
                className="btn btn-secondary w-full"
              >
                <RefreshCw className="w-4 h-4 mr-2" />
                Повторить
              </button>
            </div>
          )}

          {/* Info Block */}
          {status === 'idle' && (
            <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-xl">
              <p className="text-sm text-blue-600 dark:text-blue-400">
                💡 <strong>Как это работает:</strong>
                <br />1. Введите желаемый домен и нажмите «Проверить»
                <br />2. Если домен свободен — оформите регистрацию
                <br />3. Если занят — выберите из предложенных вариантов
                <br />4. Домен будет автоматически подключён к вашему сайту
              </p>
            </div>
          )}
        </div>
      </motion.div>
    </AnimatePresence>
  )
}

